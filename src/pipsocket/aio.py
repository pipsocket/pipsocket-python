"""The asynchronous client: the same calls as Pipsocket, awaited."""

from __future__ import annotations

import asyncio
import contextlib
import math
from collections.abc import Awaitable, Callable, Iterable
from types import TracebackType
from typing import Any, cast

import httpx

from ._core import (
    SEND,
    Attempt,
    Failed,
    Look,
    Spec,
    Wait,
    account_path,
    attach_key,
    drive,
    drive_wait,
    history_range,
    json_body,
    lookup,
    safe,
    symbol_path,
    ticket_segment,
    trade,
)
from ._http import (
    DEFAULT_MAX_RETRIES,
    DEFAULT_RETRY_DEADLINE,
    DEFAULT_TIMEOUT,
    Options,
    answer,
    body_lost,
    build_request,
    check_wait,
    no_answer,
    options,
    watch_response,
)
from .errors import APITimeoutError, PipsocketError
from .models import TradeRequest
from .stream import (
    DEFAULT_QUEUE_SIZE,
    AsyncStream,
    async_reader,
    new_machine,
    report,
    settled,
    stream_url,
    watch,
)
from .types import (
    AccountView,
    CheckView,
    DealView,
    FillingParam,
    HistoryOrderView,
    KillSwitchOutcome,
    KillSwitchView,
    OrderTypeParam,
    PendingOrderView,
    PositionView,
    RequestView,
    SymbolSummaryView,
    SymbolView,
    TickView,
    TypeTimeParam,
)

__all__ = ["AsyncAccount", "AsyncPipsocket"]


class AsyncPipsocket:
    """An asyncio client for the Pipsocket API. Its calls are those of Pipsocket, awaited.

    `api_key` defaults to the PIPSOCKET_KEY environment variable. `timeout` is per attempt, in
    seconds. A call that fails in a way worth repeating is tried again up to `max_retries`
    times, with the same idempotency key, and no new attempt starts `retry_deadline` seconds
    after the first one began. Pass your own `http_client` for proxies or custom transports.
    """

    def __init__(
        self,
        api_key: str | None = None,
        *,
        base_url: str | None = None,
        stream_url: str | None = None,
        timeout: float = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
        retry_deadline: float = DEFAULT_RETRY_DEADLINE,
        http_client: httpx.AsyncClient | None = None,
        _clock: Callable[[], float] | None = None,
        _sleep: Callable[[float], Awaitable[None]] | None = None,
        _random: Callable[[], float] | None = None,
    ) -> None:
        opts = options(
            api_key, base_url, stream_url, timeout, max_retries, retry_deadline, _clock, _random
        )
        self._owns_http = http_client is None
        self._requester = _Requester(
            opts, http_client or httpx.AsyncClient(), _sleep or asyncio.sleep
        )

    def account(self, account_id: str) -> AsyncAccount:
        """The handle for one account: every call is for one account."""
        account_path(account_id, "/")
        return AsyncAccount(self._requester, account_id)

    async def close(self) -> None:
        """Close the HTTP connections, unless the client was given its own http_client."""
        if self._owns_http:
            await self._requester.http.aclose()

    async def __aenter__(self) -> AsyncPipsocket:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()

    def __repr__(self) -> str:
        return f"AsyncPipsocket(base_url={self._requester.options.base_url!r})"


class _Requester:
    """Runs a call's spec through the core, doing the HTTP and the sleeping."""

    def __init__(
        self, opts: Options, http: httpx.AsyncClient, sleep: Callable[[float], Awaitable[None]]
    ) -> None:
        self.options = opts
        self.http = http
        self.sleep = sleep

    async def call(self, spec: Spec, timeout: float | None, max_retries: int | None) -> Any:
        settings = self.options.settings(max_retries)
        attempt_timeout = self.options.attempt_timeout(timeout)
        run = drive(spec, settings)
        step = next(run)
        try:
            while True:
                if step is SEND:
                    step = run.send(await self.attempt(spec, attempt_timeout))
                else:
                    assert isinstance(step, Wait)
                    try:
                        await self.sleep(step.seconds)
                    except Exception as error:
                        step = run.throw(error)  # the core decides how the call ends
                    else:
                        step = run.send(None)
        except StopIteration as done:
            return done.value
        finally:
            run.close()

    async def trade(
        self, spec: Spec, timeout: float | None, max_retries: int | None, streams: list[AsyncStream]
    ) -> TradeRequest:
        """A trade call. A cancellation or interrupt passes through as it is, with the key
        set on it where it can carry one; nothing is sent after it."""
        try:
            request = cast(TradeRequest, await self.call(spec, timeout, max_retries))
        except PipsocketError:
            raise
        except BaseException as error:
            attach_key(error, spec.idempotency_key)
            raise
        return newest(request, streams, spec.idempotency_key)

    async def wait(
        self, spec: Spec, request_id: str, timeout: float, streams: list[AsyncStream]
    ) -> TradeRequest:
        known = settled(streams, request_id)
        if known is not None:
            return TradeRequest(cast(RequestView, known))
        run = drive_wait(check_wait(timeout), self.options.clock)
        seen = _Seen(lambda error: report(streams, error)) if streams else None
        undo = [watch(stream, request_id, seen.saw) for stream in list(streams)] if seen else []
        step = next(run)
        try:
            while True:
                if isinstance(step, Look):
                    outcome = await self.look(spec, step.seconds, seen)
                    if isinstance(outcome, PipsocketError):
                        report(streams, outcome)
                    step = run.send(outcome)
                elif seen is not None:
                    step = run.send(await seen.wait(step.seconds))
                else:
                    await self.sleep(step.seconds)
                    step = run.send(None)
        except StopIteration as done:
            return cast(TradeRequest, done.value)
        finally:
            run.close()
            for unwatch in undo:
                unwatch()

    async def look(
        self, spec: Spec, seconds: float, seen: _Seen | None
    ) -> TradeRequest | PipsocketError:
        """One lookup for wait(), over within `seconds` all told (the HTTP timeout applies to
        each network operation, so a slow answer could otherwise run past wait()'s deadline),
        and ended at once by a settlement an open stream sees meanwhile. A lookup that runs
        over, or that the stream beats, is cancelled and left to wind down on its own (its
        cleanup may be slow): it is only a read."""
        attempt_timeout = min(self.options.timeout, seconds)
        try:
            if seen is None and math.isinf(seconds):
                return cast(TradeRequest, await self.call(spec, attempt_timeout, 0))
            return await (seen or _Seen()).race(self.call(spec, attempt_timeout, 0), seconds)
        except PipsocketError as error:
            return error

    async def attempt(self, spec: Spec, timeout: float) -> Attempt:
        """One attempt, and what it came to. A failure is placed by when it happened: before
        sending (nothing sent), or once sending began (it may have reached the host)."""
        if self.http.is_closed:
            return Failed(RuntimeError("The HTTP client has been closed."), maybe_sent=False)
        try:
            request, body = build_request(self.http, spec, self.options, timeout)
        except Exception as error:
            return Failed(error, maybe_sent=False)
        try:
            # Redirects would be sends the retry policy can't see: the API doesn't redirect.
            response = await self.http.send(request, stream=True, follow_redirects=False)
        except httpx.TransportError as error:
            return no_answer(error, body)
        except Exception as error:
            # Not the transport's: a request or response hook, say. Response hooks run after
            # the answer arrived, so it may have been sent.
            return Failed(error, maybe_sent=True)
        received = watch_response(response)
        content = bytearray()
        try:
            async for chunk in response.aiter_bytes():
                content += chunk
        except Exception as error:
            if not received.finished:
                if isinstance(error, httpx.HTTPError):
                    return body_lost(response, error, bytes(content))
                return Failed(error, maybe_sent=True)
            # The body was read to the end and closing it failed: the answer is all here.
            # (Were a compressed tail lost with it, the JSON won't parse: maybe sent too.)
        finally:
            # A failure to clean up doesn't change what the attempt came to.
            with contextlib.suppress(Exception):
                await response.aclose()
        return answer(response, bytes(content))


class _Seen:
    """A request an open stream saw settle, for a wait()."""

    def __init__(self, failed: Callable[[PipsocketError], None] = lambda error: None) -> None:
        self.request: TradeRequest | None = None
        self.changed = asyncio.Event()
        # Told of a lookup's failure even when the stream won the race: a 401 stops it too.
        self.failed = failed

    def saw(self, body: Any) -> None:
        request = TradeRequest(body)
        if request.final:
            self.request = request
            self.changed.set()

    async def wait(self, seconds: float) -> TradeRequest | None:
        """Wait up to `seconds`, or until a stream sees the request settle."""
        settled = asyncio.ensure_future(self.changed.wait())
        try:
            await asyncio.wait({settled}, timeout=seconds)
        finally:
            # Not awaited: awaiting it could swallow a cancellation of this wait() itself.
            abandon(settled, self.failed)
        return self.request

    async def race(self, lookup: Awaitable[Any], seconds: float) -> TradeRequest:
        """Look the request up within `seconds`, unless a stream sees it settle first. A final
        state seen on the stream is never replaced by an older, outstanding one from the
        lookup. A lookup that is beaten, runs over or outlives a cancelled wait() is cancelled
        without waiting for it."""
        looking = asyncio.ensure_future(lookup)
        settled = asyncio.ensure_future(self.changed.wait())
        limit = None if math.isinf(seconds) else seconds
        try:
            await asyncio.wait(
                {looking, settled}, timeout=limit, return_when=asyncio.FIRST_COMPLETED
            )
        except BaseException:
            abandon(looking, self.failed)
            raise
        finally:
            abandon(settled, self.failed)  # not awaited, as above
        if self.request is not None:
            abandon(looking, self.failed)
            return self.request
        if not looking.done():
            abandon(looking, self.failed)
            raise APITimeoutError("The lookup did not answer within wait()'s timeout.")
        try:
            return cast(TradeRequest, looking.result())
        except PipsocketError as error:
            self.failed(error)
            raise


def newest(request: TradeRequest, streams: list[Any], key: str | None) -> TradeRequest:
    """A trade call's answer, unless an open stream already saw that request settle: a slower
    202 never moves a request seen as final back to pending."""
    if request.outstanding:
        known = settled(streams, request.id)
        if known is not None:
            return TradeRequest(cast(RequestView, known), idempotency_key=key)
    return request


class AsyncAccount:
    """One account's calls. Get it with `AsyncPipsocket.account(id)`."""

    def __init__(self, requester: _Requester, account_id: str) -> None:
        self._requester = requester
        self.id = account_id
        self._streams: list[AsyncStream] = []
        self.positions = Positions(requester, account_id, self._streams)
        self.orders = Orders(requester, account_id, self._streams)
        self.requests = Requests(requester, account_id, self._streams)
        self.history = History(requester, account_id, self._streams)
        self.symbols = Symbols(requester, account_id, self._streams)
        self.kill_switch = KillSwitch(requester, account_id, self._streams)

    async def get(
        self, *, timeout: float | None = None, max_retries: int | None = None
    ) -> AccountView:
        """The account: its terminal's status, and figures from the terminal's last report."""
        spec = safe("account", "GET", self.id, "/")
        return cast(AccountView, await self._requester.call(spec, timeout, max_retries))

    def stream(
        self,
        ticks: Iterable[str] = (),
        *,
        snapshot: bool = True,
        queue_size: int = DEFAULT_QUEUE_SIZE,
    ) -> AsyncStream:
        """The account's events as they happen, as `async with acct.stream(...) as events`.

        Events are the API's messages as they are, plus `resync`, which the helper sends after
        every (re)connection and gap with a new baseline: the account, positions and pending
        orders read over REST, and the history since the last checkpoint. `snapshot=False`
        sends `resync` without reading anything. The socket is kept open and reconnected.
        """
        machine = new_machine(self._requester.options, ticks, snapshot, queue_size)
        url = stream_url(self._requester.options, self.id)
        return AsyncStream(
            url=url,
            options=self._requester.options,
            machine=machine,
            reader=async_reader(self),
            streams=self._streams,
        )

    def __repr__(self) -> str:
        return f"AsyncAccount({self.id!r})"


class _Resource:
    def __init__(self, requester: _Requester, account_id: str, streams: list[AsyncStream]) -> None:
        self._requester = requester
        self._account = account_id
        self._streams = streams


class Positions(_Resource):
    async def list(
        self, *, timeout: float | None = None, max_retries: int | None = None
    ) -> list[PositionView]:
        """The open positions."""
        spec = safe("positions", "GET", self._account, "/positions")
        return cast(list[PositionView], await self._requester.call(spec, timeout, max_retries))

    async def set_levels(
        self,
        ticket: int,
        *,
        sl: float,
        tp: float,
        idempotency_key: str | None = None,
        timeout: float | None = None,
        max_retries: int | None = None,
    ) -> TradeRequest:
        """Set a position's stop loss and take profit (0 removes one). Give both."""
        path = f"/positions/{ticket_segment(ticket)}"
        body = {"sl": sl, "tp": tp}
        spec = trade("set_position_levels", "PATCH", self._account, path, body, idempotency_key)
        return await self._requester.trade(spec, timeout, max_retries, self._streams)

    async def close(
        self,
        ticket: int,
        *,
        volume: float,
        type_filling: FillingParam,
        price: float | None = None,
        deviation: int | None = None,
        magic: int | None = None,
        comment: str | None = None,
        idempotency_key: str | None = None,
        timeout: float | None = None,
        max_retries: int | None = None,
    ) -> TradeRequest:
        """Close all of a position, or part of it with a smaller volume."""
        path = f"/positions/{ticket_segment(ticket)}/close"
        body = {
            "volume": volume,
            "type_filling": type_filling,
            "price": price,
            "deviation": deviation,
            "magic": magic,
            "comment": comment,
        }
        spec = trade("close_position", "POST", self._account, path, body, idempotency_key)
        return await self._requester.trade(spec, timeout, max_retries, self._streams)

    async def close_by(
        self,
        ticket: int,
        *,
        position_by: int,
        idempotency_key: str | None = None,
        timeout: float | None = None,
        max_retries: int | None = None,
    ) -> TradeRequest:
        """Close a position against an opposite one on the same symbol."""
        path = f"/positions/{ticket_segment(ticket)}/close-by"
        body = {"position_by": position_by}
        spec = trade("close_by", "POST", self._account, path, body, idempotency_key)
        return await self._requester.trade(spec, timeout, max_retries, self._streams)


class Orders(_Resource):
    async def list(
        self, *, timeout: float | None = None, max_retries: int | None = None
    ) -> list[PendingOrderView]:
        """The pending orders."""
        spec = safe("pending_orders", "GET", self._account, "/orders")
        return cast(list[PendingOrderView], await self._requester.call(spec, timeout, max_retries))

    async def place(
        self,
        *,
        symbol: str,
        type: OrderTypeParam,
        volume: float,
        type_filling: FillingParam,
        price: float | None = None,
        stoplimit: float | None = None,
        sl: float | None = None,
        tp: float | None = None,
        deviation: int | None = None,
        type_time: TypeTimeParam | None = None,
        expiration: int | None = None,
        magic: int | None = None,
        comment: str | None = None,
        idempotency_key: str | None = None,
        timeout: float | None = None,
        max_retries: int | None = None,
    ) -> TradeRequest:
        """Place a market or pending order: one MT5 trade request, sent once.

        The answer is the recorded request in whatever state it reached (read `state`). Every
        retry inside this call sends the same idempotency key; calling again is a new trade.
        """
        body = order_fields(locals())
        spec = trade("place_order", "POST", self._account, "/orders", body, idempotency_key)
        return await self._requester.trade(spec, timeout, max_retries, self._streams)

    async def check(
        self,
        *,
        symbol: str,
        type: OrderTypeParam,
        volume: float,
        type_filling: FillingParam,
        price: float | None = None,
        stoplimit: float | None = None,
        sl: float | None = None,
        tp: float | None = None,
        deviation: int | None = None,
        type_time: TypeTimeParam | None = None,
        expiration: int | None = None,
        magic: int | None = None,
        comment: str | None = None,
        timeout: float | None = None,
        max_retries: int | None = None,
    ) -> CheckView:
        """MT5's dry run of an order (OrderCheck): sends nothing to the broker."""
        body = json_body(order_fields(locals()))
        spec = safe("check_order", "POST", self._account, "/orders/check", body=body)
        return cast(CheckView, await self._requester.call(spec, timeout, max_retries))

    async def modify(
        self,
        ticket: int,
        *,
        price: float,
        sl: float,
        tp: float,
        type_time: TypeTimeParam,
        stoplimit: float | None = None,
        expiration: int | None = None,
        idempotency_key: str | None = None,
        timeout: float | None = None,
        max_retries: int | None = None,
    ) -> TradeRequest:
        """Change a pending order's levels. MT5 replaces every level: give the ones you keep."""
        path = f"/orders/{ticket_segment(ticket)}"
        body = {
            "price": price,
            "sl": sl,
            "tp": tp,
            "type_time": type_time,
            "stoplimit": stoplimit,
            "expiration": expiration,
        }
        spec = trade("modify_order", "PATCH", self._account, path, body, idempotency_key)
        return await self._requester.trade(spec, timeout, max_retries, self._streams)

    async def cancel(
        self,
        ticket: int,
        *,
        idempotency_key: str | None = None,
        timeout: float | None = None,
        max_retries: int | None = None,
    ) -> TradeRequest:
        """Cancel a pending order."""
        path = f"/orders/{ticket_segment(ticket)}"
        spec = trade("cancel_order", "DELETE", self._account, path, None, idempotency_key)
        return await self._requester.trade(spec, timeout, max_retries, self._streams)


class Requests(_Resource):
    async def get(
        self, request_id: str, *, timeout: float | None = None, max_retries: int | None = None
    ) -> TradeRequest:
        """A recorded trade request by its id (`req_...`), whatever its state."""
        spec = lookup(self._account, request_id)
        return cast(TradeRequest, await self._requester.call(spec, timeout, max_retries))

    async def wait(self, request_id: str, *, timeout: float = 30.0) -> TradeRequest:
        """Look a request up until it is final or `timeout` seconds pass, then return it.

        It returns the request as it stands at the timeout (pending or unknown included): it
        never raises for "still pending", and it only ever looks up, never sends. A 401 or 404
        is raised; other failures are polled through until the timeout. With a stream open on
        this account handle, a request event for it ends the wait at once.
        """
        spec = lookup(self._account, request_id)
        return await self._requester.wait(spec, request_id, timeout, self._streams)


class History(_Resource):
    async def deals(
        self,
        from_: int,
        to: int,
        *,
        timeout: float | None = None,
        max_retries: int | None = None,
    ) -> list[DealView]:
        """Deals from `from_` to `to`, both included: broker server time in seconds, not UTC."""
        spec = safe(
            "history_deals",
            "GET",
            self._account,
            "/history/deals",
            query=history_range(from_, to),
        )
        return cast(list[DealView], await self._requester.call(spec, timeout, max_retries))

    async def orders(
        self,
        from_: int,
        to: int,
        *,
        timeout: float | None = None,
        max_retries: int | None = None,
    ) -> list[HistoryOrderView]:
        """Orders no longer pending, from `from_` to `to` (broker server time in seconds)."""
        spec = safe(
            "history_orders",
            "GET",
            self._account,
            "/history/orders",
            query=history_range(from_, to),
        )
        return cast(list[HistoryOrderView], await self._requester.call(spec, timeout, max_retries))


class Symbols(_Resource):
    async def list(
        self, *, timeout: float | None = None, max_retries: int | None = None
    ) -> list[SymbolSummaryView]:
        """Every symbol the broker has."""
        spec = safe("symbols", "GET", self._account, "/symbols")
        return cast(list[SymbolSummaryView], await self._requester.call(spec, timeout, max_retries))

    async def get(
        self, name: str, *, timeout: float | None = None, max_retries: int | None = None
    ) -> SymbolView:
        """A symbol's specification, by its name exactly as the broker spells it."""
        spec = safe("symbol", "GET", self._account, symbol_path(name))
        return cast(SymbolView, await self._requester.call(spec, timeout, max_retries))

    async def tick(
        self, name: str, *, timeout: float | None = None, max_retries: int | None = None
    ) -> TickView:
        """A symbol's latest price."""
        spec = safe("tick", "GET", self._account, symbol_path(name) + "/tick")
        return cast(TickView, await self._requester.call(spec, timeout, max_retries))


class KillSwitch(_Resource):
    async def get(
        self, *, timeout: float | None = None, max_retries: int | None = None
    ) -> KillSwitchView:
        """Whether the account's kill switch is engaged."""
        spec = safe("kill_switch", "GET", self._account, "/kill-switch")
        return cast(KillSwitchView, await self._requester.call(spec, timeout, max_retries))

    async def engage(
        self,
        *,
        deviation: int | None = None,
        timeout: float | None = None,
        max_retries: int | None = None,
    ) -> KillSwitchOutcome:
        """Engage the kill switch: close every position and cancel every pending order, and
        refuse new exposure until it is lifted. Safe to repeat: engaging again retries what is
        left. If the terminal doesn't answer, AccountUnavailable carries `kill_switch`: the
        switch is engaged all the same.
        """
        body = None if deviation is None else json_body({"deviation": deviation})
        spec = safe("engage_kill_switch", "POST", self._account, "/kill-switch", body=body)
        return cast(KillSwitchOutcome, await self._requester.call(spec, timeout, max_retries))

    async def lift(
        self, *, timeout: float | None = None, max_retries: int | None = None
    ) -> KillSwitchView:
        """Lift the kill switch, so new exposure is allowed again."""
        spec = safe("lift_kill_switch", "DELETE", self._account, "/kill-switch")
        return cast(KillSwitchView, await self._requester.call(spec, timeout, max_retries))


ORDER_FIELDS = (
    "symbol",
    "type",
    "volume",
    "type_filling",
    "price",
    "stoplimit",
    "sl",
    "tp",
    "deviation",
    "type_time",
    "expiration",
    "magic",
    "comment",
)


def order_fields(arguments: dict[str, Any]) -> dict[str, Any]:
    """An order's body from a method's arguments: the API's field names, as they are."""
    return {name: arguments[name] for name in ORDER_FIELDS}


# Tasks a wait() stopped waiting for: cancelled but not awaited, since a lookup's cleanup may
# be slow and awaiting could swallow the caller's own cancellation. Kept until they end, so
# they aren't collected while running; a failure a lookup still meets (a 401) is reported.
_abandoned: set[asyncio.Future[Any]] = set()


def abandon(task: asyncio.Future[Any], failed: Callable[[PipsocketError], None]) -> None:
    def ended(task: asyncio.Future[Any]) -> None:
        _abandoned.discard(task)
        if not task.cancelled() and isinstance(task.exception(), PipsocketError):
            failed(cast(PipsocketError, task.exception()))

    task.cancel()
    _abandoned.add(task)
    task.add_done_callback(ended)
