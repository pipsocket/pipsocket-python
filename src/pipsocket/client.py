"""The synchronous client."""

from __future__ import annotations

import contextlib
import threading
import time
from collections.abc import Callable, Iterable
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
    Stream,
    new_machine,
    report,
    settled,
    stream_url,
    sync_reader,
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

__all__ = ["Account", "Pipsocket"]


class Pipsocket:
    """A client for the Pipsocket API.

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
        http_client: httpx.Client | None = None,
        _clock: Callable[[], float] | None = None,
        _sleep: Callable[[float], None] | None = None,
        _random: Callable[[], float] | None = None,
    ) -> None:
        opts = options(
            api_key, base_url, stream_url, timeout, max_retries, retry_deadline, _clock, _random
        )
        self._owns_http = http_client is None
        self._requester = _Requester(opts, http_client or httpx.Client(), _sleep or time.sleep)

    def account(self, account_id: str) -> Account:
        """The handle for one account: every call is for one account."""
        account_path(account_id, "/")
        return Account(self._requester, account_id)

    def close(self) -> None:
        """Close the HTTP connections, unless the client was given its own http_client."""
        if self._owns_http:
            self._requester.http.close()

    def __enter__(self) -> Pipsocket:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"Pipsocket(base_url={self._requester.options.base_url!r})"


class _Requester:
    """Runs a call's spec through the core, doing the HTTP and the sleeping."""

    def __init__(self, opts: Options, http: httpx.Client, sleep: Callable[[float], None]) -> None:
        self.options = opts
        self.http = http
        self.sleep = sleep

    def call(self, spec: Spec, timeout: float | None, max_retries: int | None) -> Any:
        settings = self.options.settings(max_retries)
        attempt_timeout = self.options.attempt_timeout(timeout)
        run = drive(spec, settings)
        step = next(run)
        try:
            while True:
                if step is SEND:
                    step = run.send(self.attempt(spec, attempt_timeout))
                else:
                    assert isinstance(step, Wait)
                    try:
                        self.sleep(step.seconds)
                    except Exception as error:
                        step = run.throw(error)  # the core decides how the call ends
                    else:
                        step = run.send(None)
        except StopIteration as done:
            return done.value
        finally:
            run.close()

    def trade(
        self, spec: Spec, timeout: float | None, max_retries: int | None, streams: list[Stream]
    ) -> TradeRequest:
        """A trade call. A cancellation or interrupt passes through as it is, with the key
        set on it where it can carry one; nothing is sent after it."""
        try:
            request = cast(TradeRequest, self.call(spec, timeout, max_retries))
        except PipsocketError:
            raise
        except BaseException as error:
            attach_key(error, spec.idempotency_key)
            raise
        return newest(request, streams, spec.idempotency_key)

    def wait(
        self, spec: Spec, request_id: str, timeout: float, streams: list[Stream]
    ) -> TradeRequest:
        known = settled(streams, request_id)
        if known is not None:
            return TradeRequest(cast(RequestView, known))
        run = drive_wait(check_wait(timeout), self.options.clock)
        seen = _Seen() if streams else None
        undo = [watch(stream, request_id, seen.saw) for stream in list(streams)] if seen else []
        step = next(run)
        try:
            while True:
                if isinstance(step, Look):
                    attempt_timeout = min(self.options.timeout, step.seconds)
                    outcome = self.look(spec, attempt_timeout, seen)
                    if isinstance(outcome, PipsocketError):
                        report(streams, outcome)
                    step = run.send(outcome)
                elif seen is not None:
                    step = run.send(seen.wait(step.seconds))
                else:
                    self.sleep(step.seconds)
                    step = run.send(None)
        except StopIteration as done:
            return cast(TradeRequest, done.value)
        finally:
            run.close()
            for unwatch in undo:
                unwatch()

    def look(self, spec: Spec, timeout: float, seen: _Seen | None) -> TradeRequest | PipsocketError:
        """One lookup for wait(). A settlement an open stream sees meanwhile ends it at once."""

        def lookup() -> TradeRequest:
            return cast(TradeRequest, self.call(spec, timeout, 0))

        try:
            return lookup() if seen is None else seen.race(lookup)
        except PipsocketError as error:
            return error

    def attempt(self, spec: Spec, timeout: float) -> Attempt:
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
            response = self.http.send(request, stream=True, follow_redirects=False)
        except httpx.TransportError as error:
            return no_answer(error, body)
        except Exception as error:
            # Not the transport's: a request or response hook, say. Response hooks run after
            # the answer arrived, so it may have been sent.
            return Failed(error, maybe_sent=True)
        received = watch_response(response)
        content = bytearray()
        try:
            for chunk in response.iter_bytes():
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
                response.close()
        return answer(response, bytes(content))


class _Seen:
    """A request an open stream saw settle, for a wait()."""

    def __init__(self) -> None:
        self.request: TradeRequest | None = None
        self.changed = threading.Condition()

    def saw(self, body: Any) -> None:
        request = TradeRequest(body)
        if request.final:
            with self.changed:
                self.request = request
                self.changed.notify_all()

    def wait(self, seconds: float) -> TradeRequest | None:
        """Wait up to `seconds`, or until a stream sees the request settle."""
        with self.changed:
            self.changed.wait_for(lambda: self.request is not None, seconds)
            return self.request

    def race(self, lookup: Callable[[], TradeRequest]) -> TradeRequest:
        """Look the request up, unless a stream sees it settle first. A final state seen on
        the stream is never replaced by an older, outstanding one from the lookup. (The
        lookup runs on, as a read, if the stream wins.)"""
        answers: list[TradeRequest | BaseException] = []

        def run() -> None:
            answer: TradeRequest | BaseException
            try:
                answer = lookup()
            except BaseException as error:
                answer = error
            with self.changed:
                answers.append(answer)
                self.changed.notify_all()

        threading.Thread(target=run, daemon=True).start()
        with self.changed:
            self.changed.wait_for(lambda: bool(answers) or self.request is not None)
            if self.request is not None:
                return self.request
        answer = answers[0]
        if isinstance(answer, BaseException):
            raise answer
        return answer


def newest(request: TradeRequest, streams: list[Any], key: str | None) -> TradeRequest:
    """A trade call's answer, unless an open stream already saw that request settle: a slower
    202 never moves a request seen as final back to pending."""
    if request.outstanding:
        known = settled(streams, request.id)
        if known is not None:
            return TradeRequest(cast(RequestView, known), idempotency_key=key)
    return request


class Account:
    """One account's calls. Get it with `Pipsocket.account(id)`."""

    def __init__(self, requester: _Requester, account_id: str) -> None:
        self._requester = requester
        self.id = account_id
        self._streams: list[Stream] = []
        self.positions = Positions(requester, account_id, self._streams)
        self.orders = Orders(requester, account_id, self._streams)
        self.requests = Requests(requester, account_id, self._streams)
        self.history = History(requester, account_id, self._streams)
        self.symbols = Symbols(requester, account_id, self._streams)
        self.kill_switch = KillSwitch(requester, account_id, self._streams)

    def get(self, *, timeout: float | None = None, max_retries: int | None = None) -> AccountView:
        """The account: its terminal's status, and figures from the terminal's last report."""
        spec = safe("account", "GET", self.id, "/")
        return cast(AccountView, self._requester.call(spec, timeout, max_retries))

    def stream(
        self,
        ticks: Iterable[str] = (),
        *,
        snapshot: bool = True,
        queue_size: int = DEFAULT_QUEUE_SIZE,
    ) -> Stream:
        """The account's events as they happen, as `with acct.stream(ticks=[...]) as events`.

        Events are the API's messages as they are, plus `resync`, which the helper sends after
        every (re)connection and gap with a new baseline: the account, positions and pending
        orders read over REST, and the history since the last checkpoint. `snapshot=False`
        sends `resync` without reading anything. The socket is kept open and reconnected.
        """
        machine = new_machine(self._requester.options, ticks, snapshot, queue_size)
        url = stream_url(self._requester.options, self.id)
        return Stream(
            url=url,
            options=self._requester.options,
            machine=machine,
            reader=sync_reader(self),
            streams=self._streams,
        )

    def __repr__(self) -> str:
        return f"Account({self.id!r})"


class _Resource:
    def __init__(self, requester: _Requester, account_id: str, streams: list[Stream]) -> None:
        self._requester = requester
        self._account = account_id
        self._streams = streams


class Positions(_Resource):
    def list(
        self, *, timeout: float | None = None, max_retries: int | None = None
    ) -> list[PositionView]:
        """The open positions."""
        spec = safe("positions", "GET", self._account, "/positions")
        return cast(list[PositionView], self._requester.call(spec, timeout, max_retries))

    def set_levels(
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
        return self._requester.trade(spec, timeout, max_retries, self._streams)

    def close(
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
        return self._requester.trade(spec, timeout, max_retries, self._streams)

    def close_by(
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
        return self._requester.trade(spec, timeout, max_retries, self._streams)


class Orders(_Resource):
    def list(
        self, *, timeout: float | None = None, max_retries: int | None = None
    ) -> list[PendingOrderView]:
        """The pending orders."""
        spec = safe("pending_orders", "GET", self._account, "/orders")
        return cast(list[PendingOrderView], self._requester.call(spec, timeout, max_retries))

    def place(
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
        return self._requester.trade(spec, timeout, max_retries, self._streams)

    def check(
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
        return cast(CheckView, self._requester.call(spec, timeout, max_retries))

    def modify(
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
        return self._requester.trade(spec, timeout, max_retries, self._streams)

    def cancel(
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
        return self._requester.trade(spec, timeout, max_retries, self._streams)


class Requests(_Resource):
    def get(
        self, request_id: str, *, timeout: float | None = None, max_retries: int | None = None
    ) -> TradeRequest:
        """A recorded trade request by its id (`req_...`), whatever its state."""
        spec = lookup(self._account, request_id)
        return cast(TradeRequest, self._requester.call(spec, timeout, max_retries))

    def wait(self, request_id: str, *, timeout: float = 30.0) -> TradeRequest:
        """Look a request up until it is final or `timeout` seconds pass, then return it.

        It returns the request as it stands at the timeout (pending or unknown included): it
        never raises for "still pending", and it only ever looks up, never sends. A 401 or 404
        is raised; other failures are polled through until the timeout. With a stream open on
        this account handle, a request event for it ends the wait at once.
        """
        spec = lookup(self._account, request_id)
        return self._requester.wait(spec, request_id, timeout, self._streams)


class History(_Resource):
    def deals(
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
        return cast(list[DealView], self._requester.call(spec, timeout, max_retries))

    def orders(
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
        return cast(list[HistoryOrderView], self._requester.call(spec, timeout, max_retries))


class Symbols(_Resource):
    def list(
        self, *, timeout: float | None = None, max_retries: int | None = None
    ) -> list[SymbolSummaryView]:
        """Every symbol the broker has."""
        spec = safe("symbols", "GET", self._account, "/symbols")
        return cast(list[SymbolSummaryView], self._requester.call(spec, timeout, max_retries))

    def get(
        self, name: str, *, timeout: float | None = None, max_retries: int | None = None
    ) -> SymbolView:
        """A symbol's specification, by its name exactly as the broker spells it."""
        spec = safe("symbol", "GET", self._account, symbol_path(name))
        return cast(SymbolView, self._requester.call(spec, timeout, max_retries))

    def tick(
        self, name: str, *, timeout: float | None = None, max_retries: int | None = None
    ) -> TickView:
        """A symbol's latest price."""
        spec = safe("tick", "GET", self._account, symbol_path(name) + "/tick")
        return cast(TickView, self._requester.call(spec, timeout, max_retries))


class KillSwitch(_Resource):
    def get(
        self, *, timeout: float | None = None, max_retries: int | None = None
    ) -> KillSwitchView:
        """Whether the account's kill switch is engaged."""
        spec = safe("kill_switch", "GET", self._account, "/kill-switch")
        return cast(KillSwitchView, self._requester.call(spec, timeout, max_retries))

    def engage(
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
        return cast(KillSwitchOutcome, self._requester.call(spec, timeout, max_retries))

    def lift(
        self, *, timeout: float | None = None, max_retries: int | None = None
    ) -> KillSwitchView:
        """Lift the kill switch, so new exposure is allowed again."""
        spec = safe("lift_kill_switch", "DELETE", self._account, "/kill-switch")
        return cast(KillSwitchView, self._requester.call(spec, timeout, max_retries))


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
