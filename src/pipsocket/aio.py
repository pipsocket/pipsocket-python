"""The asynchronous client: the same calls as Pipsocket, awaited."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from types import TracebackType
from typing import Any, cast

import httpx

from ._core import (
    SEND,
    Attempt,
    Failed,
    Spec,
    Wait,
    account_path,
    drive,
    history_range,
    lookup,
    safe,
    symbol_path,
)
from ._http import (
    DEFAULT_MAX_RETRIES,
    DEFAULT_RETRY_DEADLINE,
    DEFAULT_TIMEOUT,
    Options,
    answer,
    body_lost,
    build_request,
    no_answer,
    options,
    watch_response,
)
from .models import TradeRequest
from .types import (
    AccountView,
    DealView,
    HistoryOrderView,
    KillSwitchView,
    PendingOrderView,
    PositionView,
    SymbolSummaryView,
    SymbolView,
    TickView,
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
                    await self.sleep(step.seconds)
                    step = run.send(None)
        except StopIteration as done:
            return done.value
        finally:
            run.close()

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
            return Failed(error, maybe_sent=body.read_started)
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


class AsyncAccount:
    """One account's calls. Get it with `AsyncPipsocket.account(id)`."""

    def __init__(self, requester: _Requester, account_id: str) -> None:
        self._requester = requester
        self.id = account_id
        self.positions = Positions(requester, account_id)
        self.orders = Orders(requester, account_id)
        self.requests = Requests(requester, account_id)
        self.history = History(requester, account_id)
        self.symbols = Symbols(requester, account_id)
        self.kill_switch = KillSwitch(requester, account_id)

    async def get(
        self, *, timeout: float | None = None, max_retries: int | None = None
    ) -> AccountView:
        """The account: its terminal's status, and figures from the terminal's last report."""
        spec = safe("account", "GET", self.id, "/")
        return cast(AccountView, await self._requester.call(spec, timeout, max_retries))

    def __repr__(self) -> str:
        return f"AsyncAccount({self.id!r})"


class _Resource:
    def __init__(self, requester: _Requester, account_id: str) -> None:
        self._requester = requester
        self._account = account_id


class Positions(_Resource):
    async def list(
        self, *, timeout: float | None = None, max_retries: int | None = None
    ) -> list[PositionView]:
        """The open positions."""
        spec = safe("positions", "GET", self._account, "/positions")
        return cast(list[PositionView], await self._requester.call(spec, timeout, max_retries))


class Orders(_Resource):
    async def list(
        self, *, timeout: float | None = None, max_retries: int | None = None
    ) -> list[PendingOrderView]:
        """The pending orders."""
        spec = safe("pending_orders", "GET", self._account, "/orders")
        return cast(list[PendingOrderView], await self._requester.call(spec, timeout, max_retries))


class Requests(_Resource):
    async def get(
        self, request_id: str, *, timeout: float | None = None, max_retries: int | None = None
    ) -> TradeRequest:
        """A recorded trade request by its id (`req_...`), whatever its state."""
        spec = lookup(self._account, request_id)
        return cast(TradeRequest, await self._requester.call(spec, timeout, max_retries))


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
