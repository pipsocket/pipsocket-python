"""Fakes at the network and time boundaries: a scripted HTTP transport, a clock and a sleep."""

from __future__ import annotations

import inspect
import json
from collections.abc import AsyncIterator, Callable, Iterator
from dataclasses import dataclass, field
from typing import Any

import httpx

from pipsocket import AsyncPipsocket, Pipsocket

ACCOUNT = "acc_01m6tx3k9f2qw8r7bznd5yvh4c"
API_KEY = "psk_test_0000000000000000000000000000000000000"
BASE_URL = "https://api.pipsocket.com"


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds

    async def async_sleep(self, seconds: float) -> None:
        self.sleep(seconds)


class FakeTransport(httpx.BaseTransport, httpx.AsyncBaseTransport):
    """Answers with `handler`, in the order httpcore works and reporting the same `trace`
    events: a failure to connect is reported as such before anything is sent; otherwise the
    request's headers, then its body, are sent before the answer comes back."""

    def __init__(self, handler: Callable[[httpx.Request], Any]) -> None:
        self.handler = handler

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        trace = request.extensions.get("trace")
        try:
            response = self.handler(request)
        except (httpx.ConnectError, httpx.ConnectTimeout):
            if trace is not None:
                trace("connection.connect_tcp.failed", {})
            raise
        except BaseException:
            self.send(request, trace)
            raise
        self.send(request, trace)
        return response

    def send(self, request: httpx.Request, trace: Any) -> None:
        if trace is not None:
            trace("http11.send_request_headers.started", {})
        for _ in request.stream:  # type: ignore[union-attr]
            pass

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        trace = request.extensions.get("trace")
        try:
            response = self.handler(request)
            if inspect.isawaitable(response):
                response = await response
        except (httpx.ConnectError, httpx.ConnectTimeout):
            if trace is not None:
                await trace("connection.connect_tcp.failed", {})
            raise
        except BaseException:
            await self.asend(request, trace)
            raise
        await self.asend(request, trace)
        return response

    async def asend(self, request: httpx.Request, trace: Any) -> None:
        if trace is not None:
            await trace("http11.send_request_headers.started", {})
        async for _ in request.stream:  # type: ignore[union-attr]
            pass


class CutShort(httpx.SyncByteStream, httpx.AsyncByteStream):
    """A body that starts arriving, then the connection drops."""

    def __iter__(self) -> Iterator[bytes]:
        yield b'{"request": "req_'
        raise httpx.ReadError("connection reset while reading the body")

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b'{"request": "req_'
        raise httpx.ReadError("connection reset while reading the body")


# One scripted answer: an httpx.Response, an exception to raise, or a function of the request.
Reply = httpx.Response | BaseException | Callable[[httpx.Request], httpx.Response]


@dataclass
class Transport:
    """Answers attempt n with replies[n], and keeps every request it was sent."""

    replies: list[Reply]
    clock: FakeClock
    elapsed: list[float] = field(default_factory=list[float])
    requests: list[httpx.Request] = field(default_factory=list[httpx.Request])

    def handle(self, request: httpx.Request) -> httpx.Response:
        index = len(self.requests)
        self.requests.append(request)
        if index >= len(self.replies):
            raise AssertionError(
                f"Attempt {index + 1} was not expected: {len(self.replies)} scripted"
            )
        if index < len(self.elapsed):
            self.clock.advance(self.elapsed[index])
        reply = self.replies[index]
        if isinstance(reply, BaseException):
            raise reply
        if isinstance(reply, httpx.Response):
            return reply
        return reply(request)


def respond(
    status: int, body: Any = None, *, text: str | None = None, headers: dict[str, str] | None = None
) -> httpx.Response:
    if text is not None:
        return httpx.Response(status, headers=headers, content=text.encode())
    return httpx.Response(status, headers=headers, content=json.dumps(body).encode())


def connect_failed() -> httpx.ConnectError:
    return httpx.ConnectError("connection refused")


def answer_lost() -> httpx.ReadTimeout:
    return httpx.ReadTimeout("timed out waiting for the answer")


@dataclass
class Clients:
    transport: Transport
    clock: FakeClock
    random: Callable[[], float] = lambda: 0.5

    def sync(self, **options: Any) -> Pipsocket:
        http = httpx.Client(transport=FakeTransport(self.transport.handle))
        return Pipsocket(
            API_KEY,
            http_client=http,
            _clock=self.clock,
            _sleep=self.clock.sleep,
            _random=self.random,
            **options,
        )

    def asyncio(self, **options: Any) -> AsyncPipsocket:
        http = httpx.AsyncClient(transport=FakeTransport(self.transport.handle))
        return AsyncPipsocket(
            API_KEY,
            http_client=http,
            _clock=self.clock,
            _sleep=self.clock.async_sleep,
            _random=self.random,
            **options,
        )


def scripted(*replies: Reply, elapsed: list[float] | None = None) -> Clients:
    clock = FakeClock()
    return Clients(Transport(list(replies), clock, elapsed or []), clock)
