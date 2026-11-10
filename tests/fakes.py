"""Fakes at the network and time boundaries: a scripted HTTP transport, a clock and a sleep."""

from __future__ import annotations

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


class CutShort(httpx.SyncByteStream, httpx.AsyncByteStream):
    """A body that starts arriving, then the connection drops."""

    def __iter__(self) -> Iterator[bytes]:
        yield b'{"request": "req_'
        raise httpx.ReadError("connection reset while reading the body")

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b'{"request": "req_'
        raise httpx.ReadError("connection reset while reading the body")


# One scripted answer: an httpx.Response, an exception to raise, or a function of the request.
Reply = httpx.Response | Exception | Callable[[httpx.Request], httpx.Response]


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
        if isinstance(reply, Exception):
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
        http = httpx.Client(transport=httpx.MockTransport(self.transport.handle))
        return Pipsocket(
            API_KEY,
            http_client=http,
            _clock=self.clock,
            _sleep=self.clock.sleep,
            _random=self.random,
            **options,
        )

    def asyncio(self, **options: Any) -> AsyncPipsocket:
        http = httpx.AsyncClient(transport=httpx.MockTransport(self.transport.handle))
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
