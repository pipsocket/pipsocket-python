"""The stream helper's shells against a local fake stream and a fake REST API."""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable, Iterator
from typing import Any

import httpx
import pytest

from pipsocket import (
    AsyncPipsocket,
    AuthenticationError,
    NotFound,
    Pipsocket,
    StreamHandshakeError,
    TradeRequest,
    _stream_core,
)

from .fake_stream import FakeStreamServer
from .fakes import ACCOUNT, API_KEY

ACCOUNT_VIEW = {"id": ACCOUNT, "login": 5001, "server": "Demo-Server", "status": "connected"}
REQUEST_ID = "req_5f0c2a9e41d7b3a8c6e2f190"


def pending() -> dict[str, Any]:
    return {"request": REQUEST_ID, "state": "pending", "sent": {}, "result": None, "message": "..."}


def rest_api(
    lookups: Callable[[], dict[str, Any]] = pending,
) -> Callable[[httpx.Request], httpx.Response]:
    def handle(request: httpx.Request) -> httpx.Response:
        path = request.url.path.removeprefix(f"/v1/accounts/{ACCOUNT}")
        if path == "":
            return httpx.Response(200, json=ACCOUNT_VIEW)
        if path in ("/positions", "/orders"):
            return httpx.Response(200, json=[])
        if path == f"/requests/{REQUEST_ID}":
            return httpx.Response(200, json=lookups())
        return httpx.Response(404, json={"error": "no_such_account", "message": "..."})

    return handle


@pytest.fixture
def server() -> Iterator[FakeStreamServer]:
    fake = FakeStreamServer()
    yield fake
    fake.stop()


def sync_client(server: FakeStreamServer, **extra: Any) -> Pipsocket:
    http = httpx.Client(transport=httpx.MockTransport(rest_api(**extra)))
    return Pipsocket(API_KEY, stream_url=server.url, http_client=http)


def async_client(server: FakeStreamServer, **extra: Any) -> AsyncPipsocket:
    http = httpx.AsyncClient(transport=httpx.MockTransport(rest_api(**extra)))
    return AsyncPipsocket(API_KEY, stream_url=server.url, http_client=http)


def test_sync_stream_resyncs_then_delivers_events(server: FakeStreamServer) -> None:
    acct = sync_client(server).account(ACCOUNT)

    with acct.stream(ticks=["EURUSD"]) as events:
        first = next(events)
        server.send({"type": "tick", "symbol": "EURUSD", "bid": 1.0841, "time_msc": 1})
        second = next(events)

    assert first["type"] == "resync"
    assert (first["reason"], first["account"], first["positions"]) == (
        "connected",
        ACCOUNT_VIEW,
        [],
    )
    assert second == {"type": "tick", "symbol": "EURUSD", "bid": 1.0841, "time_msc": 1}
    (connection,) = server.connections
    assert connection.headers["Authorization"] == f"Bearer {API_KEY}"
    assert connection.headers["User-Agent"].startswith("pipsocket-python/")
    assert connection.received[0]["body"] == {"symbols": ["EURUSD"]}


def test_async_stream_resyncs_then_delivers_events(server: FakeStreamServer) -> None:
    async def run() -> list[dict[str, Any]]:
        ps = async_client(server)
        seen: list[dict[str, Any]] = []
        async with ps, ps.account(ACCOUNT).stream(ticks=["EURUSD"]) as events:
            seen.append(await anext(events))
            server.send({"type": "deal", "deal": {"ticket": 1}})
            seen.append(await anext(events))
        return seen

    first, second = asyncio.run(run())
    assert (first["type"], first["reason"]) == ("resync", "connected")
    assert second == {"type": "deal", "deal": {"ticket": 1}}


def test_a_dropped_socket_reconnects_sends_the_ticks_again_and_resyncs(
    server: FakeStreamServer,
) -> None:
    acct = sync_client(server).account(ACCOUNT)

    with acct.stream(ticks=["EURUSD"]) as events:
        assert next(events)["reason"] == "connected"
        events.set_ticks(["EURUSD", "XAUUSD"])
        server.wait_for(lambda: len(server.connections[0].received) == 2)
        server.drop()
        again = next(events)

    assert (again["type"], again["reason"]) == ("resync", "reconnected")
    assert len(server.connections) == 2
    assert server.connections[1].received[0]["body"] == {"symbols": ["EURUSD", "XAUUSD"]}


@pytest.mark.parametrize(
    ("status", "body", "error"),
    [
        (401, {"error": "invalid_api_key", "message": "Not a current key."}, AuthenticationError),
        (404, {"error": "no_such_account", "message": "No such account."}, NotFound),
        (426, {"error": "upgrade_required", "message": "Upgrade."}, StreamHandshakeError),
        (403, {}, StreamHandshakeError),
    ],
)
def test_a_refused_upgrade_is_raised_without_reconnecting(
    server: FakeStreamServer, status: int, body: dict[str, Any], error: type[Exception]
) -> None:
    server.refuse = lambda _: (status, body)
    acct = sync_client(server).account(ACCOUNT)

    with acct.stream() as events, pytest.raises(error) as caught:
        next(events)

    assert getattr(caught.value, "status", None) == status
    assert server.upgrades == 1


def test_a_failing_upgrade_backs_off_and_connects(server: FakeStreamServer) -> None:
    server.refuse = lambda attempt: (
        (503, {"error": "engine_unavailable", "message": "..."}) if attempt == 1 else None
    )
    acct = sync_client(server).account(ACCOUNT)

    with acct.stream() as events:
        event = next(events)

    assert (event["type"], event["reason"]) == ("resync", "connected")
    assert server.upgrades == 2


def test_an_unreachable_stream_keeps_trying_until_closed() -> None:
    http = httpx.Client(transport=httpx.MockTransport(rest_api()))
    ps = Pipsocket(API_KEY, stream_url="ws://127.0.0.1:9", http_client=http)

    with ps.account(ACCOUNT).stream() as events:
        time.sleep(1.0)
    with pytest.raises(StopIteration):
        next(events)


def test_wait_ends_on_a_stream_event(server: FakeStreamServer) -> None:
    async def run() -> tuple[TradeRequest, float]:
        ps = async_client(server)
        acct = ps.account(ACCOUNT)
        async with ps, acct.stream() as events:
            await anext(events)
            started = time.monotonic()
            waiting = asyncio.ensure_future(acct.requests.wait(REQUEST_ID, timeout=20))
            await asyncio.sleep(0.3)
            completed = {**pending(), "state": "completed", "type": "request"}
            server.send(completed)
            request = await waiting
            return request, time.monotonic() - started

    request, took = asyncio.run(run())
    assert request.state == "completed"
    assert took < 5


def test_sync_wait_ends_on_a_stream_event(server: FakeStreamServer) -> None:
    acct = sync_client(server).account(ACCOUNT)

    with acct.stream() as events:
        next(events)
        server.wait_for(lambda: len(server.connections) == 1)
        started = time.monotonic()

        def settle() -> None:
            time.sleep(0.3)
            server.send({**pending(), "state": "completed", "type": "request"})

        threading.Thread(target=settle).start()
        request = acct.requests.wait(REQUEST_ID, timeout=20)

    assert request.state == "completed"
    assert time.monotonic() - started < 5


def test_pings_are_sent(server: FakeStreamServer, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_stream_core, "PING_EVERY", 0.1)
    acct = sync_client(server).account(ACCOUNT)

    with acct.stream() as events:
        next(events)
        server.wait_for(lambda: server.connections[0].received.count("ping") >= 2)


def test_close_ends_the_iteration(server: FakeStreamServer) -> None:
    acct = sync_client(server).account(ACCOUNT)
    events = acct.stream()
    with events:
        next(events)
    with pytest.raises(StopIteration):
        next(events)
