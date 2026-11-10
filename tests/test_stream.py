"""The stream helper's shells against a local fake stream and a fake REST API."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import threading
import time
from collections.abc import Callable, Iterator
from typing import Any

import httpx
import pytest

from pipsocket import (
    APITimeoutError,
    AsyncPipsocket,
    AuthenticationError,
    NotFound,
    Pipsocket,
    StreamError,
    StreamHandshakeError,
    TradeRequest,
    _stream_core,
)
from pipsocket.stream import (  # pyright: ignore[reportPrivateUsage]
    Engine,
    Settled,
    _AsyncSink,
)

from .fake_stream import FakeStreamServer
from .fakes import ACCOUNT, API_KEY, FakeTransport

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
        if path in ("/positions", "/orders") or path.startswith("/history/"):
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
    http = httpx.Client(transport=FakeTransport(rest_api(**extra)))
    return Pipsocket(API_KEY, stream_url=server.url, http_client=http)


def async_client(server: FakeStreamServer, **extra: Any) -> AsyncPipsocket:
    http = httpx.AsyncClient(transport=FakeTransport(rest_api(**extra)))
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
    http = httpx.Client(transport=FakeTransport(rest_api()))
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


# Edges found in review


def test_the_api_key_is_never_logged(
    server: FakeStreamServer, caplog: pytest.LogCaptureFixture
) -> None:
    acct = sync_client(server).account(ACCOUNT)

    with caplog.at_level(logging.DEBUG, logger="websockets.client"), acct.stream() as events:
        next(events)

    client = "\n".join(r.getMessage() for r in caplog.records if r.name == "websockets.client")
    assert "Authorization" in client, "the upgrade's headers were logged"
    assert API_KEY not in client


def test_an_overflowing_batch_drops_its_tail_too() -> None:
    async def run() -> tuple[list[Any], list[Any]]:
        sink = _AsyncSink(2)
        engine = engine_with(sink)
        events = [{"type": "deal", "deal": {"ticket": n}} for n in range(5)]
        engine.dropped = None
        for event in events:
            await engine.do(_stream_core.Emit(event))
        return list(sink.items), engine.dropped or []

    queued, dropped = asyncio.run(run())
    assert queued == []
    assert [d["deal"]["ticket"] for d in dropped] == [0, 1, 2, 3, 4]


async def never_read(read: _stream_core.Read) -> Any:
    raise AssertionError("no read expected")


def engine_with(sink: _AsyncSink) -> Engine:
    machine = _stream_core.StreamMachine(random=lambda: 1.0)
    return Engine(
        url="ws://unused", api_key=API_KEY, machine=machine, reader=never_read,
        sink=sink, settled=Settled(),
    )  # fmt: skip


def test_a_timer_replaces_the_one_before_it() -> None:
    async def run() -> int:
        engine = engine_with(_AsyncSink(10))
        for generation in range(100):
            await engine.do(_stream_core.StartTimer("ping", 0.0, generation))
        await asyncio.sleep(0.01)
        handles = list(engine.timers.values())
        await engine.shut()
        return len(handles)

    assert asyncio.run(run()) == 1


def slow_lookup_api(delay: float) -> Callable[[httpx.Request], Any]:
    async def handle(request: httpx.Request) -> httpx.Response:
        if "/requests/" in request.url.path:
            await asyncio.sleep(delay)
        return rest_api()(request)

    return handle


def test_a_stream_settlement_beats_a_slow_lookup_async(server: FakeStreamServer) -> None:
    async def run() -> tuple[str, float]:
        http = httpx.AsyncClient(transport=FakeTransport(slow_lookup_api(0.5)))
        ps = AsyncPipsocket(API_KEY, stream_url=server.url, http_client=http)
        acct = ps.account(ACCOUNT)
        async with ps, acct.stream() as events:
            await anext(events)
            loop = asyncio.get_running_loop()
            loop.call_later(
                0.05, server.send, {**pending(), "state": "completed", "type": "request"}
            )
            started = time.monotonic()
            request = await acct.requests.wait(REQUEST_ID, timeout=0.1)
            return request.state, time.monotonic() - started

    state, took = asyncio.run(run())
    assert state == "completed"
    assert took < 0.4


def test_a_stream_settlement_beats_a_slow_lookup_sync(server: FakeStreamServer) -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        if "/requests/" in request.url.path:
            time.sleep(0.5)
        return rest_api()(request)

    http = httpx.Client(transport=FakeTransport(handle))
    acct = Pipsocket(API_KEY, stream_url=server.url, http_client=http).account(ACCOUNT)
    with acct.stream() as events:
        next(events)
        threading.Timer(
            0.05, server.send, [{**pending(), "state": "completed", "type": "request"}]
        ).start()
        started = time.monotonic()
        request = acct.requests.wait(REQUEST_ID, timeout=0.1)

    assert request.state == "completed"
    assert time.monotonic() - started < 0.4


def test_an_event_before_the_rest_answer_wins(server: FakeStreamServer) -> None:
    """Row 13: the stream's settlement arrives before the trade's slower 202 pending."""
    settled_event = {**pending(), "state": "completed", "type": "request"}

    def handle(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            server.send(settled_event)
            time.sleep(0.3)
            return httpx.Response(202, json=pending())
        return rest_api()(request)

    http = httpx.Client(transport=FakeTransport(handle))
    acct = Pipsocket(API_KEY, stream_url=server.url, http_client=http).account(ACCOUNT)
    with acct.stream() as events:
        next(events)
        order = acct.orders.place(symbol="EURUSD", type="buy", volume=0.1, type_filling="ioc")
        waited = acct.requests.wait(REQUEST_ID, timeout=0.1)

    assert order.state == "completed"
    assert order.idempotency_key is not None
    assert waited.state == "completed"


def test_a_401_on_a_wait_lookup_stops_the_stream(server: FakeStreamServer) -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        if "/requests/" in request.url.path:
            return httpx.Response(401, json={"error": "invalid_api_key", "message": "Rolled."})
        return rest_api()(request)

    http = httpx.Client(transport=FakeTransport(handle))
    acct = Pipsocket(API_KEY, stream_url=server.url, http_client=http).account(ACCOUNT)
    with acct.stream() as events:
        next(events)
        with pytest.raises(AuthenticationError):
            acct.requests.wait(REQUEST_ID, timeout=1)
        with pytest.raises(AuthenticationError):
            next(events)


def test_an_async_wait_with_a_stream_open_times_out_cleanly(server: FakeStreamServer) -> None:
    async def run() -> str:
        ps = async_client(server)
        acct = ps.account(ACCOUNT)
        async with ps, acct.stream() as events:
            await anext(events)
            return (await acct.requests.wait(REQUEST_ID, timeout=0.6)).state

    assert asyncio.run(run()) == "pending"


def test_a_settlement_seen_before_watching_is_given_at_once() -> None:
    registry = Settled()
    completed = {**pending(), "state": "completed", "type": "request"}
    registry.saw(completed)
    seen: list[Any] = []

    registry.watch(REQUEST_ID, seen.append)

    assert seen == [completed]


def test_an_overflow_is_handled_before_the_next_input(server: FakeStreamServer) -> None:
    acct = sync_client(server).account(ACCOUNT)

    with acct.stream(queue_size=1) as events:
        assert next(events)["reason"] == "connected"
        server.send({"type": "account", "server_time": 500})
        assert next(events)["server_time"] == 500
        for server_time in (1000, 2000, 3000):
            server.send({"type": "account", "server_time": server_time})
        time.sleep(0.3)
        resync = next(events)

    assert (resync["type"], resync["reason"]) == ("resync", "overflow")
    assert resync["history"]["from"] == 500 - 300, "read back from before the lost events"


def test_a_401_from_a_lookup_the_stream_beat_still_stops_it(server: FakeStreamServer) -> None:
    def handle(request: httpx.Request) -> httpx.Response:
        if "/requests/" in request.url.path:
            time.sleep(0.3)
            return httpx.Response(401, json={"error": "invalid_api_key", "message": "Rolled."})
        return rest_api()(request)

    http = httpx.Client(transport=FakeTransport(handle))
    acct = Pipsocket(API_KEY, stream_url=server.url, http_client=http).account(ACCOUNT)
    with acct.stream() as events:
        next(events)
        threading.Timer(
            0.05, server.send, [{**pending(), "state": "completed", "type": "request"}]
        ).start()
        assert acct.requests.wait(REQUEST_ID, timeout=5).state == "completed"
        time.sleep(0.5)
        assert next(events)["type"] == "request", "the settlement itself, delivered first"
        with pytest.raises(AuthenticationError):
            next(events)


def test_an_async_wait_leaves_no_task_behind(server: FakeStreamServer) -> None:
    async def run() -> list[str]:
        ps = async_client(server)
        acct = ps.account(ACCOUNT)
        async with ps, acct.stream() as events:
            await anext(events)
            for _ in range(3):
                await acct.requests.wait(REQUEST_ID, timeout=0.3)
            await asyncio.sleep(0)  # a cancelled task ends on the loop's next turn
            return [repr(task) for task in asyncio.all_tasks() if "Event.wait" in repr(task)]

    assert asyncio.run(run()) == []


@pytest.mark.parametrize("flavour", ["sync", "async"])
def test_wait_with_a_stream_open_keeps_its_deadline(flavour: str, server: FakeStreamServer) -> None:
    """A lookup that stalls past the deadline ends the wait on time, stream open or not."""

    def stalls(request: httpx.Request) -> httpx.Response:
        if "/requests/" in request.url.path:
            time.sleep(0.6)
        return rest_api()(request)

    async def stalls_async(request: httpx.Request) -> httpx.Response:
        if "/requests/" in request.url.path:
            await asyncio.sleep(0.6)
        return rest_api()(request)

    if flavour == "sync":
        http = httpx.Client(transport=FakeTransport(stalls))
        acct = Pipsocket(API_KEY, stream_url=server.url, http_client=http).account(ACCOUNT)
        with acct.stream() as events:
            next(events)
            started = time.monotonic()
            with pytest.raises(APITimeoutError):
                acct.requests.wait(REQUEST_ID, timeout=0.1)
            took = time.monotonic() - started
    else:

        async def run() -> float:
            http = httpx.AsyncClient(transport=FakeTransport(stalls_async))
            ps = AsyncPipsocket(API_KEY, stream_url=server.url, http_client=http)
            acct = ps.account(ACCOUNT)
            async with ps, acct.stream() as events:
                await anext(events)
                started = time.monotonic()
                with pytest.raises(APITimeoutError):
                    await acct.requests.wait(REQUEST_ID, timeout=0.1)
                return time.monotonic() - started

        took = asyncio.run(run())
    assert took < 0.3


@pytest.mark.parametrize("delay", range(8))
def test_cancelling_a_wait_is_never_swallowed(delay: int) -> None:
    """The caller's cancellation, landing at any point after the lookup answers, ends the
    wait cancelled: it never returns the request instead."""

    async def run() -> asyncio.Task[Any]:
        loop = asyncio.get_running_loop()
        waiting: list[asyncio.Task[Any]] = []

        def cancel_after(rounds: int) -> None:
            if rounds:
                loop.call_soon(cancel_after, rounds - 1)
            else:
                waiting[0].cancel()

        def handler(request: httpx.Request) -> httpx.Response:
            loop.call_soon(cancel_after, delay)
            return httpx.Response(200, json=pending())

        ps = AsyncPipsocket(
            API_KEY, http_client=httpx.AsyncClient(transport=FakeTransport(handler))
        )
        task = asyncio.ensure_future(ps.account(ACCOUNT).requests.wait(REQUEST_ID, timeout=0.05))
        waiting.append(task)
        with contextlib.suppress(asyncio.CancelledError):
            await task
        return task

    assert asyncio.run(run()).cancelled()


def test_cancelling_the_caller_during_shutdown_isnt_swallowed(
    server: FakeStreamServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_shut = Engine.shut

    async def slow_shut(self: Engine) -> None:
        await asyncio.sleep(0.2)
        await real_shut(self)

    monkeypatch.setattr(Engine, "shut", slow_shut)

    async def run() -> bool:
        closing = asyncio.Event()

        async def use_stream() -> None:
            ps = async_client(server)
            async with ps, ps.account(ACCOUNT).stream() as events:
                await anext(events)
                closing.set()  # the context exit closes the stream next

        task = asyncio.ensure_future(use_stream())
        await closing.wait()
        await asyncio.sleep(0.05)  # shutdown is under way
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        return task.cancelled()

    assert asyncio.run(run())


def break_the_machine(monkeypatch: pytest.MonkeyPatch) -> None:
    """The machine raises a plain RuntimeError on the stream's first message."""
    real_handle = _stream_core.StreamMachine.handle

    def handle(self: _stream_core.StreamMachine, event: _stream_core.Input) -> Any:
        if isinstance(event, _stream_core.Message):
            raise RuntimeError("a bug in the SDK")
        return real_handle(self, event)

    monkeypatch.setattr(_stream_core.StreamMachine, "handle", handle)


def test_an_engine_failure_ends_a_sync_stream(
    server: FakeStreamServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    break_the_machine(monkeypatch)
    acct = sync_client(server).account(ACCOUNT)

    with acct.stream() as events, pytest.raises(StreamError) as caught:
        next(events)

    assert isinstance(caught.value.__cause__, RuntimeError)
    events.close()  # a second close is harmless


def test_an_engine_failure_ends_an_async_stream(
    server: FakeStreamServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    break_the_machine(monkeypatch)

    async def run() -> BaseException | None:
        ps = async_client(server)
        async with ps, ps.account(ACCOUNT).stream() as events:
            try:
                await asyncio.wait_for(anext(events), 5)
            except StreamError as error:
                return error.__cause__
        return None

    assert isinstance(asyncio.run(run()), RuntimeError)


def test_the_callers_own_exception_isnt_replaced(
    server: FakeStreamServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    break_the_machine(monkeypatch)

    async def run() -> None:
        ps = async_client(server)
        async with ps, ps.account(ACCOUNT).stream():
            await asyncio.sleep(0.2)  # the engine fails meanwhile
            raise ValueError("the caller's own")

    with pytest.raises(ValueError, match="the caller's own"):
        asyncio.run(run())
