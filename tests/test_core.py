"""The core's rules for a trade call, driven directly and through the shells' requester,
including against a real HTTP server on this machine. tests/test_outcomes.py walks the same
rules through the public trade methods.
"""

from __future__ import annotations

import asyncio
import http.server
import socket
import threading
from collections.abc import AsyncIterator, Iterator
from typing import Any, ClassVar

import hpack
import httpcore
import httpx
import hyperframe.frame
import pytest

from pipsocket import (
    APIConnectionError,
    AsyncPipsocket,
    AuthenticationError,
    EngineBusy,
    OutcomeUnknown,
    Pipsocket,
)
from pipsocket._core import (
    SEND,
    Answer,
    Attempt,
    BodyLost,
    Failed,
    NoAnswer,
    Settings,
    Wait,
    drive,
    trade,
)

from .fakes import ACCOUNT, API_KEY, FakeClock, FakeTransport

KEY = "rebalance-0412"


def run(
    attempts: list[Attempt], clock: FakeClock | None = None, sleep_for: float | None = None
) -> Any:
    """Drive a place_order spec through the scripted attempts; returns the value or raises."""
    clock = clock or FakeClock()
    spec = trade("place_order", "POST", ACCOUNT, "/orders", {"symbol": "EURUSD"}, KEY)
    steps = drive(spec, Settings(2, 60.0, clock, lambda: 0.5))
    step = next(steps)
    script = iter(attempts)
    try:
        while True:
            if step is SEND:
                step = steps.send(next(script))
            else:
                assert isinstance(step, Wait)
                clock.sleep(step.seconds if sleep_for is None else sleep_for)
                step = steps.send(None)
    except StopIteration as done:
        return done.value


def lost() -> NoAnswer:
    return NoAnswer(httpx.ReadError("reset"), written=True, timed_out=False)


def completed() -> Answer:
    return Answer(200, {}, b'{"request": "req_1", "state": "completed", "message": "Done."}')


def refused(status: int, code: str) -> Answer:
    body = f'{{"error": "{code}", "message": "..."}}'.encode()
    return Answer(status, {}, body)


def test_a_wait_that_runs_past_the_deadline_starts_no_new_attempt() -> None:
    with pytest.raises(OutcomeUnknown) as caught:
        run([lost(), completed()], sleep_for=61.0)

    assert caught.value.idempotency_key == KEY


def test_a_client_closed_after_a_lost_answer_is_still_outcome_unknown() -> None:
    closed = RuntimeError("Cannot send a request, as the client has been closed.")

    with pytest.raises(OutcomeUnknown) as caught:
        run([lost(), Failed(closed, maybe_sent=False)])

    assert caught.value.idempotency_key == KEY
    assert caught.value.cause.__cause__ is closed


def test_a_client_closed_before_anything_was_sent_raises_as_it_is() -> None:
    closed = RuntimeError("Cannot send a request, as the client has been closed.")

    with pytest.raises(RuntimeError) as caught:
        run([Failed(closed, maybe_sent=False)])

    assert caught.value is closed
    assert getattr(caught.value, "idempotency_key", None) == KEY


def test_a_definite_refusal_on_a_trade_carries_the_key() -> None:
    with pytest.raises(AuthenticationError) as caught:
        run([refused(401, "invalid_api_key")])

    assert caught.value.idempotency_key == KEY


def test_bytes_of_a_body_cut_short_are_kept() -> None:
    cut = BodyLost(200, httpx.ReadError("reset"), b'{"request": "req_')

    with pytest.raises(OutcomeUnknown) as caught:
        run([cut, cut, cut])

    assert (caught.value.status, caught.value.raw) == (200, b'{"request": "req_')


# Through the shells: a failure after sending began is not "nothing was sent".


class CloseFails(httpx.SyncByteStream, httpx.AsyncByteStream):
    """A body that is cut short, and whose cleanup fails too."""

    def __iter__(self) -> Iterator[bytes]:
        yield b'{"request": "req_'
        raise httpx.ReadError("reset")

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b'{"request": "req_'
        raise httpx.ReadError("reset")

    def close(self) -> None:
        raise httpx.CloseError("close failed")

    async def aclose(self) -> None:
        raise httpx.CloseError("close failed")


COMPLETED = b'{"request": "req_1", "state": "completed", "message": "Done."}'


def through_shell(flavour: str, handler: Any, hooks: dict[str, Any] | None = None) -> Any:
    """Run a place_order spec through the sync or async shell, with no retries."""
    spec = trade("place_order", "POST", ACCOUNT, "/orders", {"symbol": "EURUSD"}, KEY)
    if flavour == "sync":
        http = httpx.Client(transport=FakeTransport(handler), event_hooks=hooks or {})
        return Pipsocket(API_KEY, http_client=http)._requester.call(spec, None, 0)  # pyright: ignore[reportPrivateUsage]

    async def run_async() -> Any:
        async_hooks = {
            name: [_awaitable(hook) for hook in hooks_for_name]
            for name, hooks_for_name in (hooks or {}).items()
        }
        http = httpx.AsyncClient(transport=FakeTransport(handler), event_hooks=async_hooks)
        ps = AsyncPipsocket(API_KEY, http_client=http)
        return await ps._requester.call(spec, None, 0)  # pyright: ignore[reportPrivateUsage]

    return asyncio.run(run_async())


def _awaitable(hook: Any) -> Any:
    async def call(response: httpx.Response) -> None:
        hook(response)

    return call


@pytest.mark.parametrize("flavour", ["sync", "async"])
def test_a_failure_after_the_answer_arrived_is_outcome_unknown(flavour: str) -> None:
    def hook(response: httpx.Response) -> None:
        raise RuntimeError("a response hook failed")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=COMPLETED)

    with pytest.raises(OutcomeUnknown) as caught:
        through_shell(flavour, handler, {"response": [hook]})

    assert caught.value.idempotency_key == KEY
    assert isinstance(caught.value.cause.__cause__, RuntimeError)


@pytest.mark.parametrize("flavour", ["sync", "async"])
def test_a_failing_cleanup_keeps_what_the_attempt_came_to(flavour: str) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=CloseFails())

    with pytest.raises(OutcomeUnknown) as caught:
        through_shell(flavour, handler)

    assert (caught.value.status, caught.value.raw) == (200, b'{"request": "req_')


@pytest.mark.parametrize("flavour", ["sync", "async"])
def test_a_closed_client_before_sending_raises_as_it_is(flavour: str) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("nothing should be sent")

    spec = trade("place_order", "POST", ACCOUNT, "/orders", {"symbol": "EURUSD"}, KEY)
    if flavour == "sync":
        http = httpx.Client(transport=FakeTransport(handler))
        http.close()
        with pytest.raises(RuntimeError) as caught:
            Pipsocket(API_KEY, http_client=http)._requester.call(spec, None, 0)  # pyright: ignore[reportPrivateUsage]
    else:

        async def run_async() -> None:
            http = httpx.AsyncClient(transport=FakeTransport(handler))
            await http.aclose()
            await AsyncPipsocket(API_KEY, http_client=http)._requester.call(spec, None, 0)  # pyright: ignore[reportPrivateUsage]

        with pytest.raises(RuntimeError) as caught:
            asyncio.run(run_async())

    assert getattr(caught.value, "idempotency_key", None) == KEY


@pytest.mark.parametrize("flavour", ["sync", "async"])
@pytest.mark.parametrize("tagged", [False, True], ids=["untagged", "tagged-with-our-request"])
def test_a_connect_error_from_a_response_hook_is_outcome_unknown(
    flavour: str, tagged: bool
) -> None:
    def hook(response: httpx.Response) -> None:
        request = response.request if tagged else None
        raise httpx.ConnectError("a hook's own request could not connect", request=request)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=COMPLETED)

    with pytest.raises(OutcomeUnknown) as caught:
        through_shell(flavour, handler, {"response": [hook]})

    assert isinstance(caught.value.cause.__cause__, httpx.ConnectError)


class CompleteThenCloseFails(httpx.SyncByteStream, httpx.AsyncByteStream):
    """The whole body arrives, then closing the stream fails with `error`."""

    def __init__(self, error: Exception) -> None:
        self.error = error

    def __iter__(self) -> Iterator[bytes]:
        yield COMPLETED

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield COMPLETED

    def close(self) -> None:
        raise self.error

    async def aclose(self) -> None:
        raise self.error


@pytest.mark.parametrize("flavour", ["sync", "async"])
@pytest.mark.parametrize("error", [httpx.CloseError("close failed"), OSError("close failed")])
def test_a_complete_answer_survives_a_failing_close(flavour: str, error: Exception) -> None:
    sent: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        return httpx.Response(200, stream=CompleteThenCloseFails(error))

    request = through_shell(flavour, handler)

    assert request.state == "completed"
    assert len(sent) == 1


# Against a real HTTP server on this machine: httpx's own transport reads the request's body
# only once it has connected, which is what tells "never connected" from "maybe sent".


class Recorded(http.server.BaseHTTPRequestHandler):
    bodies: ClassVar[list[bytes]] = []

    def do_POST(self) -> None:
        length = int(self.headers["Content-Length"])
        Recorded.bodies.append(self.rfile.read(length))
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(COMPLETED)))
        self.end_headers()
        self.wfile.write(COMPLETED)

    def log_message(self, format: str, *args: Any) -> None:
        pass


@pytest.fixture
def local_api() -> Iterator[str]:
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Recorded)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


def real_call(flavour: str, base_url: str) -> Any:
    spec = trade("place_order", "POST", ACCOUNT, "/orders", {"symbol": "EURUSD"}, KEY)
    if flavour == "sync":
        ps = Pipsocket(API_KEY, base_url=base_url, timeout=2, max_retries=0)
        return ps._requester.call(spec, None, 0)  # pyright: ignore[reportPrivateUsage]

    async def run_async() -> Any:
        async with AsyncPipsocket(API_KEY, base_url=base_url, timeout=2, max_retries=0) as ps:
            return await ps._requester.call(spec, None, 0)  # pyright: ignore[reportPrivateUsage]

    return asyncio.run(run_async())


@pytest.mark.parametrize("flavour", ["sync", "async"])
def test_a_real_transport_sends_the_body(flavour: str, local_api: str) -> None:
    Recorded.bodies.clear()

    request = real_call(flavour, local_api)

    assert request.state == "completed"
    assert Recorded.bodies == [b'{"symbol":"EURUSD"}']


@pytest.mark.parametrize("flavour", ["sync", "async"])
def test_a_real_transport_that_cant_connect_sent_nothing(flavour: str) -> None:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]  # bound, not listening: refused (or, on macOS, timed out)

        with pytest.raises(APIConnectionError) as caught:
            real_call(flavour, f"http://127.0.0.1:{port}")

    # A connection failure (APIConnectionError or its subclass APITimeoutError), never
    # OutcomeUnknown: nothing was sent.
    assert isinstance(caught.value, APIConnectionError)
    assert caught.value.idempotency_key == KEY


# HTTP/2, with httpcore's own HTTP/2 connection over its in-memory network backend: a request
# without a body is sent as headers alone, so its body is never read.


def http2_answer() -> list[bytes]:
    headers = hpack.Encoder().encode([(b":status", b"200"), (b"content-type", b"application/json")])
    return [
        hyperframe.frame.SettingsFrame().serialize(),
        hyperframe.frame.HeadersFrame(stream_id=1, data=headers, flags=["END_HEADERS"]).serialize(),
        hyperframe.frame.DataFrame(stream_id=1, data=COMPLETED, flags=["END_STREAM"]).serialize(),
    ]


def cancel_over_http2(flavour: str, hook: Any) -> Any:
    spec = trade("cancel_order", "DELETE", ACCOUNT, "/orders/881402377", None, KEY)
    if flavour == "sync":
        transport = httpx.HTTPTransport(http1=False, http2=True)
        transport._pool = httpcore.ConnectionPool(  # pyright: ignore[reportPrivateUsage]
            http1=False,
            http2=True,
            network_backend=httpcore.MockBackend(http2_answer(), http2=True),
        )
        http = httpx.Client(transport=transport, event_hooks={"response": [hook]})
        ps = Pipsocket(API_KEY, base_url="http://api.example", http_client=http)
        return ps._requester.call(spec, None, 0)  # pyright: ignore[reportPrivateUsage]

    async def run_async() -> Any:
        transport = httpx.AsyncHTTPTransport(http1=False, http2=True)
        backend = httpcore.AsyncMockBackend(http2_answer(), http2=True)
        transport._pool = httpcore.AsyncConnectionPool(  # pyright: ignore[reportPrivateUsage]
            http1=False, http2=True, network_backend=backend
        )
        http = httpx.AsyncClient(transport=transport, event_hooks={"response": [_awaitable(hook)]})
        ps = AsyncPipsocket(API_KEY, base_url="http://api.example", http_client=http)
        return await ps._requester.call(spec, None, 0)  # pyright: ignore[reportPrivateUsage]

    return asyncio.run(run_async())


def check_http2(response: httpx.Response) -> None:
    assert response.http_version == "HTTP/2"


@pytest.mark.parametrize("flavour", ["sync", "async"])
def test_a_bodyless_http2_trade_answers(flavour: str) -> None:
    assert cancel_over_http2(flavour, check_http2).state == "completed"


@pytest.mark.parametrize("flavour", ["sync", "async"])
@pytest.mark.parametrize(
    "failure",
    [httpx.ConnectError("a hook's request could not connect"), RuntimeError("a hook failed")],
)
def test_a_hook_failure_after_a_bodyless_http2_trade_is_outcome_unknown(
    flavour: str, failure: Exception
) -> None:
    def hook(response: httpx.Response) -> None:
        check_http2(response)
        raise failure

    with pytest.raises(OutcomeUnknown) as caught:
        cancel_over_http2(flavour, hook)

    assert caught.value.idempotency_key == KEY


# Answers the parsers can't take are unreadable answers: maybe sent, retried with the same key.


@pytest.mark.parametrize("flavour", ["sync", "async"])
@pytest.mark.parametrize("retry_after", ["\u00b2", "9" * 400, "-1", "1.5"])
def test_an_unusable_retry_after_is_ignored(flavour: str, retry_after: str) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        body = b'{"error": "engine_timeout", "message": "Took too long."}'
        headers = [(b"Retry-After", retry_after.encode())]  # as it came on the wire
        return httpx.Response(504, headers=headers, content=body)

    with pytest.raises(OutcomeUnknown) as caught:
        through_shell(flavour, handler)

    assert caught.value.idempotency_key == KEY
    assert caught.value.status == 504


@pytest.mark.parametrize("flavour", ["sync", "async"])
def test_json_nested_too_deep_is_an_unreadable_answer(flavour: str) -> None:
    deep = b"[" * 100_000 + b"0" + b"]" * 100_000

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=deep)

    with pytest.raises(OutcomeUnknown) as caught:
        through_shell(flavour, handler)

    assert (caught.value.status, caught.value.raw) == (200, deep)
    assert caught.value.idempotency_key == KEY


# The clock, the random source or the sleep failing while a retry is planned.


def lost_answer(request: httpx.Request) -> httpx.Response:
    raise httpx.ReadError("connection reset after the request was written")


def broken(*args: Any) -> Any:
    raise RuntimeError("broken")


@pytest.mark.parametrize("flavour", ["sync", "async"])
@pytest.mark.parametrize("part", ["random", "sleep", "clock"])
def test_a_failing_scheduler_after_a_maybe_sent_attempt_is_outcome_unknown(
    flavour: str, part: str
) -> None:
    spec = trade("place_order", "POST", ACCOUNT, "/orders", {"symbol": "EURUSD"}, KEY)
    calls = {"clock": 0}

    def clock() -> float:
        calls["clock"] += 1
        if part == "clock" and calls["clock"] > 1:  # the start reads it once; then it fails
            raise RuntimeError("broken")
        return 1000.0

    options: dict[str, Any] = {"_clock": clock, "_random": broken if part == "random" else None}

    if flavour == "sync":
        http = httpx.Client(transport=FakeTransport(lost_answer))
        sync_sleep = broken if part == "sleep" else (lambda seconds: None)
        ps = Pipsocket(API_KEY, http_client=http, _sleep=sync_sleep, **options)
        with pytest.raises(OutcomeUnknown) as caught:
            ps._requester.call(spec, None, None)  # pyright: ignore[reportPrivateUsage]
    else:

        async def async_sleep(seconds: float) -> None:
            if part == "sleep":
                raise RuntimeError("broken")

        async def run() -> None:
            http = httpx.AsyncClient(transport=FakeTransport(lost_answer))
            ps = AsyncPipsocket(API_KEY, http_client=http, _sleep=async_sleep, **options)
            await ps._requester.call(spec, None, None)  # pyright: ignore[reportPrivateUsage]

        with pytest.raises(OutcomeUnknown) as caught:
            asyncio.run(run())

    assert caught.value.idempotency_key == KEY


def test_a_long_retry_sequence_doesnt_overflow_its_backoff() -> None:
    def busy(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, content=b'{"error": "engine_busy", "message": "Busy."}')

    clock = FakeClock()
    ps = Pipsocket(
        API_KEY,
        http_client=httpx.Client(transport=FakeTransport(busy)),
        max_retries=1100,
        retry_deadline=10**9,
        _clock=lambda: 1000.0,  # time never passes: every retry is planned
        _sleep=clock.sleep,
    )

    with pytest.raises(EngineBusy):
        ps.account(ACCOUNT).positions.list()

    assert len(clock.sleeps) == 1100
    assert max(clock.sleeps) <= 8.0
