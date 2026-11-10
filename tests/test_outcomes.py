"""Every transition of a trade call and of requests.wait(), walked.

The rows of the outcome walk that the shared conformance cases already pin are listed here with
the case that owns them; the others are tests in this file.

| Row | What happens | Where it is tested |
| --- | --- | --- |
| 1 | Answered within 5 s | conformance t-completed, t-placed, t-partial, t-rejected |
| 2 | No answer within 5 s: pending, then wait() | test_wait_polls_a_pending_request_until_final |
| 3 | pending turns unknown; wait() times out | test_wait_returns_an_unknown_request_as_it_stands |
| 4 | unknown settles later | test_wait_returns_when_an_unknown_request_settles |
| 5 | Answer lost after the host recorded it | conformance t-lost-then-ok, t-timeout-then-pending |
| 6 | The call never reached the host | conformance t-connect-failed-then-ok |
| 7 | Retries or the deadline run out | conformance t-lost-exhausted, t-deadline-passed |
| 8 | Lost, then 401, 404 or engine_busy | conformance t-lost-then-401, t-timeout-then-engine-busy |
| 9 | Lost, then 423 | conformance t-lost-then-kill-switch, t-lost-then-billing-hold |
| 10 | A retry races the first attempt | the host answers both with one request: as row 5 |
| 11 | A 200 cut short or not JSON | conformance t-200-cut-short-then-ok, t-200-not-json-exhausted |
| 12 | The caller cancels after the request was written | test_*cancel*, test_an_interrupt_* |
| 13 | The stream's event before the REST answer | the stream helper |
| 14 | The stream drops while an order is pending | wait() polls REST: as row 2 |
| 15 | The host disconnects while pending | test_wait_polls_through_the_host_being_away |
| 16 | Blocked by an earlier request | conformance t-busy-retry-after-then-ok, -turns-unknown |
| 17 | The terminal vanishes: not_sent | conformance t-not-sent |
| 18 | The same key with a different body | test_a_key_is_sent_as_given_..., ..._own_key |
| 19 | The process crashes after sending | test_a_saved_key_resolves_the_same_trade |
| 20 | A key reused after 7 days | not detectable: the README says so beside the journal |
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from typing import Any

import httpx
import pytest

import pipsocket
from pipsocket import (
    APIConnectionError,
    AsyncPipsocket,
    AuthenticationError,
    NotFound,
    OutcomeUnknown,
    TradeRequest,
    Unavailable,
)

from .fakes import (
    ACCOUNT,
    API_KEY,
    CutShort,
    FakeClock,
    FakeTransport,
    answer_lost,
    respond,
    scripted,
)

REQUEST_ID = "req_5f0c2a9e41d7b3a8c6e2f190"


def request_body(state: str, **extra: Any) -> dict[str, Any]:
    result = {"retcode": 10009, "retcode_name": "TRADE_RETCODE_DONE", "order": 1, "deal": 2}
    return {
        "request": REQUEST_ID,
        "state": state,
        "sent": {"symbol": "EURUSD"},
        "result": result if state in ("completed", "placed", "partial", "rejected") else None,
        "message": state,
        **extra,
    }


# Each answer is made fresh: an httpx.Response is read once.
def pending() -> httpx.Response:
    return respond(202, request_body("pending"))


def unknown() -> httpx.Response:
    return respond(200, request_body("unknown"))


def completed() -> httpx.Response:
    return respond(200, request_body("completed"))


def away() -> httpx.Response:
    return respond(503, {"error": "engine_unavailable", "message": "The host is away."})


def place(ps: pipsocket.Pipsocket, **extra: Any) -> TradeRequest:
    return ps.account(ACCOUNT).orders.place(
        symbol="EURUSD", type="buy", volume=0.1, type_filling="ioc", **extra
    )


# requests.wait()


def test_wait_polls_a_pending_request_until_final() -> None:
    clients = scripted(pending(), respond(200, request_body("pending")), completed())

    request = clients.sync().account(ACCOUNT).requests.wait(REQUEST_ID)

    assert request.state == "completed"
    assert clients.clock.sleeps == [0.25, 0.375]
    for sent in clients.transport.requests:
        assert (sent.method, sent.url.path) == (
            "GET",
            f"/v1/accounts/{ACCOUNT}/requests/{REQUEST_ID}",
        )


def test_wait_returns_an_unknown_request_as_it_stands() -> None:
    clients = scripted(*[unknown() for _ in range(40)])

    request = clients.sync().account(ACCOUNT).requests.wait(REQUEST_ID, timeout=10)

    assert request.state == "unknown"
    assert sum(clients.clock.sleeps) == pytest.approx(10)
    assert max(clients.clock.sleeps) == 2.0
    assert all(sent.method == "GET" for sent in clients.transport.requests)


def test_wait_returns_when_an_unknown_request_settles() -> None:
    clients = scripted(
        unknown(), unknown(), respond(200, request_body("not_executed", settlement="evidence"))
    )

    request = clients.sync().account(ACCOUNT).requests.wait(REQUEST_ID)

    assert (request.state, request.settlement, request.final) == ("not_executed", "evidence", True)


def test_wait_polls_through_the_host_being_away() -> None:
    clients = scripted(pending(), away(), away(), completed())

    request = clients.sync().account(ACCOUNT).requests.wait(REQUEST_ID)

    assert request.state == "completed"
    assert len(clients.transport.requests) == 4


@pytest.mark.parametrize(
    ("answer", "error"),
    [
        (respond(401, {"error": "invalid_api_key", "message": "Rolled."}), AuthenticationError),
        (respond(404, {"error": "no_such_request", "message": "Gone."}), NotFound),
    ],
)
def test_wait_raises_what_wont_change(answer: httpx.Response, error: type[Exception]) -> None:
    clients = scripted(pending(), answer)

    with pytest.raises(error):
        clients.sync().account(ACCOUNT).requests.wait(REQUEST_ID)

    assert len(clients.transport.requests) == 2


def test_wait_with_no_answer_at_all_raises_the_last_failure() -> None:
    clients = scripted(*[away() for _ in range(40)])

    with pytest.raises(Unavailable):
        clients.sync().account(ACCOUNT).requests.wait(REQUEST_ID, timeout=5)

    assert sum(clients.clock.sleeps) == pytest.approx(5)


def test_wait_with_no_time_looks_once() -> None:
    clients = scripted(pending())

    request = clients.sync().account(ACCOUNT).requests.wait(REQUEST_ID, timeout=0)

    assert request.state == "pending"
    assert clients.clock.sleeps == []


def test_wait_gives_a_lookup_no_more_than_the_time_left() -> None:
    clients = scripted(pending(), completed())

    clients.sync(timeout=40).account(ACCOUNT).requests.wait(REQUEST_ID, timeout=12)

    first, _ = clients.transport.requests
    assert first.extensions["timeout"]["read"] == 12


def test_wait_returns_an_unrecognised_state_at_the_timeout() -> None:
    clients = scripted(*[respond(200, request_body("superseded")) for _ in range(20)])

    request = clients.sync().account(ACCOUNT).requests.wait(REQUEST_ID, timeout=3)

    assert request.state == "superseded"
    assert not request.final


def test_wait_async() -> None:
    clients = scripted(pending(), completed())

    async def run() -> TradeRequest:
        async with clients.asyncio() as ps:
            return await ps.account(ACCOUNT).requests.wait(REQUEST_ID)

    assert asyncio.run(run()).state == "completed"
    assert clients.clock.sleeps == [0.25]


# Keys


def test_each_call_makes_its_own_key() -> None:
    clients = scripted(completed(), completed())
    ps = clients.sync()

    first, second = place(ps), place(ps)

    keys = [sent.headers["Idempotency-Key"] for sent in clients.transport.requests]
    assert keys[0] != keys[1]
    assert [first.idempotency_key, second.idempotency_key] == keys
    for key in keys:
        assert uuid.UUID(key).version == 4


def test_a_saved_key_resolves_the_same_trade() -> None:
    key = pipsocket.new_key()
    clients = scripted(answer_lost(), answer_lost(), answer_lost(), completed())

    with pytest.raises(OutcomeUnknown) as caught:
        place(clients.sync(), idempotency_key=key)
    request = place(clients.sync(), idempotency_key=caught.value.idempotency_key)

    assert caught.value.idempotency_key == key
    assert request.idempotency_key == key
    assert {sent.headers["Idempotency-Key"] for sent in clients.transport.requests} == {key}


@pytest.mark.parametrize("key", ["a", "rebalance 0412", "k" * 128, "~!@#$%^&*()"])
def test_keys_the_api_accepts(key: str) -> None:
    clients = scripted(completed())

    assert place(clients.sync(), idempotency_key=key).idempotency_key == key


@pytest.mark.parametrize("key", [" leading", "trailing ", "café", "tab\there", "hk:x"])
def test_keys_the_api_refuses_are_refused_before_sending(key: str) -> None:
    clients = scripted()

    with pytest.raises(ValueError):
        place(clients.sync(), idempotency_key=key)

    assert clients.transport.requests == []


# What the trade calls send


def test_fields_left_out_are_not_sent() -> None:
    clients = scripted(completed())

    place(clients.sync(), sl=1.079, magic=None)

    (sent,) = clients.transport.requests
    assert json.loads(sent.content) == {
        "symbol": "EURUSD",
        "type": "buy",
        "volume": 0.1,
        "type_filling": "ioc",
        "sl": 1.079,
    }
    assert sent.headers["Content-Type"] == "application/json"


@pytest.mark.parametrize("ticket", [-1, True, 1.5, "881402311"])
def test_a_ticket_must_be_a_whole_number(ticket: Any) -> None:
    clients = scripted()

    with pytest.raises(ValueError):
        clients.sync().account(ACCOUNT).positions.close(ticket, volume=0.1, type_filling="ioc")

    assert clients.transport.requests == []


def test_engaging_the_kill_switch_sends_a_body_only_with_a_deviation() -> None:
    outcome = {"flat": True, "cancelled": [], "closed": [], "unanswered": [], "kill_switch": {}}
    clients = scripted(respond(200, outcome), respond(200, outcome))
    switch = clients.sync().account(ACCOUNT).kill_switch

    switch.engage()
    switch.engage(deviation=20)

    plain, with_deviation = clients.transport.requests
    assert plain.content == b""
    assert json.loads(with_deviation.content) == {"deviation": 20}
    assert "Idempotency-Key" not in plain.headers


def test_check_sends_no_key() -> None:
    clients = scripted(respond(200, {"sent": {}, "result": {"passed": True}, "message": "Ok."}))

    clients.sync().account(ACCOUNT).orders.check(
        symbol="EURUSD", type="buy", volume=0.1, type_filling="ioc"
    )

    (sent,) = clients.transport.requests
    assert "Idempotency-Key" not in sent.headers


# Row 12: the caller cancels


def test_an_interrupt_passes_through_with_the_key_and_nothing_more_is_sent() -> None:
    clients = scripted(KeyboardInterrupt(), completed())

    with pytest.raises(KeyboardInterrupt) as caught:
        place(clients.sync(), idempotency_key="rebalance-0412")

    assert getattr(caught.value, "idempotency_key", None) == "rebalance-0412"
    assert len(clients.transport.requests) == 1


def test_an_async_cancel_after_writing_passes_through_and_nothing_more_is_sent() -> None:
    sent: list[httpx.Request] = []

    async def run() -> None:
        written = asyncio.Event()

        async def hang(request: httpx.Request) -> httpx.Response:
            sent.append(request)
            written.set()
            await asyncio.sleep(3600)
            raise AssertionError("not reached")

        clock = FakeClock()
        ps = AsyncPipsocket(
            API_KEY,
            http_client=httpx.AsyncClient(transport=FakeTransport(hang)),
            _clock=clock,
            _sleep=clock.async_sleep,
        )
        raised: list[BaseException] = []

        async def placing() -> None:
            try:
                await ps.account(ACCOUNT).orders.place(
                    symbol="EURUSD", type="buy", volume=0.1, type_filling="ioc"
                )
            except asyncio.CancelledError as error:
                raised.append(error)  # the error the SDK passed through, inside the task
                raise

        order = asyncio.create_task(placing())
        await written.wait()
        order.cancel()
        with pytest.raises(asyncio.CancelledError):
            await order
        key = sent[0].headers["Idempotency-Key"]
        assert getattr(raised[0], "idempotency_key", None) == key
        await asyncio.sleep(0)
        assert clock.sleeps == []

    asyncio.run(run())
    assert len(sent) == 1


def test_an_async_cancel_during_the_wait_between_attempts_sends_nothing_more() -> None:
    clients = scripted(answer_lost(), completed())

    async def cancelled_sleep(seconds: float) -> None:
        raise asyncio.CancelledError

    async def run() -> None:
        ps = AsyncPipsocket(
            API_KEY,
            http_client=httpx.AsyncClient(transport=FakeTransport(clients.transport.handle)),
            _clock=clients.clock,
            _sleep=cancelled_sleep,
        )
        with pytest.raises(asyncio.CancelledError):
            await ps.account(ACCOUNT).orders.cancel(881402377)

    asyncio.run(run())
    assert len(clients.transport.requests) == 1


def test_a_failure_that_is_the_sdks_own_is_not_relabelled() -> None:
    clients = scripted(answer_lost(), answer_lost(), answer_lost())

    with pytest.raises(OutcomeUnknown) as caught:
        place(clients.sync())

    assert isinstance(caught.value.cause, APIConnectionError)
    assert caught.value.idempotency_key == clients.transport.requests[0].headers["Idempotency-Key"]


# The edges of a call


def test_a_wait_that_runs_past_the_deadline_starts_no_new_attempt() -> None:
    clients = scripted(answer_lost(), completed())
    ps = pipsocket.Pipsocket(
        API_KEY,
        http_client=httpx.Client(transport=FakeTransport(clients.transport.handle)),
        _clock=clients.clock,
        _sleep=lambda seconds: clients.clock.advance(61),
    )

    with pytest.raises(OutcomeUnknown):
        place(ps)

    assert len(clients.transport.requests) == 1


def test_a_client_closed_after_a_lost_answer_is_still_outcome_unknown() -> None:
    clients = scripted(answer_lost(), completed())
    http = httpx.Client(transport=FakeTransport(clients.transport.handle))
    ps = pipsocket.Pipsocket(
        API_KEY, http_client=http, _clock=clients.clock, _sleep=lambda seconds: http.close()
    )

    with pytest.raises(OutcomeUnknown) as caught:
        place(ps, idempotency_key="rebalance-0412")

    assert caught.value.idempotency_key == "rebalance-0412"
    assert isinstance(caught.value.cause.__cause__, RuntimeError)


def test_a_client_closed_before_anything_was_sent_raises_as_it_is() -> None:
    http = httpx.Client()
    http.close()
    ps = pipsocket.Pipsocket(API_KEY, http_client=http)

    with pytest.raises(RuntimeError) as caught:
        place(ps, idempotency_key="rebalance-0412")

    assert getattr(caught.value, "idempotency_key", None) == "rebalance-0412"


def test_a_definite_refusal_on_a_trade_carries_the_key() -> None:
    clients = scripted(respond(401, {"error": "invalid_api_key", "message": "Rolled."}))

    with pytest.raises(AuthenticationError) as caught:
        place(clients.sync(), idempotency_key="rebalance-0412")

    assert caught.value.idempotency_key == "rebalance-0412"


def test_bytes_of_a_body_cut_short_are_kept() -> None:
    clients = scripted(*[httpx.Response(200, stream=CutShort()) for _ in range(3)])

    with pytest.raises(OutcomeUnknown) as caught:
        place(clients.sync())

    assert (caught.value.status, caught.value.raw) == (200, b'{"request": "req_')


def test_a_failure_after_the_answer_arrived_is_outcome_unknown() -> None:
    def hook(response: httpx.Response) -> None:
        raise RuntimeError("a response hook failed")

    clients = scripted(completed())
    http = httpx.Client(
        transport=FakeTransport(clients.transport.handle), event_hooks={"response": [hook]}
    )

    with pytest.raises(OutcomeUnknown) as caught:
        place(pipsocket.Pipsocket(API_KEY, http_client=http, max_retries=0))

    assert isinstance(caught.value.cause.__cause__, RuntimeError)


def test_a_failure_after_the_answer_arrived_is_outcome_unknown_async() -> None:
    async def hook(response: httpx.Response) -> None:
        raise RuntimeError("a response hook failed")

    clients = scripted(completed())

    async def run() -> None:
        http = httpx.AsyncClient(
            transport=FakeTransport(clients.transport.handle),
            event_hooks={"response": [hook]},
        )
        ps = AsyncPipsocket(API_KEY, http_client=http, max_retries=0)
        await ps.account(ACCOUNT).orders.place(
            symbol="EURUSD", type="buy", volume=0.1, type_filling="ioc"
        )

    with pytest.raises(OutcomeUnknown):
        asyncio.run(run())


class CloseFails(httpx.SyncByteStream, httpx.AsyncByteStream):
    """A body cut short, whose cleanup fails too."""

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


def test_a_failing_cleanup_keeps_what_the_attempt_came_to() -> None:
    clients = scripted(httpx.Response(200, stream=CloseFails()))

    with pytest.raises(OutcomeUnknown) as caught:
        place(clients.sync(max_retries=0))

    assert (caught.value.status, caught.value.raw) == (200, b'{"request": "req_')


def test_a_failing_cleanup_keeps_what_the_attempt_came_to_async() -> None:
    clients = scripted(httpx.Response(200, stream=CloseFails()))

    async def run() -> None:
        async with clients.asyncio(max_retries=0) as ps:
            await ps.account(ACCOUNT).orders.cancel(881402377)

    with pytest.raises(OutcomeUnknown) as caught:
        asyncio.run(run())

    assert caught.value.raw == b'{"request": "req_'


# Row 18: the same key with a different body


def test_a_key_is_sent_as_given_and_the_first_request_comes_back() -> None:
    clients = scripted(completed(), completed())
    ps = clients.sync()

    first = place(ps, idempotency_key="rebalance-0412")
    second = ps.account(ACCOUNT).orders.place(
        symbol="EURUSD", type="sell", volume=0.2, type_filling="ioc",
        idempotency_key="rebalance-0412",
    )  # fmt: skip

    assert second.id == first.id, "the API answers with the first request and sends nothing"
    bodies = [json.loads(sent.content) for sent in clients.transport.requests]
    assert bodies[0] != bodies[1]
    keys = {sent.headers["Idempotency-Key"] for sent in clients.transport.requests}
    assert keys == {"rebalance-0412"}


# wait()'s deadline


def test_wait_gives_later_lookups_only_what_is_left() -> None:
    clients = scripted(pending(), pending(), pending(), pending(), pending(), pending())

    clients.sync().account(ACCOUNT).requests.wait(REQUEST_ID, timeout=1)

    timeouts = [sent.extensions["timeout"]["read"] for sent in clients.transport.requests]
    assert timeouts[0] == 1
    assert all(t <= 1 for t in timeouts[1:])
    assert sum(clients.clock.sleeps) == pytest.approx(1)


def test_wait_starts_no_lookup_after_an_oversleep() -> None:
    clients = scripted(pending(), respond(401, {"error": "invalid_api_key", "message": "x"}))
    ps = pipsocket.Pipsocket(
        API_KEY,
        http_client=httpx.Client(transport=FakeTransport(clients.transport.handle)),
        _clock=clients.clock,
        _sleep=lambda seconds: clients.clock.advance(seconds + 1.0),
    )

    request = ps.account(ACCOUNT).requests.wait(REQUEST_ID, timeout=0.1)

    assert request.state == "pending"
    assert len(clients.transport.requests) == 1


@pytest.mark.parametrize("flavour", ["sync", "async"])
def test_a_short_wait_gives_even_the_first_lookup_no_more(flavour: str) -> None:
    clients = scripted(completed())

    if flavour == "sync":
        clients.sync().account(ACCOUNT).requests.wait(REQUEST_ID, timeout=0.1)
    else:

        async def run() -> None:
            async with clients.asyncio() as ps:
                await ps.account(ACCOUNT).requests.wait(REQUEST_ID, timeout=0.1)

        asyncio.run(run())

    (sent,) = clients.transport.requests
    assert sent.extensions["timeout"]["read"] == pytest.approx(0.1)


def test_wait_with_no_time_looks_once_with_the_clients_timeout() -> None:
    clients = scripted(pending())

    clients.sync(timeout=40).account(ACCOUNT).requests.wait(REQUEST_ID, timeout=0)

    (sent,) = clients.transport.requests
    assert sent.extensions["timeout"]["read"] == 40


# A hook's failure comes after the answer arrived.


def hooked(clients: Any, hook: Any, **options: Any) -> pipsocket.Pipsocket:
    http = httpx.Client(
        transport=FakeTransport(clients.transport.handle), event_hooks={"response": [hook]}
    )
    return pipsocket.Pipsocket(
        API_KEY, http_client=http, _clock=clients.clock, _sleep=clients.clock.sleep, **options
    )


def failing_hook(response: httpx.Response) -> None:
    raise httpx.ConnectError("the hook's own request could not connect")


def failing_hook_naming_our_request(response: httpx.Response) -> None:
    raise httpx.ConnectError("could not connect", request=response.request)


@pytest.mark.parametrize("hook", [failing_hook, failing_hook_naming_our_request])
def test_a_connect_error_from_a_response_hook_is_outcome_unknown(hook: Any) -> None:
    clients = scripted(completed())

    with pytest.raises(OutcomeUnknown):
        place(hooked(clients, hook, max_retries=0))


def test_a_hook_failure_then_a_401_is_still_outcome_unknown() -> None:
    calls: list[int] = []

    def hook_once(response: httpx.Response) -> None:
        calls.append(1)
        if len(calls) == 1:
            failing_hook(response)

    clients = scripted(completed(), respond(401, {"error": "invalid_api_key", "message": "x"}))

    with pytest.raises(OutcomeUnknown) as caught:
        place(hooked(clients, hook_once))

    assert isinstance(caught.value.cause, AuthenticationError)


def test_a_connect_error_from_a_response_hook_is_outcome_unknown_async() -> None:
    async def hook(response: httpx.Response) -> None:
        raise httpx.ConnectError("the hook's own request could not connect")

    clients = scripted(completed())

    async def run() -> None:
        http = httpx.AsyncClient(
            transport=FakeTransport(clients.transport.handle),
            event_hooks={"response": [hook]},
        )
        ps = AsyncPipsocket(API_KEY, http_client=http, max_retries=0)
        await ps.account(ACCOUNT).orders.cancel(881402377)

    with pytest.raises(OutcomeUnknown):
        asyncio.run(run())


class CompleteThenCloseFails(httpx.SyncByteStream, httpx.AsyncByteStream):
    """The whole recorded request arrives, then closing the stream fails with `error`."""

    body = json.dumps(request_body("completed")).encode()

    def __init__(self, error: Exception) -> None:
        self.error = error

    def __iter__(self) -> Iterator[bytes]:
        yield self.body

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield self.body

    def close(self) -> None:
        raise self.error

    async def aclose(self) -> None:
        raise self.error


@pytest.mark.parametrize("flavour", ["sync", "async"])
@pytest.mark.parametrize("error", [httpx.CloseError("close failed"), OSError("close failed")])
def test_a_complete_answer_survives_a_failing_close(flavour: str, error: Exception) -> None:
    clients = scripted(httpx.Response(200, stream=CompleteThenCloseFails(error)))

    if flavour == "sync":
        request = place(clients.sync())
    else:

        async def run() -> TradeRequest:
            async with clients.asyncio() as ps:
                return await ps.account(ACCOUNT).orders.place(
                    symbol="EURUSD", type="buy", volume=0.1, type_filling="ioc"
                )

        request = asyncio.run(run())

    assert request.state == "completed"
    assert len(clients.transport.requests) == 1


# wait()'s deadline holds across a whole lookup, however the answer trickles in.


class Trickle(httpx.SyncByteStream, httpx.AsyncByteStream):
    """A pending request, in three pieces 0.06 s apart."""

    def __init__(self) -> None:
        body = json.dumps(request_body("pending")).encode()
        self.pieces = [body[:10], body[10:20], body[20:]]

    def __iter__(self) -> Iterator[bytes]:
        for piece in self.pieces:
            time.sleep(0.06)
            yield piece

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for piece in self.pieces:
            await asyncio.sleep(0.06)
            yield piece


@pytest.mark.parametrize("flavour", ["sync", "async"])
def test_wait_ends_on_time_however_slowly_the_answer_comes(flavour: str) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=Trickle())

    started = time.monotonic()
    if flavour == "sync":
        ps = pipsocket.Pipsocket(
            API_KEY, http_client=httpx.Client(transport=FakeTransport(handler))
        )
        with pytest.raises(pipsocket.APITimeoutError):
            ps.account(ACCOUNT).requests.wait(REQUEST_ID, timeout=0.1)
    else:

        async def run() -> None:
            http = httpx.AsyncClient(transport=FakeTransport(handler))
            async with AsyncPipsocket(API_KEY, http_client=http) as ps:
                await ps.account(ACCOUNT).requests.wait(REQUEST_ID, timeout=0.1)

        with pytest.raises(pipsocket.APITimeoutError):
            asyncio.run(run())
    assert time.monotonic() - started < 0.15


def test_wait_with_no_time_waits_for_its_one_lookup() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=Trickle())

    ps = pipsocket.Pipsocket(API_KEY, http_client=httpx.Client(transport=FakeTransport(handler)))

    assert ps.account(ACCOUNT).requests.wait(REQUEST_ID, timeout=0).state == "pending"


# The request's headers went out, then the connection failed: the whole of a bodyless
# request may have reached the host.


class FailsAfterHeaders(httpx.BaseTransport, httpx.AsyncBaseTransport):
    """Reports sending the headers (as httpcore does), then fails like a dropped connection.
    After that, answers with `then`, if given."""

    def __init__(self, then: list[httpx.Response] | None = None) -> None:
        self.then = then or []
        self.attempts = 0

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        self.attempts += 1
        if self.attempts > 1 and self.then:
            return self.then.pop(0)
        request.extensions["trace"]("http11.send_request_headers.started", {})
        raise httpx.ConnectError("connection dropped after the headers were sent")

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.attempts += 1
        if self.attempts > 1 and self.then:
            return self.then.pop(0)
        await request.extensions["trace"]("http11.send_request_headers.started", {})
        raise httpx.ConnectError("connection dropped after the headers were sent")


@pytest.mark.parametrize("flavour", ["sync", "async"])
@pytest.mark.parametrize("then", ["nothing", "401"])
def test_a_failure_after_the_headers_went_out_is_outcome_unknown(flavour: str, then: str) -> None:
    answers = [respond(401, {"error": "invalid_api_key", "message": "x"})] if then == "401" else []
    transport = FailsAfterHeaders(answers)
    retries = 1 if answers else 0

    if flavour == "sync":
        ps = pipsocket.Pipsocket(
            API_KEY, http_client=httpx.Client(transport=transport), max_retries=retries,
            _sleep=lambda seconds: None,
        )  # fmt: skip
        with pytest.raises(OutcomeUnknown):
            ps.account(ACCOUNT).orders.cancel(881402377)
    else:

        async def run() -> None:
            async def no_sleep(seconds: float) -> None:
                pass

            http = httpx.AsyncClient(transport=transport)
            ps = AsyncPipsocket(API_KEY, http_client=http, max_retries=retries, _sleep=no_sleep)
            await ps.account(ACCOUNT).orders.cancel(881402377)

        with pytest.raises(OutcomeUnknown):
            asyncio.run(run())


class StallsThenClosesSlowly(httpx.AsyncByteStream):
    async def __aiter__(self) -> AsyncIterator[bytes]:
        await asyncio.sleep(5)
        yield b"{}"

    async def aclose(self) -> None:
        await asyncio.sleep(0.3)


def test_wait_keeps_its_deadline_when_a_cancelled_lookup_cleans_up_slowly() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=StallsThenClosesSlowly())

    async def run() -> float:
        http = httpx.AsyncClient(transport=FakeTransport(handler))
        ps = AsyncPipsocket(API_KEY, http_client=http)
        started = time.monotonic()
        with pytest.raises(pipsocket.APITimeoutError):
            await ps.account(ACCOUNT).requests.wait(REQUEST_ID, timeout=0.1)
        took = time.monotonic() - started
        await asyncio.sleep(0.4)  # let the cancelled lookup wind down before the loop closes
        return took

    assert asyncio.run(run()) < 0.2
