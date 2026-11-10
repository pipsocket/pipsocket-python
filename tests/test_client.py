"""The client's options, headers and the edges of a call that the conformance cases don't cover."""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import httpx
import pytest

import pipsocket
from pipsocket import (
    APIConnectionError,
    APIError,
    APITimeoutError,
    AsyncPipsocket,
    AuthenticationError,
    Pipsocket,
    PipsocketError,
    Unavailable,
)

from .fakes import (
    ACCOUNT,
    API_KEY,
    CutShort,
    FakeTransport,
    answer_lost,
    connect_failed,
    respond,
    scripted,
)

POSITIONS = [{"ticket": 881402311, "symbol": "EURUSD"}]


def test_the_key_comes_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PIPSOCKET_KEY", API_KEY)
    clients = scripted(respond(200, POSITIONS))
    http = httpx.Client(transport=FakeTransport(clients.transport.handle))

    Pipsocket(http_client=http).account(ACCOUNT).positions.list()

    (request,) = clients.transport.requests
    assert request.headers["Authorization"] == f"Bearer {API_KEY}"


def test_no_key_is_refused_at_construction(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PIPSOCKET_KEY", raising=False)
    with pytest.raises(PipsocketError, match="PIPSOCKET_KEY"):
        Pipsocket()
    with pytest.raises(PipsocketError, match="PIPSOCKET_KEY"):
        AsyncPipsocket()


def test_every_call_names_the_sdk_and_runtime() -> None:
    clients = scripted(respond(200, POSITIONS))

    clients.sync().account(ACCOUNT).positions.list()

    (request,) = clients.transport.requests
    agent = request.headers["User-Agent"]
    assert agent.startswith(f"pipsocket-python/{pipsocket.__version__} ")
    assert "Python/" in agent
    assert request.headers["Accept"] == "application/json"
    assert str(request.url) == f"https://api.pipsocket.com/v1/accounts/{ACCOUNT}/positions"


def test_the_key_stays_out_of_reprs_and_errors() -> None:
    clients = scripted(respond(401, {"error": "invalid_api_key", "message": "Not a current key."}))
    ps = clients.sync(max_retries=0)

    with pytest.raises(AuthenticationError) as caught:
        ps.account(ACCOUNT).positions.list()

    for text in (repr(ps), repr(ps.account(ACCOUNT)), str(caught.value), repr(caught.value)):
        assert API_KEY not in text


def test_base_url_option() -> None:
    clients = scripted(respond(200, POSITIONS))

    clients.sync(base_url="http://localhost:8080/").account(ACCOUNT).positions.list()

    (request,) = clients.transport.requests
    assert str(request.url) == f"http://localhost:8080/v1/accounts/{ACCOUNT}/positions"


def test_names_in_the_path_are_escaped() -> None:
    clients = scripted(respond(200, {}), respond(200, {}))
    account = clients.sync().account(ACCOUNT)

    account.symbols.get("BTC/USD")
    account.symbols.tick("#AAPL")

    first, second = clients.transport.requests
    assert first.url.raw_path.decode().endswith("/symbols/BTC%2FUSD")
    assert second.url.raw_path.decode().endswith("/symbols/%23AAPL/tick")


@pytest.mark.parametrize(
    "call",
    [
        lambda ps: ps.account(""),
        lambda ps: ps.account(ACCOUNT).symbols.get(""),
        lambda ps: ps.account(ACCOUNT).requests.get(""),
        lambda ps: ps.account(ACCOUNT).history.deals(-1, 10),
        lambda ps: ps.account(ACCOUNT).history.orders(1.5, 10),
        lambda ps: ps.account(ACCOUNT).positions.list(max_retries=-1),
        lambda ps: ps.account(ACCOUNT).positions.list(timeout=0),
    ],
)
def test_bad_arguments_are_refused_before_sending(call: Callable[[Pipsocket], object]) -> None:
    clients = scripted()
    with pytest.raises(ValueError):
        call(clients.sync())
    assert clients.transport.requests == []


def test_per_call_timeout_and_retries() -> None:
    clients = scripted(respond(503, {"error": "engine_unavailable", "message": "Away."}))

    with pytest.raises(Unavailable):
        clients.sync(timeout=40).account(ACCOUNT).positions.list(timeout=5, max_retries=0)

    (request,) = clients.transport.requests
    assert request.extensions["timeout"] == httpx.Timeout(5).as_dict()


def test_default_timeout_is_forty_seconds_per_attempt() -> None:
    clients = scripted(respond(200, POSITIONS))

    clients.sync().account(ACCOUNT).positions.list()

    (request,) = clients.transport.requests
    assert request.extensions["timeout"] == httpx.Timeout(40).as_dict()


def test_a_read_that_keeps_timing_out_raises_api_timeout_error() -> None:
    clients = scripted(answer_lost(), answer_lost(), answer_lost())

    with pytest.raises(APITimeoutError):
        clients.sync().account(ACCOUNT).positions.list()

    assert len(clients.transport.requests) == 3


def test_a_read_cut_short_is_retried() -> None:
    clients = scripted(httpx.Response(200, stream=CutShort()), respond(200, POSITIONS))

    assert clients.sync().account(ACCOUNT).positions.list() == POSITIONS
    assert len(clients.transport.requests) == 2


def test_a_read_always_cut_short_raises_api_connection_error() -> None:
    clients = scripted(*[httpx.Response(200, stream=CutShort()) for _ in range(3)])

    with pytest.raises(APIConnectionError) as caught:
        clients.sync().account(ACCOUNT).positions.list()

    assert type(caught.value) is APIConnectionError


def test_a_read_stops_at_the_retry_deadline() -> None:
    clients = scripted(
        respond(503, {"error": "engine_unavailable", "message": "Away."}),
        elapsed=[60.0],
    )

    with pytest.raises(Unavailable):
        clients.sync().account(ACCOUNT).positions.list()

    assert len(clients.transport.requests) == 1
    assert clients.clock.sleeps == []


def test_a_read_honours_retry_after() -> None:
    clients = scripted(
        respond(503, {"error": "engine_busy", "message": "Busy."}, headers={"Retry-After": "3"}),
        respond(200, POSITIONS),
    )

    assert clients.sync().account(ACCOUNT).positions.list() == POSITIONS
    assert clients.clock.sleeps == [3.0]


def test_retry_deadline_option() -> None:
    clients = scripted(
        respond(503, {"error": "engine_unavailable", "message": "Away."}), elapsed=[9.0]
    )

    with pytest.raises(Unavailable):
        clients.sync(retry_deadline=9).account(ACCOUNT).positions.list()

    assert len(clients.transport.requests) == 1


def test_waits_double_between_attempts() -> None:
    busy = respond(503, {"error": "engine_busy", "message": "Busy."})
    clients = scripted(busy, busy, busy, busy, busy)
    clients.random = lambda: 0.999999

    with pytest.raises(pipsocket.EngineBusy):
        clients.sync(max_retries=4).account(ACCOUNT).positions.list()

    assert [round(s, 3) for s in clients.clock.sleeps] == [0.5, 1.0, 2.0, 4.0]


def test_a_connection_that_never_opens_raises_api_connection_error() -> None:
    clients = scripted(connect_failed(), connect_failed(), connect_failed())

    with pytest.raises(APIConnectionError) as caught:
        clients.sync().account(ACCOUNT).symbols.list()

    assert type(caught.value) is APIConnectionError
    assert isinstance(caught.value.__cause__, httpx.ConnectError)


def test_an_unknown_4xx_on_a_read_is_a_plain_api_error() -> None:
    clients = scripted(respond(418, {"error": "brand_new_code", "message": "New."}))

    with pytest.raises(APIError) as caught:
        clients.sync().account(ACCOUNT).symbols.list()

    assert type(caught.value) is APIError
    assert (caught.value.status, caught.value.code) == (418, "brand_new_code")
    assert len(clients.transport.requests) == 1


def test_a_given_http_client_is_left_open() -> None:
    clients = scripted(respond(200, POSITIONS))
    http = httpx.Client(transport=FakeTransport(clients.transport.handle))

    with Pipsocket(API_KEY, http_client=http):
        pass

    assert not http.is_closed


def test_an_owned_http_client_is_closed() -> None:
    ps = Pipsocket(API_KEY)
    ps.close()
    with pytest.raises(RuntimeError):
        ps.account(ACCOUNT).positions.list()


def test_async_cancellation_passes_through_and_sends_nothing_more() -> None:
    started = asyncio.Event()
    sent: list[httpx.Request] = []

    async def hang(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        started.set()
        await asyncio.sleep(3600)
        raise AssertionError("not reached")

    async def run() -> None:
        http = httpx.AsyncClient(transport=FakeTransport(hang))
        sleeps: list[float] = []

        async def sleep(seconds: float) -> None:
            sleeps.append(seconds)

        ps = AsyncPipsocket(API_KEY, http_client=http, _sleep=sleep)
        task = asyncio.create_task(ps.account(ACCOUNT).positions.list())
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.sleep(0)
        assert sleeps == []
        assert len(sent) == 1

    asyncio.run(run())


def test_a_wait_that_runs_past_the_deadline_starts_no_new_attempt() -> None:
    clients = scripted(respond(503, {"error": "engine_unavailable", "message": "Away."}))
    ps = Pipsocket(
        API_KEY,
        http_client=httpx.Client(transport=FakeTransport(clients.transport.handle)),
        _clock=clients.clock,
        _sleep=lambda seconds: clients.clock.advance(61),
    )

    with pytest.raises(Unavailable):
        ps.account(ACCOUNT).positions.list()

    assert len(clients.transport.requests) == 1


def test_redirects_are_not_followed() -> None:
    moved = httpx.Response(307, headers={"Location": "https://elsewhere.example/v1"})
    clients = scripted(moved, moved, moved)
    http = httpx.Client(transport=FakeTransport(clients.transport.handle), follow_redirects=True)
    ps = Pipsocket(API_KEY, http_client=http, _clock=clients.clock, _sleep=clients.clock.sleep)

    with pytest.raises(APIError):
        ps.account(ACCOUNT).positions.list()

    assert {request.url.host for request in clients.transport.requests} == {"api.pipsocket.com"}


@pytest.mark.parametrize("key", ["psk_abc\n", "psk_abc def", " psk_abc", "psk_é"])
def test_a_key_a_header_cant_carry_is_refused_without_echoing_it(key: str) -> None:
    with pytest.raises(PipsocketError) as caught:
        Pipsocket(key)

    assert key.strip() not in str(caught.value)
