"""Runs every case in conformance/ through the real client, sync and async.

Each case scripts the answers to one call. The client gets a fake transport, clock and sleep, so
nothing waits and nothing leaves the process. See conformance/README.md.
"""

from __future__ import annotations

import asyncio
import json
import random
import uuid
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, cast

import httpx
import pytest

from pipsocket import AsyncPipsocket, OutcomeUnknown, TradeRequest
from pipsocket.aio import AsyncAccount
from pipsocket.client import Account

from .fakes import ACCOUNT, BASE_URL, Clients, CutShort, FakeClock, Transport

FOLDER = Path(__file__).resolve().parent.parent / "conformance"
CASE_FILES = ("call-cases.json", "extra-cases.json")

Json = Any


def load() -> tuple[Json, list[Json]]:
    main = json.loads((FOLDER / "call-cases.json").read_text())
    cases: list[Json] = list(main["cases"])
    extra = FOLDER / "extra-cases.json"
    if extra.exists():
        more = json.loads(extra.read_text())
        main["calls"] = {**main["calls"], **more.get("calls", {})}
        main["bodies"] = {**main["bodies"], **more.get("bodies", {})}
        cases += more["cases"]
    return main, cases


SUITE, CASES = load()

# The calls this milestone has. The others arrive with the trade calls.
NOT_YET = {"place_order", "cancel_order", "check_order", "engage_kill_switch"}


def resolve(value: Json) -> Json:
    if isinstance(value, dict) and set(cast(dict[str, Json], value)) == {"use"}:
        return SUITE["bodies"][value["use"]]
    return value


def reply_for(attempt: Json) -> httpx.Response | Exception:
    if attempt.get("fail") == "connect":
        return httpx.ConnectError("connection refused")
    if attempt.get("fail") == "after_write":
        # A reset rather than a timeout: a timeout is the subclass APITimeoutError, and the
        # cases name the exact class.
        return httpx.ReadError("connection reset after the request was written")
    answer = attempt["respond"]
    status: int = answer["status"]
    headers: dict[str, str] = answer.get("headers", {})
    if answer.get("body_error"):
        return httpx.Response(status, headers=headers, stream=CutShort())
    if "text" in answer:
        return httpx.Response(status, headers=headers, content=answer["text"].encode())
    content = json.dumps(resolve(answer["body"])).encode()
    return httpx.Response(
        status, headers={"Content-Type": "application/json", **headers}, content=content
    )


def sync_call(account: Account, case: Json) -> Any:
    call = case["call"]
    if call == "list_positions":
        return account.positions.list()
    if call == "list_symbols":
        return account.symbols.list()
    if call == "get_tick":
        return account.symbols.tick("EURUSD")
    if call == "history_deals":
        return account.history.deals(1794009600, 1794095999)
    if call == "get_request":
        return account.requests.get("req_5f0c2a9e41d7b3a8c6e2f190")
    raise NotImplementedError(call)


async def async_call(account: AsyncAccount, case: Json) -> Any:
    call = case["call"]
    if call == "list_positions":
        return await account.positions.list()
    if call == "list_symbols":
        return await account.symbols.list()
    if call == "get_tick":
        return await account.symbols.tick("EURUSD")
    if call == "history_deals":
        return await account.history.deals(1794009600, 1794095999)
    if call == "get_request":
        return await account.requests.get("req_5f0c2a9e41d7b3a8c6e2f190")
    raise NotImplementedError(call)


def params() -> list[Any]:
    found: list[Any] = []
    for case in CASES:
        marks = []
        if case["call"] in NOT_YET:
            marks = [
                pytest.mark.xfail(
                    reason=f"{case['call']} arrives with the trade calls",
                    raises=NotImplementedError,
                    strict=True,
                )
            ]
        for flavour in ("sync", "async"):
            found.append(pytest.param(case, flavour, id=f"{case['id']}-{flavour}", marks=marks))
    return found


@pytest.mark.parametrize(("case", "flavour"), params())
def test_case(case: Json, flavour: str) -> None:
    clock = FakeClock()
    transport = Transport(
        [reply_for(a) for a in case["attempts"]],
        clock,
        [a.get("elapsed", 0.0) for a in case["attempts"]],
    )
    clients = Clients(transport, clock, random.Random(case["id"]).random)
    options = {**SUITE["defaults"], **case.get("options", {})}

    outcome: Any = None
    raised: BaseException | None = None
    try:
        if flavour == "sync":
            with clients.sync(**options) as ps:
                outcome = sync_call(ps.account(ACCOUNT), case)
        else:
            outcome = asyncio.run(run_async(clients, options, case, async_call))
    except NotImplementedError:
        raise
    except Exception as error:
        raised = error

    check_attempts(case, transport)
    check_sleeps(case, clock)
    expect = case["expect"]
    if "return" in expect:
        assert raised is None, f"expected a return, got {raised!r}"
        check_return(case, expect["return"], outcome, transport)
    else:
        assert raised is not None, f"expected {expect['raise']['error']}, got {outcome!r}"
        check_raise(case, expect["raise"], raised, transport)


async def run_async(
    clients: Clients,
    options: dict[str, Any],
    case: Json,
    call: Callable[[AsyncAccount, Json], Awaitable[Any]],
) -> Any:
    ps: AsyncPipsocket = clients.asyncio(**options)
    async with ps:
        return await call(ps.account(ACCOUNT), case)


def check_attempts(case: Json, transport: Transport) -> None:
    expected = case["expect"]["attempts"]
    requests = transport.requests
    assert len(requests) == expected, f"{len(requests)} attempts, expected {expected}"
    if not requests:
        return
    spec = SUITE["calls"][case["call"]]
    first = requests[0]
    assert first.method == spec["method"]
    assert str(first.url) == f"{BASE_URL}/v1/accounts/{ACCOUNT}{spec['path']}"
    if "body" in spec:
        assert json.loads(first.content) == spec["body"]
    else:
        assert first.content == b""
    for request in requests[1:]:
        assert request.method == first.method
        assert request.url == first.url
        assert request.content == first.content
    keys = {request.headers.get("Idempotency-Key") for request in requests}
    if spec["keyed"]:
        assert len(keys) == 1, f"the key changed between attempts: {keys}"
        key = keys.pop()
        assert key is not None
        if "idempotency_key" in case:
            assert key == case["idempotency_key"]
        else:
            uuid.UUID(key)
    else:
        assert keys == {None}


def check_sleeps(case: Json, clock: FakeClock) -> None:
    expected = case["expect"].get("sleeps")
    if expected is None:
        return
    assert len(clock.sleeps) == len(expected), f"slept {clock.sleeps}"
    for slept, bounds in zip(clock.sleeps, expected, strict=True):
        assert slept >= bounds.get("min", 0.0)
        assert slept <= bounds.get("max", float("inf"))


def sent_key(transport: Transport) -> str | None:
    return transport.requests[0].headers.get("Idempotency-Key") if transport.requests else None


def check_return(case: Json, expected: Json, outcome: Any, transport: Transport) -> None:
    if "trade_request" in expected:
        fields = expected["trade_request"]
        assert isinstance(outcome, TradeRequest)
        for name, value in fields.items():
            if name == "kind":
                continue  # the TypeScript SDK's discriminator; Python checks `state`
            assert getattr(outcome, name) == value, name
        keyed = SUITE["calls"][case["call"]]["keyed"]
        assert outcome.idempotency_key == (sent_key(transport) if keyed else None)
    elif "json" in expected:
        assert outcome == resolve(expected["json"])
    else:
        assert isinstance(outcome, dict)
        assert set(expected["json_has"]) <= set(cast(dict[str, Any], outcome))


def check_raise(case: Json, expected: Json, raised: BaseException, transport: Transport) -> None:
    name = expected["error"]
    if name == "ArgumentError":
        assert type(raised) is ValueError, repr(raised)
        return
    assert type(raised).__name__ == name, repr(raised)
    error: Any = raised
    if "code" in expected:
        assert error.code == expected["code"]
    if "status" in expected:
        assert error.status == expected["status"]
    if "retry_after" in expected:
        assert error.retry_after == expected["retry_after"]
    if "blocking" in expected:
        assert [b["request"] for b in error.blocking] == expected["blocking"]
    if "kill_switch" in expected:
        assert expected["kill_switch"].items() <= error.kill_switch.items()
    if isinstance(raised, OutcomeUnknown):
        assert raised.idempotency_key == sent_key(transport)
        if "cause" in expected:
            assert type(raised.cause).__name__ == expected["cause"], repr(raised.cause)
        if "cause_code" in expected:
            assert getattr(raised.cause, "code", None) == expected["cause_code"]
    if "idempotency_key" in expected:
        assert error.idempotency_key == expected["idempotency_key"]
