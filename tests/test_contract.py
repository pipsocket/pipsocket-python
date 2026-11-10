"""The SDK against the OpenAPI document: nothing in the document goes unhandled.

- The generated code matches the committed snapshot.
- Every operation has an SDK method, which sends that operation's method and path, and handles
  every response example the document gives for it.
- Every error code has an exception and a retry policy for each kind of call.
- Every request state is either final or outstanding.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pytest

from pipsocket import APIError, TradeRequest
from pipsocket._core import SAFE_POLICY, TRADE_POLICY
from pipsocket._generated.operations import OPERATIONS
from pipsocket._generated.tables import ERROR_STATUS, REQUEST_STATES
from pipsocket.errors import ERROR_CLASSES
from pipsocket.models import FINAL_STATES, OUTSTANDING_STATES

from .fakes import ACCOUNT, BASE_URL, scripted

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "tests" / "fixtures" / "spec"

ORDER = 881402377
POSITION = 881402311
REQUEST = "req_5f0c2a9e41d7b3a8c6e2f190"

# Each operation in the document: the SDK call that makes it, and the path it must send.
# A new operation in the document fails test_every_operation_has_a_method until it's added.
Call = Callable[[Any], Any]
CALLS: dict[str, tuple[Call, str]] = {
    "account": (lambda a: a.get(), ""),
    "positions": (lambda a: a.positions.list(), "/positions"),
    "pending_orders": (lambda a: a.orders.list(), "/orders"),
    "request": (lambda a: a.requests.get(REQUEST), f"/requests/{REQUEST}"),
    "symbols": (lambda a: a.symbols.list(), "/symbols"),
    "symbol": (lambda a: a.symbols.get("EURUSD"), "/symbols/EURUSD"),
    "tick": (lambda a: a.symbols.tick("EURUSD"), "/symbols/EURUSD/tick"),
    "history_deals": (
        lambda a: a.history.deals(1794009600, 1794095999),
        "/history/deals?from=1794009600&to=1794095999",
    ),
    "history_orders": (
        lambda a: a.history.orders(1794009600, 1794095999),
        "/history/orders?from=1794009600&to=1794095999",
    ),
    "kill_switch": (lambda a: a.kill_switch.get(), "/kill-switch"),
}

# The trade calls, check and the kill switch's writes arrive in the next milestone.
NOT_YET = {
    "place_order",
    "check_order",
    "modify_order",
    "cancel_order",
    "set_position_levels",
    "close_position",
    "close_by",
    "engage_kill_switch",
    "lift_kill_switch",
}

# The exception each refusal raises on a call that is safe to repeat, as the SDK's contract
# names them (independently of the SDK's own table).
SAFE_ERRORS = {
    "missing_api_key": "AuthenticationError",
    "invalid_api_key": "AuthenticationError",
    "no_such_account": "NotFound",
    "no_such_order": "NotFound",
    "no_such_position": "NotFound",
    "no_such_request": "NotFound",
    "no_such_symbol": "NotFound",
    "invalid_request": "InvalidRequest",
    "magic_reserved": "InvalidRequest",
    "range_too_large": "RangeTooLarge",
    "symbol_busy": "SymbolBusy",
    "kill_switch_engaged": "KillSwitchEngaged",
    "billing_hold": "BillingHold",
    "account_not_connected": "AccountUnavailable",
    "bridge_outdated": "AccountUnavailable",
    "account_restarting": "AccountUnavailable",
    "engine_busy": "EngineBusy",
    "engine_unavailable": "Unavailable",
    "engine_timeout": "Unavailable",
    "terminal_timeout": "TerminalTimeout",
    "terminal_error": "TerminalError",
    "bad_command": "TerminalError",
    "symbol_too_large": "TerminalError",
    "no_price": "NoPrice",
}


def test_generated_code_is_fresh() -> None:
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "generate.py"), "--check"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_every_operation_has_a_method() -> None:
    assert not set(CALLS) & NOT_YET
    assert set(CALLS) | NOT_YET == {op.id for op in OPERATIONS}


def examples() -> list[Any]:
    found: list[Any] = []
    for operation in OPERATIONS:
        if operation.id not in CALLS:
            continue
        by_status = json.loads((FIXTURES / f"{operation.id}.json").read_text())
        for status, named in by_status.items():
            for name, body in named.items():
                for flavour in ("sync", "async"):
                    found.append(
                        pytest.param(
                            operation, int(status), body, flavour,
                            id=f"{operation.id}-{status}-{name}-{flavour}",
                        )
                    )  # fmt: skip
    return found


@pytest.mark.parametrize(("operation", "status", "body", "flavour"), examples())
def test_every_documented_answer(operation: Any, status: int, body: Any, flavour: str) -> None:
    call, path = CALLS[operation.id]
    clients = scripted(httpx.Response(status, json=body))
    outcome: Any = None
    raised: Exception | None = None
    try:
        if flavour == "sync":
            outcome = call(clients.sync(max_retries=0).account(ACCOUNT))
        else:
            account = clients.asyncio(max_retries=0).account(ACCOUNT)
            outcome = asyncio.run(awaited(call(account)))
    except APIError as error:
        raised = error

    (request,) = clients.transport.requests
    assert request.method == operation.method
    assert str(request.url) == f"{BASE_URL}/v1/accounts/{ACCOUNT}{path}"

    if 200 <= status < 300:
        assert raised is None
        if operation.id == "request":
            assert isinstance(outcome, TradeRequest)
            assert outcome.raw == body
            assert outcome.http_status == status
        else:
            assert outcome == body
    else:
        assert raised is not None
        assert type(raised).__name__ == SAFE_ERRORS[body["error"]]
        assert raised.status == status
        assert raised.code == body["error"]
        assert raised.body == body


async def awaited(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


def test_every_error_code_has_an_exception_and_a_policy() -> None:
    codes = set(ERROR_STATUS)
    assert codes == set(ERROR_CLASSES), "errors.ERROR_CLASSES"
    assert codes == set(TRADE_POLICY), "_core.TRADE_POLICY"
    assert codes == set(SAFE_POLICY), "_core.SAFE_POLICY"


def test_every_request_state_is_final_or_outstanding() -> None:
    states = set(REQUEST_STATES)
    assert states == FINAL_STATES | OUTSTANDING_STATES
    assert not FINAL_STATES & OUTSTANDING_STATES
