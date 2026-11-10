"""TradeRequest: what each state means for a caller, from the document's own examples."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from pipsocket import TradeNotAccepted, TradeRequest

LOOKUPS = json.loads(
    (Path(__file__).resolve().parent / "fixtures" / "spec" / "request.json").read_text()
)["200"]
CASES = json.loads(
    (Path(__file__).resolve().parent.parent / "conformance" / "call-cases.json").read_text()
)["bodies"]


# body, accepted, final, outstanding, retcode, raises
STATES = [
    ("request_completed", True, True, False, 10009, False),
    ("request_placed", True, True, False, 10008, False),
    ("request_partial", True, True, False, 10010, False),
    ("request_rejected", False, True, False, 10016, True),
    ("request_pending", False, False, True, None, False),
    ("request_unknown", False, False, True, None, False),
    ("request_not_sent", False, True, False, None, True),
    ("request_not_executed_evidence", False, True, False, None, True),
    ("request_not_executed_operator", False, True, False, None, True),
    ("request_executed", False, True, False, None, False),
    ("request_future_state", False, False, False, None, False),
]


@pytest.mark.parametrize(("name", "accepted", "final", "outstanding", "retcode", "raises"), STATES)
def test_states(
    name: str, accepted: bool, final: bool, outstanding: bool, retcode: int | None, raises: bool
) -> None:
    body: Any = CASES[name]
    request = TradeRequest(body, http_status=200)

    assert (request.accepted, request.final, request.outstanding) == (accepted, final, outstanding)
    assert request.retcode == retcode
    assert request.raw is body
    assert request.id == body["request"]
    assert request.state == body["state"]
    if raises:
        with pytest.raises(TradeNotAccepted) as caught:
            request.raise_for_state()
        assert caught.value.request is request
    else:
        assert request.raise_for_state() is request


def test_retcode_comes_only_from_the_trade_result() -> None:
    rejected = TradeRequest(LOOKUPS["trade_rejected"])
    not_sent = TradeRequest(LOOKUPS["not_sent"])

    assert rejected.retcode_name == rejected.result["retcode_name"]
    assert not_sent.result["error"]
    assert (not_sent.retcode, not_sent.retcode_name) == (None, None)


def test_every_documented_lookup_reads() -> None:
    for body in LOOKUPS.values():
        request = TradeRequest(body)
        assert request.state == body["state"]
        assert request.idempotency_key is None


def test_from_event() -> None:
    event = {"type": "request", **LOOKUPS["done"]}

    request = TradeRequest.from_event(event)

    assert request.state == "completed"
    assert (request.idempotency_key, request.http_status) == (None, None)


@pytest.mark.parametrize("message", [{"type": "deal"}, {"type": "request", "request": "req_1"}])
def test_from_event_refuses_other_messages(message: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        TradeRequest.from_event(message)
