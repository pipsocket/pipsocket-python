"""The core's rules for a trade call, driven directly until the trade methods exist.

These pin what the shells can't show with a read: once maybe sent, always maybe sent, and the
key on every error a trade call raises.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from pipsocket import AuthenticationError, OutcomeUnknown
from pipsocket._core import (
    SEND,
    Answer,
    Attempt,
    BodyLost,
    NoAnswer,
    NotSent,
    Settings,
    Wait,
    drive,
    trade,
)

from .fakes import ACCOUNT, FakeClock

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
        run([lost(), NotSent(closed)])

    assert caught.value.idempotency_key == KEY
    assert caught.value.cause.__cause__ is closed


def test_a_client_closed_before_anything_was_sent_raises_as_it_is() -> None:
    closed = RuntimeError("Cannot send a request, as the client has been closed.")

    with pytest.raises(RuntimeError) as caught:
        run([NotSent(closed)])

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
