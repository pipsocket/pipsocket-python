"""Everything that decides what a call returns, raises or retries. No I/O.

A call is a `Spec`. `drive()` is a generator that the sync and async clients run: it yields
`SEND` (make one attempt and send back what happened) or a `Wait` (sleep that long), and ends by
returning the call's value or raising its error. The clients only do the I/O.

The rules, in short (the README has them for users):

- One idempotency key per trade call, sent unchanged on every attempt. A retry changes nothing:
  same method, path, body and key.
- A recorded request is returned, whatever its state.
- Once any attempt of a trade call may have reached the account's host, the call ends either
  with a recorded request or with OutcomeUnknown carrying the key. Never with an error that says
  nothing was sent.
- No new attempt starts after the retry deadline, counted from the start of the first attempt.
"""

from __future__ import annotations

import contextlib
import json
import re
import uuid
from collections.abc import Callable, Generator, Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Literal, cast
from urllib.parse import quote

from .errors import (
    ERROR_CLASSES,
    AccountUnavailable,
    APIConnectionError,
    APIError,
    APITimeoutError,
    InvalidRequest,
    OutcomeUnknown,
    PipsocketError,
    SymbolBusy,
)
from .models import TradeRequest, is_recorded_request
from .types import Blocking, KillSwitchView, RequestView

ACCOUNT_PATH = "/v1/accounts/{account}"

# Waits between attempts: 0.5 s doubling, capped, each times a random factor from 0.5 to 1.
FIRST_WAIT = 0.5
LONGEST_WAIT = 8.0


class Kind(Enum):
    """What sort of call a spec is, which decides how its answers are read and retried."""

    # A read, check, or the kill switch: safe to repeat, answers are JSON returned as they are.
    SAFE = "safe"
    # requests.get(): safe to repeat, the answer is a recorded request.
    LOOKUP = "lookup"
    # The six trade calls: keyed, and the answer is a recorded request.
    TRADE = "trade"


@dataclass(frozen=True)
class Spec:
    operation: str
    method: str
    path: str
    kind: Kind
    query: tuple[tuple[str, str], ...] = ()
    body: bytes | None = None
    idempotency_key: str | None = None


# What one attempt came to. The client builds one of these and sends it into drive().


@dataclass(frozen=True)
class Answer:
    """An HTTP answer, read to the end."""

    status: int
    headers: Mapping[str, str]
    content: bytes


@dataclass(frozen=True)
class BodyLost:
    """The status and headers arrived, then reading the body failed. `content` is what arrived."""

    status: int
    error: Exception
    content: bytes


@dataclass(frozen=True)
class NoAnswer:
    """No answer. `written` is False only when the connection never opened, so nothing was sent."""

    error: Exception
    written: bool
    timed_out: bool


@dataclass(frozen=True)
class Failed:
    """The attempt failed for a reason that isn't the network or the API: the HTTP client was
    closed, or another library raised. `maybe_sent` says whether sending had begun. It isn't
    retried, and its error is raised as it is unless the call may have reached the host."""

    error: Exception
    maybe_sent: bool


Attempt = Answer | BodyLost | NoAnswer | Failed


class _Send:
    def __repr__(self) -> str:
        return "SEND"


SEND = _Send()


@dataclass(frozen=True)
class Wait:
    seconds: float


Step = _Send | Wait


@dataclass(frozen=True)
class Settings:
    max_retries: int
    retry_deadline: float
    clock: Callable[[], float]
    random: Callable[[], float]


# Specs for each call. Paths are under /v1/accounts/{account}.


def safe(operation: str, method: str, account: str, path: str, **extra: Any) -> Spec:
    return Spec(operation, method, account_path(account, path), Kind.SAFE, **extra)


def lookup(account: str, request_id: str) -> Spec:
    if not request_id:
        raise ValueError("A request id is required.")
    path = account_path(account, f"/requests/{segment(request_id)}")
    return Spec("request", "GET", path, Kind.LOOKUP)


def trade(
    operation: str,
    method: str,
    account: str,
    path: str,
    body: Mapping[str, Any] | None,
    idempotency_key: str | None,
) -> Spec:
    key = new_key() if idempotency_key is None else check_key(idempotency_key)
    return Spec(
        operation,
        method,
        account_path(account, path),
        Kind.TRADE,
        body=None if body is None else json_body(body),
        idempotency_key=key,
    )


def account_path(account: str, path: str) -> str:
    if not account:
        raise ValueError("An account id is required.")
    base = ACCOUNT_PATH.format(account=segment(account))
    return base if path == "/" else base + path


def segment(value: str) -> str:
    """One path segment, with anything that isn't safe in a path escaped (a "/" included)."""
    return quote(value, safe="")


def ticket(value: int) -> str:
    """A ticket for a path: a whole number, 0 or more."""
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:  # pyright: ignore[reportUnnecessaryIsInstance]
        raise ValueError(f"A ticket is a whole number, 0 or more, not {value!r}.")
    return str(value)


def symbol_path(name: str) -> str:
    if not name:
        raise ValueError("A symbol name is required.")
    return f"/symbols/{segment(name)}"


def history_range(from_: int, to: int) -> tuple[tuple[str, str], ...]:
    """A history range's query: broker server time in whole seconds, both ends included."""
    for name, value in (("from_", from_), ("to", to)):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:  # pyright: ignore[reportUnnecessaryIsInstance]
            raise ValueError(f"{name} is broker server time in whole seconds, not {value!r}.")
    return (("from", str(from_)), ("to", str(to)))


def json_body(fields: Mapping[str, Any]) -> bytes:
    """The body sent on every attempt: fields left as None are left out."""
    present = {name: value for name, value in fields.items() if value is not None}
    return json.dumps(present, separators=(",", ":"), allow_nan=False).encode()


def new_key() -> str:
    """A new idempotency key: one per trade. Save it before sending to survive a crash."""
    return str(uuid.uuid4())


_RESERVED_PREFIX = "hk:"
_KEY_LENGTH = 128
_VISIBLE = re.compile(r"[\x21-\x7e](?:[\x20-\x7e]*[\x21-\x7e])?")


def check_key(key: str) -> str:
    """An idempotency key: 1 to 128 visible ASCII characters, not starting with "hk:".

    Spaces are allowed inside a key but not at either end (HTTP would drop them).
    """
    if not isinstance(key, str):  # pyright: ignore[reportUnnecessaryIsInstance]
        raise ValueError("An idempotency key is a string.")
    if key.startswith(_RESERVED_PREFIX):
        raise ValueError(
            f"An idempotency key can't start with {_RESERVED_PREFIX!r}: Pipsocket uses it."
        )
    if not 1 <= len(key) <= _KEY_LENGTH or not _VISIBLE.fullmatch(key):
        raise ValueError(
            f"An idempotency key is 1 to {_KEY_LENGTH} visible ASCII characters, "
            "with no space at either end."
        )
    return key


# How each error code is retried.

Retry = Literal["no", "yes", "once", "if_retry_after"]


@dataclass(frozen=True)
class Policy:
    retry: Retry
    # On a trade call: this answer leaves open that the trade reached the account's host.
    maybe_sent: bool = False


_STOP = Policy("no")

# On a trade call. "maybe sent" is permanent for the call once any attempt is marked.
TRADE_POLICY: dict[str, Policy] = {
    "missing_api_key": _STOP,
    "invalid_api_key": _STOP,
    "no_such_account": _STOP,
    # The ticket was read fresh and is gone; this attempt recorded nothing.
    "no_such_order": _STOP,
    "no_such_position": _STOP,
    "no_such_request": _STOP,
    "no_such_symbol": _STOP,
    "invalid_request": _STOP,
    "magic_reserved": _STOP,
    "range_too_large": _STOP,
    # Retried after Retry-After: nothing was sent. Without it a blocker is unknown: stop.
    "symbol_busy": Policy("if_retry_after"),
    "kill_switch_engaged": _STOP,
    "billing_hold": _STOP,
    "account_not_connected": _STOP,
    "bridge_outdated": _STOP,
    "account_restarting": _STOP,
    # The host refused it before running it.
    "engine_busy": Policy("yes"),
    # The call may have reached the host. The same key returns the request if it was
    # recorded, and sends it if not.
    "engine_unavailable": Policy("yes", maybe_sent=True),
    "engine_timeout": Policy("yes", maybe_sent=True),
    # From the fresh read that cancel, modify, set levels, close and close by make before
    # recording anything (placing an order has no such read: see _policy()).
    "terminal_timeout": Policy("yes"),
    # These arrive inside a recorded request on a trade call, never as a bare refusal. If one
    # does, it is treated as an unknown 5xx: maybe sent, retried with the same key.
    "terminal_error": Policy("yes", maybe_sent=True),
    "bad_command": Policy("yes", maybe_sent=True),
    "symbol_too_large": Policy("yes", maybe_sent=True),
    "no_price": Policy("yes", maybe_sent=True),
    "not_sent": Policy("yes", maybe_sent=True),
    "trade_rejected": _STOP,
    "not_executed": _STOP,
}

# On a read, check, lookup or the kill switch: these send nothing to the broker.
SAFE_POLICY: dict[str, Policy] = {
    "missing_api_key": _STOP,
    "invalid_api_key": _STOP,
    "no_such_account": _STOP,
    "no_such_order": _STOP,
    "no_such_position": _STOP,
    "no_such_request": _STOP,
    "no_such_symbol": _STOP,
    "invalid_request": _STOP,
    "magic_reserved": _STOP,
    "range_too_large": _STOP,
    "symbol_busy": _STOP,
    "kill_switch_engaged": _STOP,
    "billing_hold": _STOP,
    "account_not_connected": _STOP,
    "bridge_outdated": _STOP,
    "account_restarting": _STOP,
    "engine_busy": Policy("yes"),
    "engine_unavailable": Policy("yes"),
    "engine_timeout": Policy("yes"),
    "terminal_timeout": Policy("yes"),
    "terminal_error": Policy("once"),
    "bad_command": _STOP,
    "symbol_too_large": _STOP,
    # The terminal already waited 2 s for a price.
    "no_price": _STOP,
    "not_sent": _STOP,
    "trade_rejected": _STOP,
    "not_executed": _STOP,
}

# The two codes that mean a trade may have reached the host; on a trade call their error is a
# plain APIError (the cause of OutcomeUnknown), not Unavailable, which means "safe to repeat".
_MAYBE_SENT_CODES = frozenset({"engine_unavailable", "engine_timeout"})


def _policy(spec: Spec, code: str, status: int) -> Policy:
    if spec.kind is Kind.TRADE:
        if code == "terminal_timeout" and spec.operation == "place_order":
            # Not documented for placing an order, so nothing says it recorded nothing.
            return Policy("yes", maybe_sent=True)
        known = TRADE_POLICY.get(code)
        if known is not None:
            return known
        return Policy("yes", maybe_sent=True) if status >= 500 else _STOP
    known = SAFE_POLICY.get(code)
    if known is not None:
        return known
    return Policy("yes") if status >= 500 else _STOP


# Reading one attempt.


@dataclass(frozen=True)
class Verdict:
    """What one attempt came to: a value, or an error and whether to try again."""

    value: Any = None
    error: PipsocketError | None = None
    retry: Retry = "no"
    maybe_sent: bool = False
    retry_after: int | None = None
    status: int | None = None
    raw: bytes | None = None
    once_key: str | None = field(default=None)
    # An exception that isn't the SDK's own, raised as it is when nothing may have been sent.
    original: Exception | None = None


def read_attempt(spec: Spec, attempt: Attempt) -> Verdict:
    if isinstance(attempt, Failed):
        error = PipsocketError(f"The attempt failed: {describe(attempt.error)}")
        error.__cause__ = attempt.error
        return Verdict(error=error, maybe_sent=attempt.maybe_sent, original=attempt.error)
    if isinstance(attempt, NoAnswer):
        kind = APITimeoutError if attempt.timed_out else APIConnectionError
        error = kind(f"No answer from the API: {describe(attempt.error)}")
        error.__cause__ = attempt.error
        return Verdict(error=error, retry="yes", maybe_sent=attempt.written)
    if isinstance(attempt, BodyLost):
        error = APIConnectionError(
            f"The answer ({attempt.status}) was cut short: {describe(attempt.error)}"
        )
        error.__cause__ = attempt.error
        return Verdict(
            error=error,
            retry="yes",
            maybe_sent=True,
            status=attempt.status,
            raw=attempt.content,
        )
    return read_answer(spec, attempt)


def read_answer(spec: Spec, answer: Answer) -> Verdict:
    status, raw = answer.status, answer.content
    parsed, body = parse_json(raw)

    recorded = spec.kind is not Kind.SAFE and parsed and is_recorded_request(body)
    if recorded and (spec.kind is Kind.TRADE or 200 <= status < 300):
        request = TradeRequest(
            cast(RequestView, body), http_status=status, idempotency_key=spec.idempotency_key
        )
        return Verdict(value=request)

    if 200 <= status < 300:
        if spec.kind is Kind.SAFE and parsed:
            return Verdict(value=body)
        return unreadable(status, raw, body, "The answer isn't what the call returns")

    if status == 400 and not raw.strip():
        error = InvalidRequest(
            "Refused with no body: a ticket in the path isn't a whole number.",
            status=status,
            code=None,
            body=None,
            raw=raw,
        )
        return Verdict(error=error, status=status, raw=raw)

    if status == 403 and raw.strip() == b"error code: 1010":
        error = APIError(
            "Cloudflare refused this client before the API saw the call (error code: 1010).",
            status=status,
            code=None,
            body=None,
            raw=raw,
        )
        return Verdict(error=error, status=status, raw=raw)

    fields = cast(dict[str, Any], body) if parsed and isinstance(body, dict) else {}
    code = fields.get("error")
    if not isinstance(code, str):
        return unreadable(status, raw, body if parsed else None, "The refusal has no error code")

    policy = _policy(spec, code, status)
    retry_after = parse_retry_after(answer.headers)
    error = refusal(spec, status, code, fields, raw, retry_after)
    retry = policy.retry
    if retry == "if_retry_after":
        retry = "yes" if retry_after is not None else "no"
    return Verdict(
        error=error,
        retry=retry,
        maybe_sent=policy.maybe_sent,
        retry_after=retry_after,
        status=status,
        raw=raw,
        once_key=code if retry == "once" else None,
    )


def unreadable(status: int, raw: bytes, body: Any, what: str) -> Verdict:
    """An answer that isn't the API's JSON, or isn't the shape its status needs.

    It may have come from past the API (a gateway page, a body cut short), so a trade call
    counts it as maybe sent. Retrying with the same key replays the recorded answer.
    """
    error = APIError(f"{what} ({status}).", status=status, code=None, body=body, raw=raw)
    return Verdict(error=error, retry="yes", maybe_sent=True, status=status, raw=raw)


def refusal(
    spec: Spec,
    status: int,
    code: str,
    body: dict[str, Any],
    raw: bytes,
    retry_after: int | None,
) -> APIError:
    message = body.get("message")
    text = message if isinstance(message, str) else code
    kind = ERROR_CLASSES.get(code, APIError)
    if spec.kind is Kind.TRADE and code in _MAYBE_SENT_CODES:
        kind = APIError
    if kind is SymbolBusy:
        blocking = body.get("blocking")
        return SymbolBusy(
            text,
            status=status,
            code=code,
            body=body,
            raw=raw,
            blocking=cast(list[Blocking], blocking) if isinstance(blocking, list) else [],
            retry_after=retry_after,
        )
    if kind is AccountUnavailable:
        switch = body.get("kill_switch")
        return AccountUnavailable(
            text,
            status=status,
            code=code,
            body=body,
            raw=raw,
            kill_switch=cast(KillSwitchView, switch) if isinstance(switch, dict) else None,
        )
    return kind(text, status=status, code=code, body=body, raw=raw)


def parse_json(raw: bytes) -> tuple[bool, Any]:
    try:
        return True, json.loads(raw)
    except ValueError:
        return False, None


def parse_retry_after(headers: Mapping[str, str]) -> int | None:
    """Retry-After in whole seconds. The HTTP-date form isn't used by the API, so it is ignored."""
    value = headers.get("retry-after")
    if value is None or not value.strip().isdigit():
        return None
    return int(value.strip())


def describe(error: BaseException) -> str:
    return str(error) or type(error).__name__


# Running a call.


def drive(spec: Spec, settings: Settings) -> Generator[Step, Attempt | None, Any]:
    """Run one call: yields SEND (send back an Attempt) or a Wait (send back None)."""
    start = settings.clock()
    deadline = start + settings.retry_deadline
    maybe_sent = False
    retries = 0
    retried_once: set[str] = set()

    while True:
        attempt = yield SEND
        assert attempt is not None
        verdict = read_attempt(spec, attempt)
        if verdict.error is None:
            return verdict.value

        maybe_sent = maybe_sent or verdict.maybe_sent
        wait = next_wait(verdict, retries, settings) if retries < settings.max_retries else None
        if verdict.retry == "once":
            assert verdict.once_key is not None
            if verdict.once_key in retried_once:
                wait = None
            retried_once.add(verdict.once_key)
        if wait is not None and settings.clock() + wait > deadline:
            wait = None
        if wait is None:
            raise final_error(spec, verdict, maybe_sent)

        retries += 1
        yield Wait(wait)
        # The wait may have run long: no new attempt starts after the deadline.
        if settings.clock() > deadline:
            raise final_error(spec, verdict, maybe_sent)


def final_error(spec: Spec, verdict: Verdict, maybe_sent: bool) -> Exception:
    """How a call ends without a value. On a trade call that may have reached the host, that is
    OutcomeUnknown with the key; otherwise the last attempt's error, carrying the key too."""
    assert verdict.error is not None
    if spec.kind is Kind.TRADE and maybe_sent:
        assert spec.idempotency_key is not None
        unknown = OutcomeUnknown(
            idempotency_key=spec.idempotency_key,
            cause=verdict.error,
            status=verdict.status,
            raw=verdict.raw,
        )
        unknown.__cause__ = verdict.error
        return unknown
    error: Exception = verdict.original or verdict.error
    if spec.kind is Kind.TRADE:
        attach_key(error, spec.idempotency_key)
    return error


def attach_key(error: BaseException, key: str | None) -> None:
    """Set the trade call's idempotency key on an error it raises, where the object can carry
    one. On the SDK's own errors it is always there; on a cancellation or another library's
    error it is a convenience only: the dependable way is a key made with new_key() and saved
    before sending."""
    if key is None:
        return
    with contextlib.suppress(AttributeError, TypeError):
        error.idempotency_key = key  # type: ignore[attr-defined]


def next_wait(verdict: Verdict, retries: int, settings: Settings) -> float | None:
    """How long to wait before the next attempt, or None if this answer isn't retried."""
    if verdict.retry == "no":
        return None
    if verdict.retry_after is not None:
        return float(verdict.retry_after)
    nominal = min(FIRST_WAIT * 2**retries, LONGEST_WAIT)
    return nominal * (0.5 + 0.5 * settings.random())
