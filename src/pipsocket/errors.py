"""The exceptions the SDK raises.

An exception means there is no recorded request in hand: either nothing was sent, or the
outcome is unknown and `OutcomeUnknown` carries the idempotency key to resolve it with. A trade
that MT5 rejected, or that the terminal did not send, is returned as a `TradeRequest`, not raised.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .models import TradeRequest
    from .types import Blocking, KillSwitchView

__all__ = [
    "APIConnectionError",
    "APIError",
    "APITimeoutError",
    "AccountUnavailable",
    "AuthenticationError",
    "BillingHold",
    "EngineBusy",
    "InvalidRequest",
    "KillSwitchEngaged",
    "NoPrice",
    "NotFound",
    "OutcomeUnknown",
    "PipsocketError",
    "RangeTooLarge",
    "SymbolBusy",
    "TerminalError",
    "TerminalTimeout",
    "TradeNotAccepted",
    "Unavailable",
]


class PipsocketError(Exception):
    """The base of every exception this SDK raises.

    `idempotency_key` is set on every exception a trade call raises: call again with it to get
    the recorded request, or send the trade if it never arrived. None on other calls.
    """

    idempotency_key: str | None = None


class APIConnectionError(PipsocketError):
    """The network failed: no usable answer came back."""


class APITimeoutError(APIConnectionError):
    """An attempt ran out of time before its answer arrived."""


class APIError(PipsocketError):
    """The API refused the call, or answered with something that isn't its JSON.

    `code` is the API's `error` code (None when the answer had none), `body` the parsed JSON
    (None when it wasn't JSON), and `raw` the answer's bytes as they came.
    """

    status: int
    code: str | None
    message: str
    body: Any
    raw: bytes

    def __init__(
        self, message: str, *, status: int, code: str | None, body: Any, raw: bytes
    ) -> None:
        super().__init__(message)
        self.message = message
        self.status = status
        self.code = code
        self.body = body
        self.raw = raw

    def __str__(self) -> str:
        label = f"{self.status} {self.code}" if self.code else str(self.status)
        return f"{label}: {self.message}"

    def __repr__(self) -> str:
        return f"{type(self).__name__}(status={self.status}, code={self.code!r})"


class AuthenticationError(APIError):
    """401: no API key, or one that isn't the workspace's current key."""


class NotFound(APIError):
    """404: no such account, order, position, request or symbol."""


class InvalidRequest(APIError):
    """A field, query parameter or header was refused (422), or a path held a ticket that isn't a
    whole number (400, with no body). Nothing was sent."""


class RangeTooLarge(APIError):
    """422 range_too_large: ask for a shorter history range."""


class SymbolBusy(APIError):
    """409 symbol_busy: an earlier request on the symbol has no outcome yet. Nothing was sent.

    `blocking` lists those requests with their states. `retry_after` is the answer's Retry-After
    in seconds, or None when a blocking request is `unknown`: look it up instead of retrying.
    """

    blocking: list[Blocking]
    retry_after: int | None

    def __init__(
        self,
        message: str,
        *,
        status: int,
        code: str | None,
        body: Any,
        raw: bytes,
        blocking: list[Blocking],
        retry_after: int | None,
    ) -> None:
        super().__init__(message, status=status, code=code, body=body, raw=raw)
        self.blocking = blocking
        self.retry_after = retry_after


class KillSwitchEngaged(APIError):
    """423 kill_switch_engaged: new exposure is refused until the kill switch is lifted."""


class BillingHold(APIError):
    """423 billing_hold: the account is close-only until the workspace's payment goes through."""


class AccountUnavailable(APIError):
    """503: the account's terminal isn't connected, runs an outdated bridge, or is restarting.

    `kill_switch` is set when engaging the kill switch answered this way: the switch is engaged
    all the same, and the sweep isn't finished.
    """

    kill_switch: KillSwitchView | None

    def __init__(
        self,
        message: str,
        *,
        status: int,
        code: str | None,
        body: Any,
        raw: bytes,
        kill_switch: KillSwitchView | None = None,
    ) -> None:
        super().__init__(message, status=status, code=code, body=body, raw=raw)
        self.kill_switch = kill_switch


class EngineBusy(APIError):
    """503 engine_busy: the account's host had too many calls in flight. Nothing was done."""


class Unavailable(APIError):
    """503 engine_unavailable or 504 engine_timeout, on a call that is safe to repeat."""


class TerminalTimeout(APIError):
    """504 terminal_timeout: the account's terminal did not answer within 5 seconds."""


class TerminalError(APIError):
    """503 terminal_error, bad_command or symbol_too_large: the terminal did not give the answer."""


class NoPrice(APIError):
    """503 no_price: the terminal has no price for the symbol yet."""


class OutcomeUnknown(PipsocketError):
    """A trade call may have reached the account's host, and no recorded request came back.

    The trade may or may not have been sent. Call again with the same `idempotency_key` to get
    the recorded request (or send it, if it never arrived), or read positions and history first.
    `cause` is the last error, `status` and `raw` the last answer's status and bytes, if any.
    """

    idempotency_key: str  # pyright: ignore[reportIncompatibleVariableOverride]
    cause: PipsocketError
    status: int | None
    raw: bytes | None

    def __init__(
        self,
        *,
        idempotency_key: str,
        cause: PipsocketError,
        status: int | None,
        raw: bytes | None,
    ) -> None:
        super().__init__(
            "The trade may have reached the account's host, and its outcome is unknown. "
            "Call again with the same idempotency_key to get it, or check positions and "
            f"history. Last error: {cause}"
        )
        self.idempotency_key = idempotency_key  # pyright: ignore[reportIncompatibleVariableOverride]
        self.cause = cause
        self.status = status
        self.raw = raw


class TradeNotAccepted(PipsocketError):
    """Raised by `TradeRequest.raise_for_state()` for a rejected, not_sent or not_executed
    request."""

    request: TradeRequest

    def __init__(self, request: TradeRequest) -> None:
        super().__init__(f"Request {request.id} is {request.state}: {request.message}")
        self.request = request


# The API's error codes and the exception each raises. Codes not listed raise a plain APIError.
# engine_unavailable and engine_timeout raise Unavailable only on calls that are safe to repeat;
# on a trade call they mean the outcome is unknown (see _core).
ERROR_CLASSES: dict[str, type[APIError]] = {
    "missing_api_key": AuthenticationError,
    "invalid_api_key": AuthenticationError,
    "no_such_account": NotFound,
    "no_such_order": NotFound,
    "no_such_position": NotFound,
    "no_such_request": NotFound,
    "no_such_symbol": NotFound,
    "invalid_request": InvalidRequest,
    "magic_reserved": InvalidRequest,
    "range_too_large": RangeTooLarge,
    "symbol_busy": SymbolBusy,
    "kill_switch_engaged": KillSwitchEngaged,
    "billing_hold": BillingHold,
    "account_not_connected": AccountUnavailable,
    "bridge_outdated": AccountUnavailable,
    "account_restarting": AccountUnavailable,
    "engine_busy": EngineBusy,
    "engine_unavailable": Unavailable,
    "engine_timeout": Unavailable,
    "terminal_timeout": TerminalTimeout,
    "terminal_error": TerminalError,
    "bad_command": TerminalError,
    "symbol_too_large": TerminalError,
    "no_price": NoPrice,
    # Recorded requests come back as TradeRequest. A refusal with one of these codes but no
    # request in its body isn't what the API sends, so it stays a plain APIError.
    "trade_rejected": APIError,
    "not_executed": APIError,
    "not_sent": APIError,
}
