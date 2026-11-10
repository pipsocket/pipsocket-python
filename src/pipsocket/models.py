"""TradeRequest: a recorded trade request, as a trade call, a lookup or the stream gives it."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from .errors import TradeNotAccepted
from .types import RequestView, SentRequest

__all__ = ["ACCEPTED_STATES", "FINAL_STATES", "OUTSTANDING_STATES", "TradeRequest"]

# MT5 took the request.
ACCEPTED_STATES = frozenset({"completed", "placed", "partial"})
# Nothing more happens to the request.
FINAL_STATES = ACCEPTED_STATES | {"rejected", "not_sent", "not_executed", "executed"}
# No outcome yet: it may still execute.
OUTSTANDING_STATES = frozenset({"pending", "unknown"})
# `result` is MT5's own answer (a TradeResult) in these states.
_TRADE_SERVER_ANSWERED = ACCEPTED_STATES | {"rejected"}
# raise_for_state() raises for these.
_NOT_ACCEPTED = frozenset({"rejected", "not_sent", "not_executed"})


class TradeRequest:
    """A recorded trade request. The wire body is kept untouched in `raw`.

    `state` is the API's string as it came: one of the nine known states, or anything a newer
    API sends (treated as neither accepted nor final). `result` depends on the state:

    - completed, placed, partial, rejected: MT5's answer (`retcode`, `retcode_name`, `order`,
      `deal` and the rest)
    - not_sent: the terminal's refusal, `{error, message}`
    - not_executed: the refusal when `settlement` is "evidence"; who settled it and why when it
      is "operator"
    - executed: who settled it and why, `{outcome, settled_by, reason}`
    - pending, unknown: None

    `idempotency_key` is set only on the answer to a trade call this client made, and
    `http_status` only on answers from the REST API (not on stream events).
    """

    __slots__ = (
        "error",
        "http_status",
        "id",
        "idempotency_key",
        "message",
        "raw",
        "result",
        "sent",
        "settlement",
        "state",
    )

    id: str
    state: str
    sent: SentRequest
    result: Any
    message: str
    error: str | None
    settlement: str | None
    idempotency_key: str | None
    http_status: int | None
    raw: RequestView

    def __init__(
        self,
        body: RequestView,
        *,
        http_status: int | None = None,
        idempotency_key: str | None = None,
    ) -> None:
        if not is_recorded_request(body):
            raise ValueError("Not a recorded request: it needs a string `request` and `state`.")
        self.raw = body
        self.id = body["request"]
        self.state = body["state"]
        self.sent = body.get("sent", cast(SentRequest, {}))
        self.result = body.get("result")
        self.message = body.get("message", "")
        self.error = body.get("error")
        self.settlement = body.get("settlement")
        self.idempotency_key = idempotency_key
        self.http_status = http_status

    @classmethod
    def from_event(cls, message: Mapping[str, Any]) -> TradeRequest:
        """The TradeRequest for a stream `request` event. It has no idempotency key or status."""
        if message.get("type") != "request":
            raise ValueError(f"Not a request event: its type is {message.get('type')!r}.")
        return cls(cast(RequestView, dict(message)))

    @property
    def accepted(self) -> bool:
        """MT5 took it: completed, placed or partial."""
        return self.state in ACCEPTED_STATES

    @property
    def final(self) -> bool:
        """Nothing more happens to it. False for a state this SDK doesn't know."""
        return self.state in FINAL_STATES

    @property
    def outstanding(self) -> bool:
        """No outcome yet (pending or unknown): it may still execute."""
        return self.state in OUTSTANDING_STATES

    @property
    def retcode(self) -> int | None:
        """MT5's retcode, when `result` is MT5's answer."""
        value = self._trade_result().get("retcode")
        return value if isinstance(value, int) and not isinstance(value, bool) else None

    @property
    def retcode_name(self) -> str | None:
        """The retcode's name, such as TRADE_RETCODE_DONE, when `result` is MT5's answer."""
        value = self._trade_result().get("retcode_name")
        return value if isinstance(value, str) else None

    def raise_for_state(self) -> TradeRequest:
        """Raise TradeNotAccepted if the request is rejected, not_sent or not_executed.

        Returns the request otherwise, pending and unknown included: those may still execute.
        """
        if self.state in _NOT_ACCEPTED:
            raise TradeNotAccepted(self)
        return self

    def _trade_result(self) -> Mapping[str, Any]:
        result: object = self.result
        if self.state in _TRADE_SERVER_ANSWERED and isinstance(result, Mapping):
            return cast(Mapping[str, Any], result)
        return {}

    def __repr__(self) -> str:
        return f"TradeRequest(id={self.id!r}, state={self.state!r})"


def is_recorded_request(body: object) -> bool:
    """The one check made on a body: a JSON object with a string `request` and `state`."""
    if not isinstance(body, Mapping):
        return False
    fields = cast(Mapping[str, object], body)
    return isinstance(fields.get("request"), str) and isinstance(fields.get("state"), str)
