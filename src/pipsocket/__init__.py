"""Python client for the Pipsocket API.

    from pipsocket import Pipsocket

    ps = Pipsocket()  # reads PIPSOCKET_KEY
    acct = ps.account("acc_01m6tx3k9f2qw8r7bznd5yvh4c")
    print(acct.get()["balance"])

See https://docs.pipsocket.com for the API itself.
"""

from ._core import new_key
from ._version import __version__
from .aio import AsyncPipsocket
from .client import Pipsocket
from .errors import (
    AccountUnavailable,
    APIConnectionError,
    APIError,
    APITimeoutError,
    AuthenticationError,
    BillingHold,
    EngineBusy,
    InvalidRequest,
    KillSwitchEngaged,
    NoPrice,
    NotFound,
    OutcomeUnknown,
    PipsocketError,
    RangeTooLarge,
    StreamError,
    StreamHandshakeError,
    SymbolBusy,
    TerminalError,
    TerminalTimeout,
    TradeNotAccepted,
    Unavailable,
)
from .models import TradeRequest

__all__ = [
    "APIConnectionError",
    "APIError",
    "APITimeoutError",
    "AccountUnavailable",
    "AsyncPipsocket",
    "AuthenticationError",
    "BillingHold",
    "EngineBusy",
    "InvalidRequest",
    "KillSwitchEngaged",
    "NoPrice",
    "NotFound",
    "OutcomeUnknown",
    "Pipsocket",
    "PipsocketError",
    "RangeTooLarge",
    "StreamError",
    "StreamHandshakeError",
    "SymbolBusy",
    "TerminalError",
    "TerminalTimeout",
    "TradeNotAccepted",
    "TradeRequest",
    "Unavailable",
    "__version__",
    "new_key",
]
