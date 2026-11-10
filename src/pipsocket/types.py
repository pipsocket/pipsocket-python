"""The API's answer and request types, generated from its OpenAPI document.

Answers are plain dicts; these TypedDicts describe them for type checkers and editors. Nothing
is checked at run time, so a field or value a newer API adds is passed through as it is. Enums
in answers are open (a known value or any other string), enums in requests closed: the closed
copy of an enum used on both sides has the suffix "Param".
"""

from ._generated.types import (
    AccountView as AccountView,
    Action as Action,
    Blocking as Blocking,
    CheckResult as CheckResult,
    CheckView as CheckView,
    CloseBy as CloseBy,
    ClosePosition as ClosePosition,
    DealView as DealView,
    EngageKillSwitch as EngageKillSwitch,
    ErrorBody as ErrorBody,
    Filling as Filling,
    FillingParam as FillingParam,
    HandSettlement as HandSettlement,
    HistoryOrderView as HistoryOrderView,
    KillSwitchNotConnected as KillSwitchNotConnected,
    KillSwitchOutcome as KillSwitchOutcome,
    KillSwitchView as KillSwitchView,
    ModifyOrder as ModifyOrder,
    OrderType as OrderType,
    OrderTypeParam as OrderTypeParam,
    PendingOrderView as PendingOrderView,
    PlaceOrder as PlaceOrder,
    PositionLevels as PositionLevels,
    PositionView as PositionView,
    Remaining as Remaining,
    RequestResult as RequestResult,
    RequestState as RequestState,
    RequestView as RequestView,
    SentRequest as SentRequest,
    Settlement as Settlement,
    SweptOrder as SweptOrder,
    SweptPosition as SweptPosition,
    SymbolSummaryView as SymbolSummaryView,
    SymbolView as SymbolView,
    TickView as TickView,
    TradeResult as TradeResult,
    TypeTime as TypeTime,
    TypeTimeParam as TypeTimeParam,
)
