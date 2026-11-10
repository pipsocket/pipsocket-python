"""The stream helper's state machine. No I/O.

The shells in stream.py feed it inputs (the socket opened, a message arrived, a timer fired, a
REST read answered, the caller set ticks or closed) and carry out the actions it returns (open
or close the socket, send a text, start or cancel a timer, make a REST read, hand an event to the
caller, notify a wait(), stop with an error).

    connecting --> failed      refused (401, 404, 426 or another 4xx)
    connecting --> backoff     network error, or a 5xx from the upgrade
    connecting --> syncing     subscribed
    syncing --> live           ticks set, snapshot read, resync event sent
    syncing --> syncing        lagged, or the local queue overflowed: start again
    syncing --> host_away      engine_disconnected: start again once the host is back
    syncing --> failed         401 or 404 on a resync read
    syncing --> backoff        socket closed, no pong, or the resync not done in 120 s
    live --> syncing           lagged, or the local queue overflowed
    live --> host_away         engine_disconnected
    host_away --> syncing      an event arrives, or the probe read answers
    host_away --> failed       401 or 404 on the probe read
    host_away --> backoff      socket closed, or no pong
    live --> backoff           socket closed, or no pong
    backoff --> connecting     0.5 s doubling to 30 s, with jitter
    any --> closed             close()
"""

from __future__ import annotations

import json
from collections import deque
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Literal, cast

from ._core import refusal_from_body
from .errors import AccountUnavailable, AuthenticationError, NotFound, PipsocketError, RangeTooLarge

Json = dict[str, Any]

# Timings, in seconds.
PING_EVERY = 20.0
PONG_WITHIN = 10.0
SUBSCRIBED_WITHIN = 30.0
RESYNC_WITHIN = 120.0
FIRST_BACKOFF = 0.5
LONGEST_BACKOFF = 30.0
FIRST_PROBE = 2.0
LONGEST_PROBE = 30.0
# History: read back from the checkpoint less this margin, up to the newest time plus a day.
HISTORY_MARGIN = 300
HISTORY_AHEAD = 86400
SMALLEST_SLICE = 60
# How many deal and order tickets to remember for deduplicating history.
REMEMBERED_TICKETS = 10_000
# The API's events. Anything else (answers to calls, refusals of them) isn't one.
EVENTS = frozenset({"request", "deal", "transaction", "account", "status", "tick"})


class State(Enum):
    CONNECTING = "connecting"
    SYNCING = "syncing"
    LIVE = "live"
    HOST_AWAY = "host_away"
    BACKOFF = "backoff"
    FAILED = "failed"
    CLOSED = "closed"


Reason = Literal["connected", "reconnected", "lagged", "host_back", "overflow"]


# Inputs


@dataclass(frozen=True)
class Start:
    """The caller opened the stream."""


@dataclass(frozen=True)
class Opened:
    """The upgrade succeeded."""


@dataclass(frozen=True)
class Refused:
    """The upgrade was answered with a status other than 101."""

    status: int
    error: PipsocketError


@dataclass(frozen=True)
class Unreachable:
    """The upgrade failed for a network reason: no status."""

    error: Exception


@dataclass(frozen=True)
class Closed:
    """The socket closed."""


@dataclass(frozen=True)
class Text:
    """A text message that isn't JSON (the stream's "pong")."""

    text: str


@dataclass(frozen=True)
class Message:
    """A JSON message from the stream."""

    body: Json


@dataclass(frozen=True)
class Timer:
    name: str
    generation: int


@dataclass(frozen=True)
class Answered:
    """A REST read the machine asked for: its value, or the error it raised."""

    read: Read
    value: Any = None
    error: PipsocketError | None = None


@dataclass(frozen=True)
class SetTicks:
    symbols: tuple[str, ...]


@dataclass(frozen=True)
class Overflowed:
    """The caller fell behind and the shell's queue of events filled up: it was emptied.
    `dropped` are the events that never reached the caller, oldest first."""

    dropped: tuple[Json, ...] = ()


@dataclass(frozen=True)
class Fatal:
    """A read made outside the machine (a wait() lookup) failed. A 401 or 404 for the
    account stops the stream too: it can no longer resync."""

    error: PipsocketError


@dataclass(frozen=True)
class Watch:
    request_id: str


@dataclass(frozen=True)
class Unwatch:
    request_id: str


@dataclass(frozen=True)
class Close:
    """The caller closed the stream."""


Input = (
    Start
    | Opened
    | Refused
    | Unreachable
    | Closed
    | Text
    | Message
    | Timer
    | Answered
    | SetTicks
    | Overflowed
    | Fatal
    | Watch
    | Unwatch
    | Close
)


# Actions


@dataclass(frozen=True)
class Connect:
    pass


@dataclass(frozen=True)
class Disconnect:
    pass


@dataclass(frozen=True)
class Send:
    text: str


@dataclass(frozen=True)
class StartTimer:
    name: str
    seconds: float
    generation: int


@dataclass(frozen=True)
class Read:
    """A REST read: kind is account, positions, orders, deals, history_orders or request."""

    kind: str
    generation: int
    request_id: str | None = None
    from_: int | None = None
    to: int | None = None


@dataclass(frozen=True)
class Emit:
    event: Json


@dataclass(frozen=True)
class Notify:
    """A recorded request seen on the stream or in a resync lookup, for wait() and for
    trade calls to reconcile a slower REST answer with."""

    body: Json


@dataclass(frozen=True)
class Fail:
    error: PipsocketError


Action = Connect | Disconnect | Send | StartTimer | Read | Emit | Notify | Fail


# The resync in progress.


@dataclass
class Resync:
    reason: Reason
    generation: int
    # Waiting for the socket's latest requested tick set to be acknowledged before reading.
    waiting_ticks: bool = False
    account: Json | None = None
    positions: list[Json] | None = None
    orders: list[Json] | None = None
    terminal_away: bool = False
    history_reason: str | None = None
    history_from: int | None = None
    history_to: int | None = None
    deals: list[Json] = field(default_factory=list[Json])
    history_orders: list[Json] = field(default_factory=list[Json])
    # Ranges still to read, as (kind, from, to).
    ranges: deque[tuple[str, int, int]] = field(default_factory=deque[tuple[str, int, int]])
    lookups: set[str] = field(default_factory=set[str])
    waiting: Read | None = None
    # A different login or server than the checkpoint's: committed when the resync finishes.
    new_identity: tuple[Any, Any] | None = None


@dataclass
class Checkpoint:
    server_time: int | None = None
    login: int | None = None
    server: str | None = None


class StreamMachine:
    def __init__(
        self,
        *,
        ticks: Iterable[str] = (),
        read_state: bool = True,
        queue_limit: int = 1000,
        random: Callable[[], float],
    ) -> None:
        self.state = State.CONNECTING
        self.ticks: tuple[str, ...] = tuple(ticks)
        self.read_state = read_state
        self.queue_limit = queue_limit
        self.random = random
        self.watched: set[str] = set()
        self.checkpoint = Checkpoint()
        self.connected_before = False
        self.socket_open = False
        self.subscribed = False
        self.failures = 0
        self.probe_every = FIRST_PROBE
        self.resync: Resync | None = None
        self.probe: Read | None = None
        self.pending_reason: Reason | None = None
        self.held: list[Json] = []
        self.newest_ticks: dict[str, Json] = {}
        self.terminal_away = False
        self.calls = 0
        # One /ticks call at a time: calls on a stream run side by side, so the set last sent
        # must be the one the stream keeps.
        self.ticks_call: str | None = None
        self.ticks_sending: tuple[str, ...] = ()
        self.ticks_acknowledged: tuple[str, ...] = ()
        self.generations: dict[str, int] = {}
        self.read_generation = 0
        self.recent_deals: deque[int] = deque(maxlen=REMEMBERED_TICKETS)
        self.recent_orders: deque[int] = deque(maxlen=REMEMBERED_TICKETS)
        # Account events emitted, with the checkpoint before each: an overflow takes back
        # the ones the caller never got.
        self.account_events: deque[tuple[Json, int | None]] = deque(maxlen=4 * queue_limit + 64)
        self.out: list[Action] = []

    # The one entry point.

    def handle(self, event: Input) -> list[Action]:
        self.out = []
        if self.state in (State.FAILED, State.CLOSED):
            return []
        match event:
            case Start():
                self.connect()
            case Close():
                self.close()
            case Opened():
                self.opened()
            case Refused(status=status, error=error):
                self.refused(status, error)
            case Unreachable():
                self.lost()
            case Closed():
                self.lost()
            case Text(text=text):
                if text == "pong":
                    self.bump("pong")
            case Message(body=body):
                self.message(body)
            case Timer(name=name, generation=generation):
                if self.generations.get(name) == generation:
                    self.timer(name)
            case Answered():
                self.answered(event)
            case SetTicks(symbols=symbols):
                self.set_ticks(symbols)
            case Overflowed(dropped=dropped):
                self.take_back(dropped)
                self.overflowed()
            case Fatal(error=error):
                if fatal(error):
                    self.fail(error)
            case Watch(request_id=request_id):
                self.watched.add(request_id)
            case Unwatch(request_id=request_id):
                self.watched.discard(request_id)
        return self.out

    # Timers: starting one with a name replaces the one before; bump() cancels it.

    def start_timer(self, name: str, seconds: float) -> None:
        generation = self.generations.get(name, 0) + 1
        self.generations[name] = generation
        self.out.append(StartTimer(name, seconds, generation))

    def bump(self, name: str) -> None:
        self.generations[name] = self.generations.get(name, 0) + 1

    def stop_timers(self) -> None:
        for name in list(self.generations):
            self.bump(name)

    # Connecting and losing the socket.

    def connect(self) -> None:
        self.state = State.CONNECTING
        self.subscribed = False
        self.out.append(Connect())

    def opened(self) -> None:
        if self.state is not State.CONNECTING:
            return
        self.socket_open = True
        self.start_timer("subscribed", SUBSCRIBED_WITHIN)
        self.start_timer("ping", PING_EVERY)

    def refused(self, status: int, error: PipsocketError) -> None:
        if status >= 500 or status == 429:
            self.lost()
        else:
            self.fail(error)

    def lost(self) -> None:
        """The socket closed or never opened: wait, then connect again from scratch."""
        if self.state in (State.BACKOFF, State.CLOSED, State.FAILED):
            return
        if self.socket_open:
            self.out.append(Disconnect())
        self.socket_open = False
        self.subscribed = False
        self.stop_timers()
        self.abandon_resync()
        self.ticks_call = None
        self.ticks_acknowledged = ()
        self.state = State.BACKOFF
        self.start_timer("backoff", self.backoff())

    def backoff(self) -> float:
        """0.5 s doubling to 30 s, each times a random factor from 0.5 to 1."""
        nominal = FIRST_BACKOFF * 2 ** min(self.failures, 16)
        self.failures += 1
        return min(nominal, LONGEST_BACKOFF) * (0.5 + 0.5 * self.random())

    def close(self) -> None:
        if self.socket_open:
            self.out.append(Disconnect())
        self.socket_open = False
        self.stop_timers()
        self.state = State.CLOSED

    def fail(self, error: PipsocketError) -> None:
        if self.socket_open:
            self.out.append(Disconnect())
        self.socket_open = False
        self.stop_timers()
        self.state = State.FAILED
        self.out.append(Fail(error))

    def timer(self, name: str) -> None:
        if name == "backoff" and self.state is State.BACKOFF:
            self.connect()
        elif name == "ping" and self.socket_open:
            self.out.append(Send("ping"))
            self.start_timer("pong", PONG_WITHIN)
            self.start_timer("ping", PING_EVERY)
        elif name in ("pong", "subscribed", "resync_deadline"):
            # The socket is dead, never subscribed, or a resync is stuck: start over.
            self.lost()
        elif name == "ticks_retry":
            self.send_ticks_if_due()
        elif name == "resync_retry" and self.state is State.SYNCING:
            self.begin_resync_reads()
        elif name == "probe" and self.state is State.HOST_AWAY:
            self.read_generation += 1
            self.probe = Read("account", self.read_generation)
            self.out.append(self.probe)

    # Messages.

    def message(self, body: Json) -> None:
        kind = body.get("type")
        if kind == "subscribed":
            if self.state is State.CONNECTING and not self.subscribed:
                self.subscribed = True
                self.bump("subscribed")
                reason: Reason = "reconnected" if self.connected_before else "connected"
                self.connected_before = True
                self.begin_resync(reason)
            return
        if kind in ("response", "error") and body.get("id") == self.ticks_call:
            self.ticks_answered(body)
            return
        if kind == "request":
            self.out.append(Notify(body))
        if self.state is State.SYNCING:
            self.during_resync(body)
        elif self.state is State.LIVE:
            self.while_live(body)
        elif self.state is State.HOST_AWAY:
            self.host_back_with(body)

    def while_live(self, body: Json) -> None:
        kind = body.get("type")
        if kind == "lagged":
            self.emit(body)
            self.begin_resync("lagged")
        elif kind == "engine_disconnected":
            self.emit(body)
            self.host_away()
        else:
            self.deliver(body)
            if kind == "status" and self.terminal_away and body.get("status") == "connected":
                # The last snapshot had no positions or orders: the terminal is back now.
                self.begin_resync("host_back")

    def during_resync(self, body: Json) -> None:
        kind = body.get("type")
        if kind == "lagged":
            self.begin_resync("lagged")
        elif kind == "engine_disconnected":
            self.host_away()
        else:
            self.hold(body)

    def host_back_with(self, body: Json) -> None:
        if body.get("type") not in EVENTS:
            return
        self.begin_resync("host_back")
        self.hold(body)

    def hold(self, body: Json) -> None:
        """Keep an event that arrives during a resync, to deliver after it."""
        kind = body.get("type")
        if kind == "account":
            return
        if kind == "tick":
            symbol = str(body.get("symbol"))
            kept = self.newest_ticks.get(symbol)
            if kept is None or (body.get("time_msc") or 0) >= (kept.get("time_msc") or 0):
                self.newest_ticks[symbol] = body
            return
        if kind in ("response", "error"):
            return
        self.held.append(body)
        if len(self.held) > self.queue_limit:
            self.overflowed()

    def host_away(self) -> None:
        self.abandon_resync()
        self.state = State.HOST_AWAY
        self.probe_every = FIRST_PROBE
        self.start_timer("probe", self.probe_every)

    def overflowed(self) -> None:
        if self.state not in (State.LIVE, State.SYNCING):
            return
        self.held.clear()
        self.newest_ticks.clear()
        self.begin_resync("overflow")

    def take_back(self, dropped: tuple[Json, ...]) -> None:
        """Events the caller never got don't count as delivered: the checkpoint goes back to
        before the first dropped account event, and dropped deals aren't remembered."""
        for body in dropped:
            if body.get("type") == "deal":
                forget(self.recent_deals, deal_ticket(body))
            elif body.get("type") == "resync" and isinstance(body.get("history"), dict):
                history = cast(Json, body["history"])
                for deal in history.get("deals", []):
                    forget(self.recent_deals, deal_ticket(deal))
                for order in history.get("orders", []):
                    forget(self.recent_orders, order_ticket(order))
        dropped_ids = {id(body) for body in dropped if body.get("type") == "account"}
        while self.account_events and id(self.account_events[-1][0]) in dropped_ids:
            _, before = self.account_events.pop()
            self.checkpoint.server_time = before

    # Delivering.

    def deliver(self, body: Json) -> None:
        kind = body.get("type")
        if kind == "account":
            server_time = body.get("server_time")
            if isinstance(server_time, int) and not isinstance(server_time, bool):
                before = self.checkpoint.server_time
                self.account_events.append((body, before))
                self.checkpoint.server_time = max(before or 0, server_time)
        elif kind == "deal":
            ticket = deal_ticket(body)
            if ticket is not None:
                self.recent_deals.append(ticket)
        self.emit(body)

    def emit(self, body: Json) -> None:
        self.out.append(Emit(body))

    # Ticks.

    def set_ticks(self, symbols: tuple[str, ...]) -> None:
        """Remember the set, and send it once the socket is subscribed and no other /ticks
        call is unanswered."""
        self.ticks = symbols
        self.send_ticks_if_due()

    def send_ticks_if_due(self) -> None:
        if not self.subscribed or self.ticks_call is not None:
            return
        if self.ticks == self.ticks_acknowledged:
            return
        self.calls += 1
        self.ticks_call = f"ticks-{self.calls}"
        self.ticks_sending = self.ticks
        body = {
            "id": self.ticks_call,
            "method": "PUT",
            "path": "/ticks",
            "body": {"symbols": list(self.ticks)},
        }
        self.out.append(Send(json.dumps(body)))

    def ticks_settled(self) -> bool:
        return self.ticks_call is None and self.ticks == self.ticks_acknowledged

    def ticks_answered(self, body: Json) -> None:
        self.ticks_call = None
        status = body.get("status")
        if body.get("type") == "response" and status == 200:
            self.ticks_acknowledged = self.ticks_sending
            self.send_ticks_if_due()
            resync = self.resync
            if resync is not None and resync.waiting_ticks and self.ticks_settled():
                resync.waiting_ticks = False
                self.after_ticks()
            return
        if body.get("type") == "response" and isinstance(status, int) and 400 <= status < 500:
            self.fail(ticks_refused(status, body.get("body")))
            return
        # Refused by the stream itself (too_many_calls): try the resync again, or send the
        # set again later.
        if self.resync is not None and self.resync.waiting_ticks:
            self.resync_failed()
        else:
            self.start_timer("ticks_retry", self.backoff())

    # Resync.

    def begin_resync(self, reason: Reason) -> None:
        restarting = self.state is State.SYNCING and self.resync is not None
        if restarting and self.resync is not None:
            # A gap during a resync: start again, so the snapshot is read after it. A new
            # socket's resync keeps its reason, and its ticks if they aren't set yet. The 120 s
            # deadline still counts from the first start, so gaps can't keep it syncing.
            if self.resync.reason in ("connected", "reconnected"):
                reason = self.resync.reason
            self.bump("resync_retry")
        else:
            self.abandon_resync()
            self.start_timer("resync_deadline", RESYNC_WITHIN)
        self.state = State.SYNCING
        self.read_generation += 1
        self.resync = Resync(reason, self.read_generation)
        self.begin_resync_reads()

    def abandon_resync(self) -> None:
        if self.resync is not None:
            self.bump("resync_retry")
            self.bump("resync_deadline")
        self.resync = None

    def begin_resync_reads(self) -> None:
        resync = self.resync
        assert resync is not None
        self.read_generation += 1
        resync.generation = self.read_generation
        # Every resync reads after the socket's latest tick set is acknowledged: on a new socket
        # that is the set sent again, and a set asked for meanwhile waits its turn too.
        self.send_ticks_if_due()
        if not self.ticks_settled():
            resync.waiting_ticks = True
            return
        self.after_ticks()

    def after_ticks(self) -> None:
        resync = self.resync
        assert resync is not None
        if self.read_state:
            self.read(resync, Read("account", resync.generation))
        else:
            self.finish_resync()

    def read(self, resync: Resync, read: Read) -> None:
        resync.waiting = read
        self.out.append(read)

    def answered(self, answer: Answered) -> None:
        read = answer.read
        if answer.error is not None and fatal(answer.error):
            # The key was rolled or the account is gone: that holds whether or not this read's
            # answer is still wanted.
            self.fail(answer.error)
            return
        if read == self.probe:
            self.probe = None
            if self.state is State.HOST_AWAY:
                self.probe_answered(answer)
            return
        resync = self.resync
        if self.state is not State.SYNCING or resync is None or read != resync.waiting:
            return
        resync.waiting = None
        error = answer.error
        if error is not None:
            if fatal(error):
                self.fail(error)
            elif read.kind in ("positions", "orders") and isinstance(error, AccountUnavailable):
                resync.terminal_away = True
                self.next_after_snapshot(resync)
            elif read.kind in ("deals", "history_orders") and isinstance(error, RangeTooLarge):
                self.split_range(resync, read)
            elif read.kind in ("deals", "history_orders") and isinstance(error, AccountUnavailable):
                self.history_unavailable(resync, "unavailable")
            elif read.kind == "request":
                resync.lookups.discard(read.request_id or "")
                self.next_lookup(resync)
            else:
                self.resync_failed()
            return

        value = answer.value
        if read.kind == "account":
            resync.account = value
            self.read(resync, Read("positions", resync.generation))
        elif read.kind == "positions":
            resync.positions = value
            self.read(resync, Read("orders", resync.generation))
        elif read.kind == "orders":
            resync.orders = value
            self.next_after_snapshot(resync)
        elif read.kind in ("deals", "history_orders"):
            target = resync.deals if read.kind == "deals" else resync.history_orders
            target.extend(value)
            self.next_range(resync)
        elif read.kind == "request":
            resync.lookups.discard(read.request_id or "")
            if isinstance(value, dict):
                self.out.append(Notify(cast(Json, value)))
            self.next_lookup(resync)

    def next_after_snapshot(self, resync: Resync) -> None:
        if resync.terminal_away:
            resync.positions = None
            resync.orders = None
            # History needs the terminal too: report it unavailable, keep the checkpoint.
            self.history_unavailable(resync, "unavailable")
            return
        self.plan_history(resync)

    def plan_history(self, resync: Resync) -> None:
        account = resync.account or {}
        login, server = account.get("login"), account.get("server")
        checkpoint = self.checkpoint
        if checkpoint.login is not None and (login, server) != (
            checkpoint.login,
            checkpoint.server,
        ):
            resync.new_identity = (login, server)
            self.history_unavailable(resync, "account_changed")
            return
        if checkpoint.server_time is None:
            self.history_unavailable(resync, "no_checkpoint")
            return
        newest = checkpoint.server_time
        server_time = account.get("server_time")
        if isinstance(server_time, int) and not isinstance(server_time, bool):
            newest = max(newest, server_time)
            start = min(checkpoint.server_time, server_time) - HISTORY_MARGIN
        else:
            start = checkpoint.server_time - HISTORY_MARGIN
        resync.history_from = max(start, 0)
        resync.history_to = newest + HISTORY_AHEAD
        resync.ranges = deque(
            [
                ("deals", resync.history_from, resync.history_to),
                ("history_orders", resync.history_from, resync.history_to),
            ]
        )
        self.next_range(resync)

    def split_range(self, resync: Resync, read: Read) -> None:
        assert read.from_ is not None
        assert read.to is not None
        if read.to - read.from_ < 2 * SMALLEST_SLICE:
            self.history_unavailable(resync, "range_too_large")
            return
        middle = (read.from_ + read.to) // 2
        resync.ranges.appendleft((read.kind, middle + 1, read.to))
        resync.ranges.appendleft((read.kind, read.from_, middle))
        self.next_range(resync)

    def next_range(self, resync: Resync) -> None:
        if resync.ranges:
            kind, start, end = resync.ranges.popleft()
            self.read(resync, Read(kind, resync.generation, from_=start, to=end))
            return
        self.begin_lookups(resync)

    def history_unavailable(self, resync: Resync, reason: str) -> None:
        resync.history_reason = reason
        resync.ranges.clear()
        resync.deals.clear()
        resync.history_orders.clear()
        self.begin_lookups(resync)

    def begin_lookups(self, resync: Resync) -> None:
        resync.lookups = set(self.watched)
        self.next_lookup(resync)

    def next_lookup(self, resync: Resync) -> None:
        if resync.lookups:
            request_id = sorted(resync.lookups)[0]
            self.read(resync, Read("request", resync.generation, request_id=request_id))
            return
        self.finish_resync()

    def resync_failed(self) -> None:
        """A resync read failed in a way that may mend: try the resync again after a wait."""
        resync = self.resync
        assert resync is not None
        resync.waiting = None
        self.start_timer("resync_retry", self.backoff())
        # Start the next attempt from a clean slate; held events are kept.
        self.resync = Resync(resync.reason, resync.generation)

    def finish_resync(self) -> None:
        resync = self.resync
        assert resync is not None
        self.bump("resync_deadline")
        self.failures = 0
        if resync.new_identity is not None:
            # A different login: its own checkpoint, and nothing remembered from the old one.
            login, server = resync.new_identity
            self.checkpoint = Checkpoint(None, login, server)
            self.recent_deals.clear()
            self.recent_orders.clear()
            self.account_events.clear()
        history: Json | None = None
        if self.read_state and resync.history_reason is None and resync.history_from is not None:
            deals = self.fresh(resync.deals, self.recent_deals, deal_ticket)
            orders = self.fresh(resync.history_orders, self.recent_orders, order_ticket)
            history = {
                "deals": deals,
                "orders": orders,
                "from": resync.history_from,
                "to": resync.history_to,
            }
        if self.read_state:
            account = resync.account or {}
            if self.checkpoint.login is None:
                self.checkpoint.login = account.get("login")
                self.checkpoint.server = account.get("server")
        self.terminal_away = resync.terminal_away
        event: Json = {
            "type": "resync",
            "reason": resync.reason,
            "account": resync.account,
            "positions": resync.positions,
            "orders": resync.orders,
            "history": history,
            "history_reason": resync.history_reason if self.read_state else None,
        }
        self.resync = None
        self.state = State.LIVE
        self.emit(event)
        waiting = self.held + list(self.newest_ticks.values())
        self.held, self.newest_ticks = [], {}
        for index, body in enumerate(waiting):
            self.while_live(body)
            if self.state is not State.LIVE:
                # That event started another resync (or the host went): the rest wait for it.
                for rest in waiting[index + 1 :]:
                    self.hold(rest)
                return

    def fresh(
        self, entries: list[Json], seen: deque[int], ticket: Callable[[Json], int | None]
    ) -> list[Json]:
        """History entries not already delivered, in order, each ticket once."""
        known = set(seen)
        kept: list[Json] = []
        for entry in entries:
            number = ticket(entry)
            if number is not None and number in known:
                continue
            if number is not None:
                known.add(number)
                seen.append(number)
            kept.append(entry)
        return kept

    # The host away.

    def probe_answered(self, answer: Answered) -> None:
        if answer.error is None:
            self.begin_resync("host_back")
            return
        if fatal(answer.error):
            self.fail(answer.error)
            return
        self.probe_every = min(self.probe_every * 2, LONGEST_PROBE)
        self.start_timer("probe", self.probe_every)


def fatal(error: PipsocketError) -> bool:
    """A failure that won't mend: the key was rolled, or the account is gone."""
    if isinstance(error, AuthenticationError):
        return True
    return isinstance(error, NotFound) and error.code == "no_such_account"


def forget(remembered: deque[int], ticket: int | None) -> None:
    if ticket is not None and ticket in remembered:
        remembered.remove(ticket)


def deal_ticket(body: Json) -> int | None:
    """A deal's ticket, from a deal event ({"deal": {...}}) or a history entry."""
    deal: object = body.get("deal", body)
    if not isinstance(deal, dict):
        return None
    value: object = cast(Json, deal).get("ticket")
    return value if isinstance(value, int) else None


def order_ticket(body: Json) -> int | None:
    value = body.get("ticket")
    return value if isinstance(value, int) else None


def ticks_refused(status: int, body: Any) -> PipsocketError:
    """The stream refused the tick set, such as 422 too_many_symbols: raised as it is."""
    return refusal_from_body(status, body)
