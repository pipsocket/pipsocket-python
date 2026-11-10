"""The stream helper's state machine, walked without sockets: one test per transition.

| Row | What happens | Test |
| --- | --- | --- |
| 1 | First connect | test_first_connect |
| 2 | The handshake is refused | test_a_refused_handshake_stops |
| 3 | The handshake fails for a network reason | test_a_network_failure_backs_off_and_tries_again |
| 4 | The socket closes | test_a_closed_socket_reconnects_and_resyncs |
| 5 | The socket closes again while syncing | test_closing_while_syncing_throws_the_resync_away |
| 6 | lagged | test_lagged_resyncs_without_reconnecting |
| 7 | lagged or engine_disconnected during a resync | test_a_gap_during_a_resync_restarts_it |
| 8 | The host goes away, then comes back | test_the_host_comes_back_with_an_event, ..._probe |
| 8a | The host back, the account stopped | test_the_host_comes_back_with_the_terminal_stopped |
| 9 | The host away and the socket closed | test_snapshot_reads_are_retried_while_the_host_is_away |
| 10 | The caller is slow | test_an_overflow_resyncs |
| 11 | The key was rolled, then the socket closes | test_a_refused_handshake_stops |
| 11a | The key was rolled, the socket stays open | test_a_401_on_any_read_stops |
| 11b | A resync keeps failing | test_a_stuck_resync_reconnects |
| 12 | The account is stopped or removed | test_status_events_pass_through, test_a_404_... |
| 13 | The account's login changes | test_a_new_login_drops_the_checkpoint |
| 14 | close() during backoff or syncing | test_close_stops_at_once |
| 15 | An order pending across rows 4 to 10 | test_watched_requests_are_looked_up_on_resync |
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from pipsocket import (
    AccountUnavailable,
    AuthenticationError,
    NotFound,
    StreamHandshakeError,
    _stream_core as core,
)
from pipsocket.errors import RangeTooLarge, Unavailable

ACCOUNT = {
    "id": "acc_01m6tx3k9f2qw8r7bznd5yvh4c",
    "login": 5001,
    "server": "Demo-Server",
    "status": "connected",
}
POSITIONS = [{"ticket": 1, "symbol": "EURUSD"}]
ORDERS = [{"ticket": 2, "symbol": "EURUSD"}]


def unavailable() -> Unavailable:
    return Unavailable("Away.", status=503, code="engine_unavailable", body=None, raw=b"")


def not_connected() -> AccountUnavailable:
    return AccountUnavailable(
        "Stopped.", status=503, code="account_not_connected", body=None, raw=b""
    )


def rolled() -> AuthenticationError:
    return AuthenticationError("Rolled.", status=401, code="invalid_api_key", body=None, raw=b"")


class Driver:
    """Feeds the machine and keeps every action it returned."""

    def __init__(self, *, ticks: tuple[str, ...] = ("EURUSD",), snapshot: bool = True) -> None:
        self.machine = core.StreamMachine(
            ticks=ticks, read_state=snapshot, queue_limit=5, random=lambda: 1.0
        )
        self.actions: list[core.Action] = []
        self.timers: dict[str, core.StartTimer] = {}

    def feed(self, event: core.Input) -> list[core.Action]:
        actions = self.machine.handle(event)
        self.actions += actions
        for action in actions:
            if isinstance(action, core.StartTimer):
                self.timers[action.name] = action
        return actions

    @property
    def state(self) -> core.State:
        return self.machine.state

    def fire(self, name: str) -> list[core.Action]:
        timer = self.timers[name]
        return self.feed(core.Timer(name, timer.generation))

    def message(self, **body: Any) -> list[core.Action]:
        return self.feed(core.Message(body))

    def subscribe(self) -> list[core.Action]:
        self.feed(core.Start())
        self.feed(core.Opened())
        return self.message(type="subscribed", account="acc_01m6tx3k9f2qw8r7bznd5yvh4c")

    def ticks_call(self, actions: list[core.Action]) -> str:
        (send,) = [a for a in actions if isinstance(a, core.Send) and a.text != "ping"]
        call = json.loads(send.text)
        assert (call["method"], call["path"]) == ("PUT", "/ticks")
        return call["id"]

    def answer_ticks(self, actions: list[core.Action]) -> list[core.Action]:
        call_id = self.ticks_call(actions)
        return self.message(type="response", id=call_id, status=200, body={"symbols": ["EURUSD"]})

    def pending_read(self, actions: list[core.Action]) -> core.Read:
        (read,) = [a for a in actions if isinstance(a, core.Read)]
        return read

    def answer(
        self, actions: list[core.Action], value: Any = None, error: Any = None
    ) -> list[core.Action]:
        return self.feed(core.Answered(self.pending_read(actions), value=value, error=error))

    def snapshot(
        self, actions: list[core.Action], account: dict[str, Any] = ACCOUNT, **reads: Any
    ) -> list[core.Action]:
        """Answer the account, positions and orders reads, then any history and lookups."""
        actions = self.answer(actions, account)
        actions = self.answer(actions, reads.get("positions", POSITIONS))
        actions = self.answer(actions, reads.get("orders", ORDERS))
        while any(isinstance(a, core.Read) for a in actions):
            read = self.pending_read(actions)
            actions = self.answer(actions, reads.get(read.kind, []))
        return actions

    def connect_live(self, account: dict[str, Any] = ACCOUNT) -> list[core.Action]:
        actions = self.answer_ticks(self.subscribe())
        return self.snapshot(actions, account)

    def emitted(self, actions: list[core.Action]) -> list[dict[str, Any]]:
        return [a.event for a in actions if isinstance(a, core.Emit)]

    def resync(self, actions: list[core.Action]) -> dict[str, Any]:
        (event,) = [e for e in self.emitted(actions) if e["type"] == "resync"]
        return event


def kinds(actions: list[core.Action]) -> list[str]:
    return [type(a).__name__ for a in actions]


# Row 1


def test_first_connect() -> None:
    driver = Driver()
    assert kinds(driver.feed(core.Start())) == ["Connect"]
    driver.feed(core.Opened())
    actions = driver.message(type="subscribed", account="acc_01m6tx3k9f2qw8r7bznd5yvh4c")
    assert driver.state is core.State.SYNCING
    assert not any(isinstance(a, core.Read) for a in actions), "the ticks are set first"

    actions = driver.answer_ticks(actions)
    assert driver.pending_read(actions).kind == "account"
    actions = driver.snapshot(actions)

    assert driver.state is core.State.LIVE
    event = driver.resync(actions)
    assert event == {
        "type": "resync",
        "reason": "connected",
        "account": ACCOUNT,
        "positions": POSITIONS,
        "orders": ORDERS,
        "history": None,
        "history_reason": "no_checkpoint",
    }


def test_no_ticks_means_no_ticks_call() -> None:
    driver = Driver(ticks=())
    actions = driver.subscribe()
    assert driver.pending_read(actions).kind == "account"


def test_live_events_pass_through_unchanged_and_one_for_one() -> None:
    driver = Driver()
    driver.connect_live()
    ticks = [
        {"type": "tick", "symbol": "EURUSD", "bid": 1.0 + i / 10000, "time_msc": i}
        for i in range(3)
    ]
    for tick in ticks:
        assert driver.emitted(driver.message(**tick)) == [tick]


def test_snapshot_false_reads_nothing() -> None:
    driver = Driver(snapshot=False)
    actions = driver.answer_ticks(driver.subscribe())
    assert not any(isinstance(a, core.Read) for a in actions)
    event = driver.resync(actions)
    assert (event["account"], event["positions"], event["history"]) == (None, None, None)


# Rows 2, 3, 11, 12


@pytest.mark.parametrize("status", [401, 404, 426, 403])
def test_a_refused_handshake_stops(status: int) -> None:
    driver = Driver()
    driver.feed(core.Start())
    error = StreamHandshakeError("Refused.", status=status)

    actions = driver.feed(core.Refused(status, error))

    assert driver.state is core.State.FAILED
    assert actions == [core.Fail(error)]
    assert driver.feed(core.Start()) == []


@pytest.mark.parametrize(
    "event",
    [core.Unreachable(OSError("down")), core.Refused(503, StreamHandshakeError("x", status=503))],
)
def test_a_network_failure_backs_off_and_tries_again(event: core.Input) -> None:
    driver = Driver()
    driver.feed(core.Start())

    driver.feed(event)
    assert driver.state is core.State.BACKOFF
    assert driver.timers["backoff"].seconds == 0.5

    assert kinds(driver.fire("backoff")) == ["Connect"]
    driver.feed(core.Unreachable(OSError("still down")))
    assert driver.timers["backoff"].seconds == 1.0


def test_backoff_is_capped_at_thirty_seconds() -> None:
    driver = Driver()
    driver.feed(core.Start())
    for _ in range(12):
        driver.feed(core.Unreachable(OSError("down")))
        driver.fire("backoff")
    assert driver.timers["backoff"].seconds == 30.0


# Row 4


def test_a_closed_socket_reconnects_and_resyncs() -> None:
    driver = Driver()
    driver.connect_live()
    driver.message(type="account", server_time=1_794_067_800)

    driver.feed(core.Closed())
    assert driver.state is core.State.BACKOFF
    driver.fire("backoff")
    driver.feed(core.Opened())
    actions = driver.message(type="subscribed", account="acc_01m6tx3k9f2qw8r7bznd5yvh4c")
    assert driver.ticks_call(actions), "the tick set is sent again on the new socket"
    actions = driver.answer_ticks(actions)
    account = {**ACCOUNT, "server_time": 1_794_067_900}
    actions = driver.answer(actions, account)
    actions = driver.answer(actions, POSITIONS)
    actions = driver.answer(actions, ORDERS)

    deals_read = driver.pending_read(actions)
    assert (deals_read.kind, deals_read.from_, deals_read.to) == (
        "deals",
        1_794_067_800 - 300,
        1_794_067_900 + 86_400,
    )
    actions = driver.answer(actions, [{"ticket": 9}])
    assert driver.pending_read(actions).kind == "history_orders"
    actions = driver.answer(actions, [{"ticket": 8}])

    event = driver.resync(actions)
    assert event["reason"] == "reconnected"
    assert event["history"] == {
        "deals": [{"ticket": 9}],
        "orders": [{"ticket": 8}],
        "from": 1_794_067_500,
        "to": 1_794_154_300,
    }
    assert event["history_reason"] is None


# Row 5


def test_closing_while_syncing_throws_the_resync_away() -> None:
    driver = Driver()
    actions = driver.answer_ticks(driver.subscribe())
    stale = driver.pending_read(actions)

    driver.feed(core.Closed())
    assert driver.state is core.State.BACKOFF
    assert driver.feed(core.Answered(stale, value=ACCOUNT)) == []
    assert driver.machine.checkpoint.server_time is None


# Row 6


def test_lagged_resyncs_without_reconnecting() -> None:
    driver = Driver()
    driver.connect_live()

    actions = driver.message(type="lagged", missed=12)

    assert driver.emitted(actions) == [{"type": "lagged", "missed": 12}]
    assert driver.state is core.State.SYNCING
    assert driver.pending_read(actions).kind == "account", "the socket stayed open: no ticks call"
    assert not any(isinstance(a, (core.Connect, core.Disconnect)) for a in actions)
    assert driver.resync(driver.snapshot(actions))["reason"] == "lagged"


# Row 7


@pytest.mark.parametrize("gap", ["lagged", "engine_disconnected"])
def test_a_gap_during_a_resync_restarts_it(gap: str) -> None:
    driver = Driver()
    driver.connect_live()
    actions = driver.message(type="lagged", missed=1)
    first = driver.pending_read(actions)

    actions = driver.message(type=gap)
    assert driver.feed(core.Answered(first, value=ACCOUNT)) == [], "the old read is thrown away"
    if gap == "engine_disconnected":
        assert driver.state is core.State.HOST_AWAY
        actions = driver.message(type="deal", deal={"ticket": 4})
    assert driver.state is core.State.SYNCING
    actions = driver.snapshot(actions)
    assert driver.state is core.State.LIVE


# Events during a resync


def test_events_during_a_resync_are_held_thinned_or_dropped() -> None:
    driver = Driver()
    actions = driver.answer_ticks(driver.subscribe())
    held = [
        {"type": "deal", "deal": {"ticket": 7}},
        {"type": "request", "request": "req_1", "state": "completed"},
        {"type": "transaction", "transaction": "deal_add"},
        {"type": "status", "status": "connected", "detail": None},
    ]
    for body in held:
        assert driver.emitted(driver.message(**body)) == []
    driver.message(type="account", server_time=5)
    for i, bid in enumerate([1.1, 1.2, 1.3]):
        driver.message(type="tick", symbol="EURUSD", bid=bid, time_msc=100 + i)
    driver.message(type="tick", symbol="XAUUSD", bid=2400.0, time_msc=50)

    emitted = driver.emitted(driver.snapshot(actions))

    assert emitted[0]["type"] == "resync"
    assert emitted[1:5] == held
    assert emitted[5:] == [
        {"type": "tick", "symbol": "EURUSD", "bid": 1.3, "time_msc": 102},
        {"type": "tick", "symbol": "XAUUSD", "bid": 2400.0, "time_msc": 50},
    ]
    assert driver.machine.checkpoint.server_time is None, "account events in a resync are dropped"


def test_held_events_that_overflow_restart_the_resync() -> None:
    driver = Driver()
    actions = driver.answer_ticks(driver.subscribe())
    for ticket in range(6):
        actions = driver.message(type="deal", deal={"ticket": ticket})
    assert driver.state is core.State.SYNCING
    event = driver.resync(driver.snapshot(actions))
    assert event["reason"] == "connected", "a first connect keeps its reason"
    assert driver.machine.held == []


# Rows 8 and 8a


def test_the_host_comes_back_with_an_event() -> None:
    driver = Driver()
    driver.connect_live()

    actions = driver.message(type="engine_disconnected")
    assert driver.emitted(actions) == [{"type": "engine_disconnected"}]
    assert driver.state is core.State.HOST_AWAY
    assert driver.timers["probe"].seconds == 2.0

    actions = driver.message(type="deal", deal={"ticket": 3})
    assert driver.state is core.State.SYNCING
    emitted = driver.emitted(driver.snapshot(actions))
    assert emitted[0]["reason"] == "host_back"
    assert emitted[1] == {"type": "deal", "deal": {"ticket": 3}}


def test_the_host_comes_back_with_a_probe() -> None:
    driver = Driver()
    driver.connect_live()
    driver.message(type="engine_disconnected")

    actions = driver.fire("probe")
    assert driver.pending_read(actions).kind == "account"
    driver.answer(actions, error=unavailable())
    assert driver.timers["probe"].seconds == 4.0
    for _ in range(5):
        driver.answer(driver.fire("probe"), error=unavailable())
    assert driver.timers["probe"].seconds == 30.0

    actions = driver.answer(driver.fire("probe"), ACCOUNT)
    assert driver.state is core.State.SYNCING
    assert driver.resync(driver.snapshot(actions))["reason"] == "host_back"


def test_the_host_comes_back_with_the_terminal_stopped() -> None:
    driver = Driver()
    driver.connect_live()
    driver.message(type="account", server_time=100)
    driver.message(type="engine_disconnected")
    actions = driver.answer(driver.fire("probe"), ACCOUNT)

    actions = driver.answer(actions, {**ACCOUNT, "status": "stopped"})
    actions = driver.answer(actions, error=not_connected())
    event = driver.resync(actions)

    assert event["positions"] is None
    assert event["orders"] is None
    assert (event["history"], event["history_reason"]) == (None, "unavailable")
    assert driver.machine.checkpoint.server_time == 100, "the checkpoint stays where it was"

    actions = driver.message(type="status", status="connected", detail=None)
    assert driver.emitted(actions) == [{"type": "status", "status": "connected", "detail": None}]
    assert driver.state is core.State.SYNCING, "the terminal is back: read the snapshot again"


# Row 9 and 11b


def test_snapshot_reads_are_retried_while_the_host_is_away() -> None:
    driver = Driver()
    actions = driver.answer_ticks(driver.subscribe())
    driver.answer(actions, error=unavailable())
    assert driver.state is core.State.SYNCING
    assert driver.timers["resync_retry"].seconds == 0.5

    actions = driver.fire("resync_retry")
    assert driver.pending_read(actions).kind == "account"
    assert driver.resync(driver.snapshot(actions))["reason"] == "connected"


def test_a_stuck_resync_reconnects() -> None:
    driver = Driver()
    actions = driver.answer_ticks(driver.subscribe())
    driver.answer(actions, error=unavailable())
    assert driver.timers["resync_deadline"].seconds == 120

    actions = driver.fire("resync_deadline")

    assert kinds(actions)[0] == "Disconnect"
    assert driver.state is core.State.BACKOFF


# Row 10


def test_an_overflow_resyncs() -> None:
    driver = Driver()
    driver.connect_live()

    actions = driver.feed(core.Overflowed())

    assert driver.state is core.State.SYNCING
    assert driver.resync(driver.snapshot(actions))["reason"] == "overflow"


# Rows 11a and 12


@pytest.mark.parametrize(
    "error",
    [rolled(), NotFound("Gone.", status=404, code="no_such_account", body=None, raw=b"")],
)
def test_a_401_or_404_on_any_read_stops(error: Exception) -> None:
    driver = Driver()
    actions = driver.answer_ticks(driver.subscribe())

    actions = driver.answer(actions, error=error)

    assert driver.state is core.State.FAILED
    assert actions == [core.Disconnect(), core.Fail(error)]  # type: ignore[arg-type]


def test_a_401_on_the_probe_stops() -> None:
    driver = Driver()
    driver.connect_live()
    driver.message(type="engine_disconnected")

    actions = driver.answer(driver.fire("probe"), error=rolled())

    assert driver.state is core.State.FAILED
    assert isinstance(actions[-1], core.Fail)


def test_status_events_pass_through() -> None:
    driver = Driver()
    driver.connect_live()
    body = {"type": "status", "status": "stopped", "detail": "Stopped from the dashboard."}
    assert driver.emitted(driver.message(**body)) == [body]
    assert driver.state is core.State.LIVE


# Row 13 and the history rules


def history_after_reconnect(
    driver: Driver, account: dict[str, Any], **reads: Any
) -> dict[str, Any]:
    driver.feed(core.Closed())
    driver.fire("backoff")
    driver.feed(core.Opened())
    actions = driver.answer_ticks(
        driver.message(type="subscribed", account="acc_01m6tx3k9f2qw8r7bznd5yvh4c")
    )
    return driver.resync(driver.snapshot(actions, account, **reads))


def test_a_new_login_drops_the_checkpoint() -> None:
    driver = Driver()
    driver.connect_live()
    driver.message(type="account", server_time=1000)

    event = history_after_reconnect(driver, {**ACCOUNT, "login": 6002})

    assert (event["history"], event["history_reason"]) == (None, "account_changed")
    assert driver.machine.checkpoint.server_time is None


def test_broker_time_going_back_reads_from_the_lower() -> None:
    driver = Driver()
    driver.connect_live()
    driver.message(type="account", server_time=10_000)

    event = history_after_reconnect(driver, {**ACCOUNT, "server_time": 9_000})

    assert (event["history"]["from"], event["history"]["to"]) == (8_700, 10_000 + 86_400)


def test_a_range_too_large_is_split_down_to_a_minute() -> None:
    driver = Driver()
    driver.connect_live()
    driver.message(type="account", server_time=10_000)
    driver.feed(core.Closed())
    driver.fire("backoff")
    driver.feed(core.Opened())
    actions = driver.answer_ticks(
        driver.message(type="subscribed", account="acc_01m6tx3k9f2qw8r7bznd5yvh4c")
    )
    actions = driver.answer(driver.answer(driver.answer(actions, ACCOUNT), []), [])

    too_large = RangeTooLarge("Too much.", status=422, code="range_too_large", body=None, raw=b"")
    first = driver.pending_read(actions)
    actions = driver.answer(actions, error=too_large)
    half = driver.pending_read(actions)
    assert (half.kind, half.from_) == ("deals", first.from_)
    assert half.to is not None
    assert first.to is not None
    assert half.to < first.to

    while any(isinstance(a, core.Read) for a in actions):
        actions = driver.answer(actions, error=too_large)
    event = driver.resync(actions)
    assert (event["history"], event["history_reason"]) == (None, "range_too_large")


def test_history_is_deduplicated_against_delivered_deals() -> None:
    driver = Driver()
    driver.connect_live()
    driver.message(type="account", server_time=10_000)
    driver.message(type="deal", deal={"ticket": 71})

    event = history_after_reconnect(driver, ACCOUNT, deals=[{"ticket": 71}, {"ticket": 72}])

    assert event["history"]["deals"] == [{"ticket": 72}]


# Row 14


@pytest.mark.parametrize("where", ["backoff", "syncing"])
def test_close_stops_at_once(where: str) -> None:
    driver = Driver()
    actions = driver.answer_ticks(driver.subscribe())
    if where == "backoff":
        driver.feed(core.Closed())
    read = driver.pending_read(actions) if where == "syncing" else None

    driver.feed(core.Close())

    assert driver.state is core.State.CLOSED
    assert (
        driver.feed(
            core.Timer("backoff", driver.timers["backoff"].generation if where == "backoff" else 0)
        )
        == []
    )
    if read is not None:
        assert driver.feed(core.Answered(read, value=ACCOUNT)) == []


# Row 15: wait() and the stream


def test_watched_requests_are_looked_up_on_resync() -> None:
    driver = Driver()
    driver.feed(core.Watch("req_9"))
    actions = driver.answer_ticks(driver.subscribe())
    actions = driver.answer(driver.answer(driver.answer(actions, ACCOUNT), POSITIONS), ORDERS)

    lookup = driver.pending_read(actions)
    assert (lookup.kind, lookup.request_id) == ("request", "req_9")
    settled = {"request": "req_9", "state": "completed"}
    actions = driver.answer(actions, settled)
    assert core.Notify(settled) in actions
    assert driver.resync(actions)


def test_a_request_event_notifies_at_once_even_during_a_resync() -> None:
    driver = Driver()
    driver.feed(core.Watch("req_9"))
    driver.answer_ticks(driver.subscribe())
    body = {"type": "request", "request": "req_9", "state": "completed"}

    actions = driver.message(**body)

    assert actions == [core.Notify(body)]


# Ticks and keep-alive


def test_ticks_set_while_a_ticks_call_is_unanswered_are_sent_after_it() -> None:
    driver = Driver()
    driver.connect_live()
    first = driver.feed(core.SetTicks(("XAUUSD",)))
    call = driver.ticks_call(first)
    assert driver.feed(core.SetTicks(("GBPUSD",))) == []

    actions = driver.message(type="response", id=call, status=200, body={"symbols": ["XAUUSD"]})

    assert json.loads(driver_send(actions))["body"] == {"symbols": ["GBPUSD"]}


def driver_send(actions: list[core.Action]) -> str:
    (send,) = [a for a in actions if isinstance(a, core.Send)]
    return send.text


def test_too_many_symbols_is_raised_as_it_is() -> None:
    driver = Driver()
    driver.connect_live()
    call = driver.ticks_call(driver.feed(core.SetTicks(tuple(f"S{i}" for i in range(101)))))

    actions = driver.message(
        type="response",
        id=call,
        status=422,
        body={"error": "too_many_symbols", "message": "Over 100."},
    )

    assert driver.state is core.State.FAILED
    (fail,) = [a for a in actions if isinstance(a, core.Fail)]
    assert (fail.error.status, fail.error.code) == (422, "too_many_symbols")  # type: ignore[attr-defined]


def test_no_pong_reconnects() -> None:
    driver = Driver()
    driver.connect_live()

    assert core.Send("ping") in driver.fire("ping")
    actions = driver.fire("pong")

    assert kinds(actions)[0] == "Disconnect"
    assert driver.state is core.State.BACKOFF


def test_a_pong_keeps_the_socket() -> None:
    driver = Driver()
    driver.connect_live()
    driver.fire("ping")
    pong = driver.timers["pong"]

    driver.feed(core.Text("pong"))

    assert driver.feed(core.Timer("pong", pong.generation)) == []
    assert driver.state is core.State.LIVE


def test_never_subscribed_reconnects() -> None:
    driver = Driver()
    driver.feed(core.Start())
    driver.feed(core.Opened())

    driver.fire("subscribed")

    assert driver.state is core.State.BACKOFF


def test_gaps_during_a_resync_dont_extend_its_deadline() -> None:
    driver = Driver()
    driver.answer_ticks(driver.subscribe())
    deadline = driver.timers["resync_deadline"]

    driver.message(type="lagged", missed=1)
    driver.feed(core.Overflowed())

    assert driver.timers["resync_deadline"] == deadline
    driver.feed(core.Timer("resync_deadline", deadline.generation))
    assert driver.state is core.State.BACKOFF


# Overflow, flushing, identity, ticks and backoff edges


def test_an_overflow_takes_back_what_the_caller_never_got() -> None:
    driver = Driver()
    driver.connect_live()
    first = {"type": "account", "server_time": 1000}
    deal = {"type": "deal", "deal": {"ticket": 71}}
    second = {"type": "account", "server_time": 2000}
    for body in (first, deal, second):
        driver.message(**body)
    sent = driver.emitted(driver.actions)
    first, deal, second = sent[-3], sent[-2], sent[-1]

    driver.feed(core.Overflowed((deal, second)))

    assert driver.machine.checkpoint.server_time == 1000
    assert 71 not in driver.machine.recent_deals


def test_events_held_behind_one_that_starts_another_resync_are_kept() -> None:
    driver = Driver()
    actions = driver.answer_ticks(driver.subscribe())
    status = {"type": "status", "status": "connected", "detail": None}
    transaction = {"type": "transaction", "transaction": "deal_add"}
    driver.message(**status)
    driver.message(**transaction)

    actions = driver.answer(actions, ACCOUNT)
    actions = driver.answer(actions, error=not_connected())  # the snapshot has no terminal
    assert driver.resync(actions)["positions"] is None
    assert driver.emitted(actions)[1:] == [status], "the status starts another resync"
    assert driver.state is core.State.SYNCING

    emitted = driver.emitted(driver.snapshot(actions))
    assert emitted[1:] == [transaction], "the event behind it waited for the new resync"


def test_a_new_login_is_committed_only_when_its_resync_finishes() -> None:
    driver = Driver()
    driver.connect_live()
    driver.message(type="account", server_time=1000)
    driver.message(type="deal", deal={"ticket": 71})
    driver.feed(core.Closed())
    driver.fire("backoff")
    driver.feed(core.Opened())
    actions = driver.answer_ticks(
        driver.message(type="subscribed", account="acc_01m6tx3k9f2qw8r7bznd5yvh4c")
    )
    other = {**ACCOUNT, "login": 6002}
    driver.feed(core.Watch("req_9"))  # a lookup keeps the resync from finishing at once
    actions = driver.answer(driver.answer(driver.answer(actions, other), POSITIONS), ORDERS)
    assert driver.pending_read(actions).kind == "request"
    driver.feed(core.Closed())  # interrupted before it finished
    driver.feed(core.Unwatch("req_9"))

    event = history_after_reconnect_from_backoff(driver, other)
    assert event["history_reason"] == "account_changed"

    driver.message(type="account", server_time=5000)
    event = history_after_reconnect(driver, other, deals=[{"ticket": 71}])
    assert event["history"]["deals"] == [{"ticket": 71}], "the old login's tickets are forgotten"


def history_after_reconnect_from_backoff(driver: Driver, account: dict[str, Any]) -> dict[str, Any]:
    driver.fire("backoff")
    driver.feed(core.Opened())
    actions = driver.answer_ticks(
        driver.message(type="subscribed", account="acc_01m6tx3k9f2qw8r7bznd5yvh4c")
    )
    return driver.resync(driver.snapshot(actions, account))


def test_a_gap_during_the_first_resync_doesnt_send_a_second_ticks_call() -> None:
    driver = Driver()
    actions = driver.subscribe()
    call = driver.ticks_call(actions)

    actions = driver.message(type="lagged", missed=1)

    assert not any(isinstance(a, core.Send) for a in actions)
    actions = driver.message(type="response", id=call, status=200, body={"symbols": ["EURUSD"]})
    assert driver.pending_read(actions).kind == "account"


def test_a_resync_waits_for_the_latest_tick_set() -> None:
    driver = Driver()
    actions = driver.subscribe()
    first = driver.ticks_call(actions)
    driver.feed(core.SetTicks(("XAUUSD",)))

    actions = driver.message(type="response", id=first, status=200, body={"symbols": ["EURUSD"]})
    second = json.loads(driver_send(actions))
    assert second["body"] == {"symbols": ["XAUUSD"]}
    assert not any(isinstance(a, core.Read) for a in actions), "not before the new set is set"

    actions = driver.message(
        type="response", id=second["id"], status=200, body={"symbols": ["XAUUSD"]}
    )
    assert driver.pending_read(actions).kind == "account"


def test_backoff_never_overflows() -> None:
    driver = Driver()
    driver.feed(core.Start())
    driver.machine.failures = 5000

    driver.feed(core.Unreachable(OSError("down")))

    assert driver.timers["backoff"].seconds == 30.0


def test_a_fatal_failure_reported_from_outside_stops_the_stream() -> None:
    driver = Driver()
    driver.connect_live()

    actions = driver.feed(core.Fatal(rolled()))

    assert driver.state is core.State.FAILED
    assert isinstance(actions[-1], core.Fail)


def test_a_missing_request_reported_from_outside_doesnt_stop_it() -> None:
    driver = Driver()
    driver.connect_live()
    gone = NotFound("No such request.", status=404, code="no_such_request", body=None, raw=b"")

    assert driver.feed(core.Fatal(gone)) == []
    assert driver.state is core.State.LIVE
