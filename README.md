# pipsocket

Python client for the Pipsocket API: trade MetaTrader 5 accounts hosted by Pipsocket from Linux,
macOS or Windows.

> **Alpha.** The calls and their behaviour may still change before 0.1.0.

## Why

The terminal runs at Pipsocket, so your code runs anywhere Python does: one `pip install`, no
terminal on your machine, and typed answers.

| | The MetaTrader5 package | pipsocket |
| --- | --- | --- |
| Runs on | Windows only (its wheels on PyPI are all `win_amd64`, checked 9 Nov 2026) | Linux, macOS, Windows |
| Needs | An MT5 terminal running on the same machine | A Pipsocket account; the terminal is hosted |

## Install

```bash
pip install --pre pipsocket
```

Python 3.10 or later. Its dependencies are [httpx](https://www.python-httpx.org) and
[websockets](https://websockets.readthedocs.io).

## Quickstart

Make your workspace's API key on the dashboard's **API key** page, where your accounts' ids are
listed too, and put the key in `PIPSOCKET_KEY`. Use a demo account while you try things.

```python
from pipsocket import Pipsocket

ps = Pipsocket()  # reads PIPSOCKET_KEY
acct = ps.account("acc_01m6tx3k9f2qw8r7bznd5yvh4c")

print(acct.get()["balance"])
eurusd = acct.symbols.get("EURUSD")  # filling_modes, volume_min, volume_step, ...

order = acct.orders.place(symbol="EURUSD", type="buy", volume=0.01, type_filling="ioc")
if order.outstanding:  # pending or unknown: it may still execute
    order = acct.requests.wait(order.id)

match order.state:
    case "completed":
        print("filled:", order.result["deal"], "at", order.result["price"])
        # On a hedging account the position's ticket is the order's.
        position = order.result["position"] or order.result["order"]
        closed = acct.positions.close(position, volume=0.01, type_filling="ioc")
        print("closed:", closed.state)
    case "rejected":
        print("rejected:", order.retcode_name)
    case _:
        print(order.state, order.message)
```

Answers are plain dicts, typed with `TypedDict`s from the API's OpenAPI document
(`pipsocket.types`), so they go straight into anything that takes dicts:

```python
import datetime

from pipsocket import Pipsocket

acct = Pipsocket().account("acc_01m6tx3k9f2qw8r7bznd5yvh4c")

# History ranges are the broker's server time in seconds, both ends included, not UTC.
day = int(datetime.datetime(2026, 11, 9, tzinfo=datetime.timezone.utc).timestamp())
deals = acct.history.deals(day, day + 86399)
profit = sum(deal["profit"] for deal in deals)
print(len(deals), "deals,", profit)
```

## Order outcomes

Each trade call (`orders.place`, `orders.modify`, `orders.cancel`, `positions.close`,
`positions.close_by`, `positions.set_levels`) is one MT5 trade request, sent once and recorded as
a request (`req_...`). The call returns that request as a `TradeRequest`, **whatever its state**:
a rejection is a result, not an exception. `acct.requests.get(id)` looks one up, and
`acct.requests.wait(id, timeout=30)` looks it up until it is final or the time is up, then
returns it as it stands; it never raises for "still pending" and never sends anything.

A `TradeRequest` has `id`, `state`, `sent`, `result`, `message`, `error`, `settlement`, the
whole body as `raw`, the `idempotency_key` it was sent with and the `http_status`, plus
`accepted`, `final`, `outstanding`, `retcode` and `retcode_name`.

```python
from pipsocket import Pipsocket

acct = Pipsocket().account("acc_01m6tx3k9f2qw8r7bznd5yvh4c")

request = acct.requests.get("req_5f0c2a9e41d7b3a8c6e2f190")
match request.state:
    case "completed" | "placed" | "partial":
        print("filled or placed:", request.result["order"], request.result["deal"])
    case "rejected":
        print("MT5 rejected it:", request.retcode, request.retcode_name)
    case "pending" | "unknown":
        print("no outcome yet: it may still execute")
    case _:
        print(request.state, request.message)
```

| `state` | Meaning |
| --- | --- |
| `completed`, `placed`, `partial` | MT5 took it. `result` is MT5's answer. Final. |
| `rejected` | MT5 rejected it: `retcode` says why. Final. |
| `pending` | Sent, no answer from the trade server yet. Never sent again. |
| `unknown` | No answer within 130 seconds: it may have executed. Settled by proof only. |
| `not_sent` | The terminal didn't send it: `result["error"]` says why. Final. |
| `not_executed` | Was `unknown`, settled as never executed (`settlement` says by whom). Final. |
| `executed` | Was `unknown`, settled as executed by the account's owner. Final. |

`raise_for_state()` raises `TradeNotAccepted` for `rejected`, `not_sent` and `not_executed`, for
those who prefer exceptions. `orders.check(...)` is MT5's dry run of an order and sends nothing to
the broker. A state this SDK doesn't know yet comes through as its string,
neither accepted nor final. See [Errors and retcodes](https://docs.pipsocket.com/errors).

## Streaming

`acct.stream(...)` keeps the account's WebSocket open: fills, deals, prices and the account as they
happen, reconnected for you.

```python
from pipsocket import Pipsocket, TradeRequest

acct = Pipsocket().account("acc_01m6tx3k9f2qw8r7bznd5yvh4c")

with acct.stream(ticks=["EURUSD"]) as events:
    for event in events:
        match event["type"]:
            case "resync":  # after every (re)connection or gap: a new baseline
                print("resync:", event["reason"], len(event["positions"] or []), "positions")
            case "tick":
                print(event["symbol"], event["bid"], event["ask"])
            case "request":
                print("settled:", TradeRequest.from_event(event).state)
                break
```

Events are the API's messages as they are (see [the WebSocket stream](https://docs.pipsocket.com/websocket)),
plus one the SDK adds, `resync`. The helper sends the tick set again after every reconnect,
answers the keep-alive, and resyncs when the socket was closed, the stream said `lagged`, the
account's host came back, or your code fell behind (`reason`: `connected`, `reconnected`,
`lagged`, `host_back`, `overflow`).

What a `resync` gives:

- **A new baseline**: `account`, `positions` and `orders` read over REST after the stream was
  live. Replace your own state with it. `positions` and `orders` are `None` when the terminal
  isn't running, which is not the same as empty.
- **History since the last checkpoint**, best effort: `history` has the `deals` and `orders` from
  the broker's server time `from` to `to`, or is `None` with `history_reason` saying why
  (`no_checkpoint`, `account_changed`, `range_too_large` or `unavailable`).
- Every deal, request, transaction and status event from then on, delivered after it.

What it doesn't give: one moment in time (the reads happen one after another), an order between
events and the snapshot (events are notices, not deltas: never add them to the snapshot), or
ticks and transaction events from a gap, which are gone. Deduplicate deals and orders by ticket
and requests by id, and read again when exact state matters. `snapshot=False` sends `resync`
without reading anything. `AsyncPipsocket` has the same helper, with `async with` and
`async for`.

While a stream is open on an account handle, `acct.requests.wait(id)` ends as soon as the stream
sees the request settle.

## Errors, retries and idempotency keys

An exception means there is no recorded request in hand. Every exception derives from
`PipsocketError`. A refusal from the API is an `APIError` with `status`, `code` (the API's
`error`), `message`, `body` and `raw`, and a subclass for each kind: `AuthenticationError`,
`NotFound`, `InvalidRequest`, `SymbolBusy`, `KillSwitchEngaged`, `BillingHold`,
`AccountUnavailable`, `EngineBusy`, `Unavailable`, `TerminalTimeout`, `TerminalError`, `NoPrice`
and the rest. A network failure is an `APIConnectionError` (or `APITimeoutError`). None of
these, raised from a trade call, means anything was sent, with one exception:

**`OutcomeUnknown`** means the trade may have reached the account's host and no answer came
back. It may have executed. It carries the `idempotency_key` the call sent and the last error as
`cause`. Call again with the same key: you get the recorded request if there is one, and the
trade is sent if it never arrived. Or read positions and history first.

Every trade call sends an `Idempotency-Key`: a new one per call, the same one on every retry
inside the call. Calls that fail in a way worth repeating (the host busy or away, a dropped
connection, an answer that isn't the API's JSON, `symbol_busy` with `Retry-After`) are tried
again, twice by default, waiting 0.5 s and then 1 s (or what `Retry-After` says), and no new
attempt starts 60 seconds after the first: a market order sent minutes late is not what you
asked for. Refusals that won't change, such as a bad key or `kill_switch_engaged`, are raised at
once.

To survive a crash or a cancellation, make the key yourself and save it before sending. After a
restart, the same call with the same key returns the recorded request instead of trading again.
Make a new key for every new trade, and never build one from the order's fields (such as
"buy-eurusd"): the key is the whole match, so a second order with the same key returns the first
one and sends nothing.

A key protects a trade only while Pipsocket keeps its request: until 7 days after its outcome is
known. After that the same key sends a new trade. So resolve a journal's open entries promptly,
and never replay an old one blindly: check positions and history first.

```python
import json
import pathlib

import pipsocket
from pipsocket import OutcomeUnknown, Pipsocket

acct = Pipsocket().account("acc_01m6tx3k9f2qw8r7bznd5yvh4c")
journal = pathlib.Path("orders.jsonl")


def note(entry: dict[str, object]) -> None:
    with journal.open("a") as file:
        file.write(json.dumps(entry) + "\n")


key = pipsocket.new_key()
note({"key": key, "symbol": "EURUSD", "volume": 0.01})  # saved before it is sent

try:
    order = acct.orders.place(
        symbol="EURUSD", type="buy", volume=0.01, type_filling="ioc", idempotency_key=key
    )
except OutcomeUnknown as error:
    # The entry stays open: after a restart, resolve it with the same key.
    print("may have executed; call again with", error.idempotency_key)
else:
    if order.final:
        note({"key": key, "request": order.id, "state": order.state})  # resolved
    else:
        print("no outcome yet: wait for", order.id, "before closing the entry")
    print(order.id, order.state)
```

Timeouts and retries can be set on the client, or per call:

```python
from pipsocket import Pipsocket

ps = Pipsocket(timeout=20, max_retries=4, retry_deadline=30)  # timeout is per attempt
acct = ps.account("acc_01m6tx3k9f2qw8r7bznd5yvh4c")
print(acct.kill_switch.get(max_retries=0)["engaged"])
```

The kill switch closes every position and cancels every pending order, and refuses new exposure
until it is lifted. Engaging it again retries whatever is left:

```python
from pipsocket import AccountUnavailable, Pipsocket

acct = Pipsocket().account("acc_01m6tx3k9f2qw8r7bznd5yvh4c")
try:
    outcome = acct.kill_switch.engage(deviation=20)
    print("flat:", outcome["flat"])
except AccountUnavailable as error:
    print("engaged, but the terminal didn't answer:", error.kill_switch)
```

## Sync and async

`Pipsocket` is synchronous; `AsyncPipsocket` has the same calls, awaited.

```python
import asyncio

from pipsocket import AsyncPipsocket


async def main() -> None:
    async with AsyncPipsocket() as ps:
        acct = ps.account("acc_01m6tx3k9f2qw8r7bznd5yvh4c")
        account, positions = await asyncio.gather(acct.get(), acct.positions.list())
        print(account["equity"], len(positions))


asyncio.run(main())
```

Keep the key on your server: the API doesn't answer browsers, and the key is never logged or put
in an error message by this SDK.

## Links

- Docs: [Quickstart](https://docs.pipsocket.com/quickstart),
  [API reference](https://docs.pipsocket.com/api/place-an-order),
  [WebSocket stream](https://docs.pipsocket.com/websocket),
  [Errors and retcodes](https://docs.pipsocket.com/errors)
- [MCP server](https://docs.pipsocket.com/mcp) for AI apps
- [Changelog](CHANGELOG.md)

## Versioning and support

Versions follow semver and stay below 1.0 for now: a minor release may change the API of this
SDK, and the changelog says how. Report bugs in this SDK on
[GitHub issues](https://github.com/pipsocket/pipsocket-python/issues). Questions about your account
or billing go to [support@pipsocket.com](mailto:support@pipsocket.com).

## Licence

MIT. See [LICENSE](LICENSE).

MetaTrader is a trademark of MetaQuotes Ltd. Pipsocket is not affiliated with MetaQuotes.
