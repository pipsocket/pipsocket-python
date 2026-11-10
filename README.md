# pipsocket

Python client for the Pipsocket API: trade MetaTrader 5 accounts hosted by Pipsocket from Linux,
macOS or Windows.

> **Alpha.** This release has the read calls: the account, positions, pending orders, history,
> symbols and prices, request lookups and the kill switch's state. Trade calls and the stream
> helper follow in the next alphas.

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

Python 3.10 or later. Its dependency is [httpx](https://www.python-httpx.org).

## Quickstart

Make your workspace's API key on the dashboard's **API key** page, where your accounts' ids are
listed too, and put the key in `PIPSOCKET_KEY`.

```python
from pipsocket import Pipsocket

ps = Pipsocket()  # reads PIPSOCKET_KEY
acct = ps.account("acc_01m6tx3k9f2qw8r7bznd5yvh4c")

account = acct.get()
print(account["status"], account["balance"], account["currency"])

for position in acct.positions.list():
    print(position["ticket"], position["symbol"], position["type"], position["volume"])

eurusd = acct.symbols.get("EURUSD")
print(eurusd["filling_modes"], eurusd["volume_min"])
print(acct.symbols.tick("EURUSD")["bid"])
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

A trade is recorded as a request (`req_...`), and `acct.requests.get(id)` looks it up, whatever
its state. It comes back as a `TradeRequest`: `id`, `state`, `sent`, `result`, `message`,
`error`, `settlement` and the whole body as `raw`, plus `accepted`, `final`, `outstanding`,
`retcode` and `retcode_name`.

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
those who prefer exceptions. A state this SDK doesn't know yet comes through as its string,
neither accepted nor final. See [Errors and retcodes](https://docs.pipsocket.com/errors).

## Errors and retries

Every exception derives from `PipsocketError`. A refusal from the API is an `APIError` with
`status`, `code` (the API's `error`), `message`, `body` and `raw`, and a subclass for each kind:
`AuthenticationError`, `NotFound`, `InvalidRequest`, `RangeTooLarge`, `AccountUnavailable`,
`EngineBusy`, `Unavailable`, `TerminalTimeout`, `TerminalError`, `NoPrice` and the rest. A
network failure is an `APIConnectionError` (or `APITimeoutError`).

```python
from pipsocket import AccountUnavailable, NotFound, Pipsocket

acct = Pipsocket().account("acc_01m6tx3k9f2qw8r7bznd5yvh4c")

try:
    positions = acct.positions.list()
except AccountUnavailable as error:
    print("the terminal isn't running:", error.code)
except NotFound:
    print("no such account for this key")
else:
    print(len(positions), "open positions")
```

Calls that fail in a way worth repeating (the account's host busy or away, a timeout, a dropped
connection, an answer that isn't the API's JSON) are tried again, twice by default, waiting 0.5 s
and then 1 s (or what `Retry-After` says), and no new attempt starts 60 seconds after the first.
Refusals that won't change, such as a bad key or a missing symbol, are raised at once.

```python
from pipsocket import Pipsocket

ps = Pipsocket(timeout=20, max_retries=4, retry_deadline=30)  # timeout is per attempt
acct = ps.account("acc_01m6tx3k9f2qw8r7bznd5yvh4c")
print(acct.kill_switch.get(max_retries=0)["engaged"])  # or per call
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
