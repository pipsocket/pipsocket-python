# Changelog

Each release names the API document snapshot it was built from (`openapi.json`, with the
fingerprint `python scripts/spec_diff.py --fingerprint openapi.json` prints).

## 0.1.0 (10 Nov 2026)

The first release.

### Added

- `Pipsocket` (sync) and `AsyncPipsocket` (async), with `api_key` (default `PIPSOCKET_KEY`),
  `base_url`, `stream_url`, `timeout` (40 s per attempt), `max_retries` (2) and
  `retry_deadline` (60 s).
- The read calls on `ps.account(id)`: `get`, `positions.list`, `orders.list`, `requests.get`,
  `history.deals`, `history.orders`, `symbols.list`, `symbols.get`, `symbols.tick` and
  `kill_switch.get`.
- The trade calls: `orders.place`, `orders.modify`, `orders.cancel`, `positions.close`,
  `positions.close_by` and `positions.set_levels`. Each returns the recorded request as a
  `TradeRequest` whatever its state, sends one idempotency key on every attempt, and raises
  `OutcomeUnknown` (with the key) when the trade may have reached the host and no answer came back.
- `idempotency_key=` on the trade calls, checked before sending, and `pipsocket.new_key()`.
- `orders.check`, `kill_switch.engage` and `kill_switch.lift`.
- `requests.wait(id, timeout=30)`: looks a request up until it is final or the time is up.
- `acct.stream(ticks=[...])`, sync and async: the account's events, reconnected, with the tick set
  sent again, keep-alive, and a `resync` event after every reconnection or gap carrying a new
  snapshot and the history since the last checkpoint. A stream open on an account handle ends
  that handle's `requests.wait()` as soon as the request settles. After `close()`, iteration
  ends at once and events still queued are not delivered.
- `TradeRequest`, the type a recorded request comes back as, from a trade call, a lookup or a
  stream event (`TradeRequest.from_event`).
- An exception for every error code the API answers with, and retries for the answers worth
  repeating.
- Types for every answer, generated from the API's OpenAPI document (`pipsocket.types`).
- Dependencies: `httpx`, and `websockets` 16.1 or later (which drops credentials on a redirect
  to another origin).

### Spec

- Built from the API document of 9 Nov 2026, fingerprint `ed112fbcea9b`.
