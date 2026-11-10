# Changelog

Each release names the API document snapshot it was built from (`openapi.json`, with the
fingerprint `python scripts/spec_diff.py --fingerprint openapi.json` prints).

## Unreleased

### Added

- The trade calls: `orders.place`, `orders.modify`, `orders.cancel`, `positions.close`,
  `positions.close_by` and `positions.set_levels`. Each returns the recorded request as a
  `TradeRequest` whatever its state, sends one idempotency key on every attempt, and raises
  `OutcomeUnknown` (with the key) when the trade may have reached the host and no answer came back.
- `idempotency_key=` on the trade calls, checked before sending.
- `orders.check`, `kill_switch.engage` and `kill_switch.lift`.
- `requests.wait(id, timeout=30)`: looks a request up until it is final or the time is up.

## 0.1.0a1

The first alpha: the read calls.

### Added

- `Pipsocket` (sync) and `AsyncPipsocket` (async), with `api_key` (default `PIPSOCKET_KEY`),
  `base_url`, `timeout` (40 s per attempt), `max_retries` (2) and `retry_deadline` (60 s).
- The read calls on `ps.account(id)`: `get`, `positions.list`, `orders.list`, `requests.get`,
  `history.deals`, `history.orders`, `symbols.list`, `symbols.get`, `symbols.tick` and
  `kill_switch.get`.
- `TradeRequest`, the type a recorded request comes back as.
- An exception for every error code the API answers with, and retries for the answers worth
  repeating.
- Types for every answer, generated from the API's OpenAPI document (`pipsocket.types`).

### Spec

- Built from the API document of 9 Nov 2026, fingerprint `ed112fbcea9b`.
