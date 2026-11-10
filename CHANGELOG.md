# Changelog

Each release names the API document snapshot it was built from (`openapi.json`, with the
fingerprint `python scripts/spec_diff.py --fingerprint openapi.json` prints).

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
