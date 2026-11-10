# Contributing

Bug reports and pull requests for this SDK are welcome. Questions about your account, billing or
a trade go to [support@pipsocket.com](mailto:support@pipsocket.com).

## Setup

You need [uv](https://docs.astral.sh/uv/).

```bash
uv sync
uv run pytest
```

Before a pull request, run the checks CI runs:

```bash
uv run ruff check
uv run ruff format --check
uv run pyright
uv run python scripts/generate.py --check
uv run pytest
```

## How the SDK is laid out

- `openapi.json` is a snapshot of the published document, `https://docs.pipsocket.com/openapi.json`.
- `scripts/generate.py` writes `src/pipsocket/_generated/` (types and tables) and
  `tests/fixtures/spec/` (the document's response examples) from it. Never edit those by hand:
  change the snapshot and run the script.
- `src/pipsocket/_core.py` decides what a call returns, raises and retries, without any I/O.
  `client.py` and `aio.py` are thin sync and async shells over it.
- `conformance/` holds language-neutral cases that this SDK and the TypeScript SDK both run. Change
  them only together with the other SDK.

## Tests

Tests never talk to Pipsocket: they use fake transports, a fake clock and local fake servers. Don't
add a real API key, account or anything from one to this repository.

## Changelog

Add a line to `CHANGELOG.md` under the next version in the same pull request (Added, Changed,
Fixed, Spec).
