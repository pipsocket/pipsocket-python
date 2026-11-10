"""Every Python example in README.md runs as written, against a fake API at the HTTP transport."""

from __future__ import annotations

import io
import json
import re
import time
from collections.abc import Iterator
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any

import httpx
import pytest

from pipsocket import stream
from pipsocket._generated.operations import OPERATIONS

from .fake_stream import FakeStreamServer
from .fakes import API_KEY

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "tests" / "fixtures" / "spec"
EXAMPLES = re.findall(r"```python\n(.*?)```", (ROOT / "README.md").read_text(), re.DOTALL)
PREFIX = "/v1/accounts/acc_01m6tx3k9f2qw8r7bznd5yvh4c"


def documented_answer(request: httpx.Request) -> httpx.Response:
    """The first 200 or 202 example the document gives for the operation the request names."""
    path = request.url.path.removeprefix(PREFIX) or "/"
    for operation in OPERATIONS:
        pattern = re.sub(r"\{[a-z_]+\}", "[^/]+", operation.path)
        if operation.method == request.method and re.fullmatch(pattern, path):
            examples: dict[str, Any] = json.loads((FIXTURES / f"{operation.id}.json").read_text())
            status = "200" if "200" in examples else "202"
            body = next(iter(examples[status].values()))
            return httpx.Response(int(status), json=body)
    return httpx.Response(404, json={"error": "no_such_account", "message": "Not found."})


class FakeAPI:
    def __init__(self) -> None:
        self.sent: list[httpx.Request] = []
        # When set, every trade call's answer is lost after the request was written.
        self.lose_trades = False


@pytest.fixture
def fake_stream(monkeypatch: pytest.MonkeyPatch) -> Iterator[FakeStreamServer]:
    """The stream at wss.pipsocket.com, answered by a local fake."""
    server = FakeStreamServer()
    server.after_ticks = [
        {"type": "tick", "symbol": "EURUSD", "bid": 1.0841, "ask": 1.08412, "time_msc": 1},
        {"type": "request", **json.loads((FIXTURES / "request.json").read_text())["200"]["done"]},
    ]
    real_connect = stream.connect

    def connect(url: str, **options: Any) -> Any:
        assert url.startswith("wss://wss.pipsocket.com/v1/accounts/")
        return real_connect(url.replace("wss://wss.pipsocket.com", server.url), **options)

    monkeypatch.setattr(stream, "connect", connect)
    yield server
    server.stop()


@pytest.fixture
def fake_api(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, fake_stream: FakeStreamServer
) -> FakeAPI:
    api = FakeAPI()

    def handle(_: object, request: httpx.Request) -> httpx.Response:
        assert request.url.host == "api.pipsocket.com"
        assert request.headers["Authorization"] == f"Bearer {API_KEY}"
        api.sent.append(request)
        for _ in request.stream:  # type: ignore[union-attr]  (sent, as a real transport would)
            pass
        if api.lose_trades and "Idempotency-Key" in request.headers:
            raise httpx.ReadError("connection reset")
        return documented_answer(request)

    async def handle_async(_: object, request: httpx.Request) -> httpx.Response:
        return handle(_, request)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("PIPSOCKET_KEY", API_KEY)
    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", handle)
    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", handle_async)
    monkeypatch.setattr(time, "sleep", lambda seconds: None)
    return api


def test_the_readme_has_examples() -> None:
    assert len(EXAMPLES) >= 5


@pytest.mark.parametrize("source", EXAMPLES, ids=[f"example-{i}" for i in range(len(EXAMPLES))])
def test_example_runs(source: str, fake_api: FakeAPI) -> None:
    output = io.StringIO()
    with redirect_stdout(output):
        exec(compile(source, "README.md", "exec"), {"__name__": "__main__"})
    assert fake_api.sent, "the example made no call"
    assert output.getvalue(), "the example printed nothing"


JOURNAL = next(example for example in EXAMPLES if "orders.jsonl" in example)


def journal_after(fake_api: FakeAPI) -> list[dict[str, Any]]:
    with redirect_stdout(io.StringIO()):
        exec(compile(JOURNAL, "README.md", "exec"), {"__name__": "__main__"})
    return [json.loads(line) for line in Path("orders.jsonl").read_text().splitlines()]


def test_the_journal_example_resolves_a_settled_order(fake_api: FakeAPI) -> None:
    opened, resolved = journal_after(fake_api)

    assert resolved["key"] == opened["key"]
    assert resolved["state"] == "completed"


def test_the_journal_example_keeps_an_unknown_outcome_open(fake_api: FakeAPI) -> None:
    fake_api.lose_trades = True

    (opened,) = journal_after(fake_api)

    trades = [request for request in fake_api.sent if request.method == "POST"]
    assert trades, "the order was attempted"
    assert {request.headers["Idempotency-Key"] for request in trades} == {opened["key"]}
