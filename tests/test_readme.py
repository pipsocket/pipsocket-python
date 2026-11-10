"""Every Python example in README.md runs as written, against a fake API at the HTTP transport."""

from __future__ import annotations

import io
import json
import re
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any

import httpx
import pytest

from pipsocket._generated.operations import OPERATIONS

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


@pytest.fixture
def fake_api(monkeypatch: pytest.MonkeyPatch) -> list[httpx.Request]:
    sent: list[httpx.Request] = []

    def handle(_: object, request: httpx.Request) -> httpx.Response:
        assert request.url.host == "api.pipsocket.com"
        assert request.headers["Authorization"] == f"Bearer {API_KEY}"
        sent.append(request)
        for _ in request.stream:  # type: ignore[union-attr]  (sent, as a real transport would)
            pass
        return documented_answer(request)

    async def handle_async(_: object, request: httpx.Request) -> httpx.Response:
        return handle(_, request)

    monkeypatch.setenv("PIPSOCKET_KEY", API_KEY)
    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", handle)
    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", handle_async)
    return sent


def test_the_readme_has_examples() -> None:
    assert len(EXAMPLES) >= 5


@pytest.mark.parametrize("source", EXAMPLES, ids=[f"example-{i}" for i in range(len(EXAMPLES))])
def test_example_runs(source: str, fake_api: list[httpx.Request]) -> None:
    output = io.StringIO()
    with redirect_stdout(output):
        exec(compile(source, "README.md", "exec"), {"__name__": "__main__"})
    assert fake_api, "the example made no call"
    assert output.getvalue(), "the example printed nothing"
