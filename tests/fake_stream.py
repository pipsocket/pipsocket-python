"""A local fake of the account stream, on its own thread and event loop.

It answers the upgrade (or refuses it), sends `subscribed`, answers "ping" with "pong" and
PUT /ticks calls with the set, and records what each connection received. Tests push events
with send() and drop connections with drop().
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from http import HTTPStatus
from typing import Any

from websockets.asyncio.server import Server, ServerConnection, serve
from websockets.http11 import Request, Response

Refusal = tuple[int, dict[str, Any]] | None


@dataclass
class Connection:
    socket: ServerConnection
    headers: dict[str, str]
    received: list[Any] = field(default_factory=list[Any])


class FakeStreamServer:
    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()
        self.connections: list[Connection] = []
        self.upgrades = 0
        # Called with the upgrade's number (1, 2, ...): a (status, body) refusal, or None.
        self.refuse: Callable[[int], Refusal] = lambda _: None
        # Events sent to every connection once its tick set is answered.
        self.after_ticks: list[dict[str, Any]] = []
        self.server: Server | None = None
        ready = threading.Event()
        self.thread = threading.Thread(target=self._run, args=(ready,), daemon=True)
        self.thread.start()
        ready.wait()

    def _run(self, ready: threading.Event) -> None:
        asyncio.set_event_loop(self.loop)

        async def start() -> None:
            self.server = await serve(
                self._handle, "127.0.0.1", 0, process_request=self._process_request
            )
            ready.set()

        self.loop.run_until_complete(start())
        self.loop.run_forever()

    @property
    def url(self) -> str:
        assert self.server is not None
        host, port = next(iter(self.server.sockets)).getsockname()[:2]
        return f"ws://{host}:{port}"

    def _process_request(self, connection: ServerConnection, request: Request) -> Response | None:
        self.upgrades += 1
        refusal = self.refuse(self.upgrades)
        if refusal is None:
            return None
        status, body = refusal
        response = connection.respond(HTTPStatus(status), json.dumps(body))
        response.headers["Content-Type"] = "application/json"
        return response

    async def _handle(self, socket: ServerConnection) -> None:
        assert socket.request is not None
        connection = Connection(socket, dict(socket.request.headers.raw_items()))
        self.connections.append(connection)
        await socket.send(
            json.dumps({"type": "subscribed", "account": "acc_01m6tx3k9f2qw8r7bznd5yvh4c"})
        )
        async for raw in socket:
            if raw == "ping":
                connection.received.append("ping")
                await socket.send("pong")
                continue
            call = json.loads(raw)
            connection.received.append(call)
            if call.get("path") == "/ticks":
                symbols = call["body"]["symbols"]
                answer = {
                    "type": "response",
                    "id": call["id"],
                    "status": 200,
                    "body": {"symbols": symbols},
                }
                await socket.send(json.dumps(answer))
                for event in self.after_ticks:
                    await socket.send(json.dumps(event))

    def _call(self, work: Any) -> Any:
        return asyncio.run_coroutine_threadsafe(work, self.loop).result(5)

    def send(self, event: dict[str, Any], connection: int = -1) -> None:
        self._call(self.connections[connection].socket.send(json.dumps(event)))

    def drop(self, connection: int = -1) -> None:
        self._call(self.connections[connection].socket.close())

    def wait_for(self, check: Callable[[], bool], seconds: float = 5.0) -> None:
        deadline = time.monotonic() + seconds
        while not check():
            if time.monotonic() > deadline:
                raise AssertionError("timed out waiting on the fake stream")
            time.sleep(0.01)

    def stop(self) -> None:
        async def close() -> None:
            assert self.server is not None
            self.server.close()
            await self.server.wait_closed()

        self._call(close())
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(5)
