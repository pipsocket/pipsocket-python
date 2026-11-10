"""The stream helper's shells: they do the I/O for the state machine in _stream_core.

One engine, on asyncio and websockets, runs the machine: it opens and reads the socket, keeps
the timers, makes the REST reads and queues events for the caller. `AsyncStream` runs it in the
caller's event loop; `Stream` runs it on a background thread with its own loop, so a synchronous
caller can take events at its own pace while the socket is still read promptly (pings and call
answers need that).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import threading
from collections import OrderedDict, deque
from collections.abc import Awaitable, Callable, Iterable
from types import TracebackType
from typing import TYPE_CHECKING, Any, Protocol

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidStatus, InvalidURI

from . import _stream_core as core
from ._core import refusal_from_body, segment
from ._http import USER_AGENT, Options
from .errors import (
    AuthenticationError,
    NotFound,
    PipsocketError,
    StreamError,
    StreamHandshakeError,
)
from .models import FINAL_STATES

__all__ = ["AsyncStream", "Stream"]

# Events the caller hasn't taken yet. When this many wait, the queue is emptied and the stream
# resyncs (reason "overflow").
DEFAULT_QUEUE_SIZE = 1000
# How long one upgrade may take.
OPEN_TIMEOUT = 10.0

Event = dict[str, Any]
Reader = Callable[[core.Read], Awaitable[Any]]
Watcher = Callable[[Event], None]


class _End:
    """Marks the end of the events: closed by the caller, or stopped with an error."""

    def __init__(self, error: PipsocketError | None) -> None:
        self.error = error


class _Crashed:
    """A spawned task's unexpected failure, queued for the engine's loop to end the stream."""

    def __init__(self, error: Exception) -> None:
        self.error = error


class Sink(Protocol):
    """Where the engine puts events for the caller."""

    def put(self, item: Event | _End) -> list[Event]:
        """Queue an item. If the queue was full it is emptied, and everything in it, with this
        item, is returned: the events the caller will never get."""
        ...


class Settled:
    """Recorded requests the stream has seen settle, and the wait() calls watching for them.
    Shared with the caller's thread, hence the lock."""

    REMEMBERED = 1000

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.finals: OrderedDict[str, Event] = OrderedDict()
        self.watchers: dict[str, set[Watcher]] = {}

    def saw(self, body: Event) -> None:
        request_id = body.get("request")
        if not isinstance(request_id, str):
            return
        with self.lock:
            if body.get("state") in FINAL_STATES:
                self.finals[request_id] = body
                self.finals.move_to_end(request_id)
                while len(self.finals) > self.REMEMBERED:
                    self.finals.popitem(last=False)
            watchers = list(self.watchers.get(request_id, ()))
        for watcher in watchers:
            watcher(body)

    def final(self, request_id: str) -> Event | None:
        with self.lock:
            return self.finals.get(request_id)

    def watch(self, request_id: str, watcher: Watcher) -> bool:
        """Add a watcher, and give it at once a settlement already seen (registering and
        reading the cache under one lock, so none slips between). True if it is the first
        watcher for this id."""
        with self.lock:
            watchers = self.watchers.setdefault(request_id, set())
            first = not watchers
            watchers.add(watcher)
            known = self.finals.get(request_id)
        if known is not None:
            watcher(known)
        return first

    def unwatch(self, request_id: str, watcher: Watcher) -> bool:
        """Remove a watcher. True if it was the last for this id."""
        with self.lock:
            watchers = self.watchers.get(request_id, set())
            watchers.discard(watcher)
            if watchers:
                return False
            self.watchers.pop(request_id, None)
            return True


if TYPE_CHECKING:
    _LoggerAdapter = logging.LoggerAdapter[logging.Logger]
else:  # not subscriptable at run time before Python 3.11
    _LoggerAdapter = logging.LoggerAdapter


class _Redacted(_LoggerAdapter):
    """websockets' logger, with the API key taken out of everything it logs (it logs the
    upgrade's headers at debug level)."""

    def __init__(self, logger: logging.Logger, secret: str) -> None:
        super().__init__(logger, {})
        self.secret = secret

    def log(self, level: int, msg: object, *args: object, **kwargs: Any) -> None:
        if self.isEnabledFor(level):
            msg = self.redact(msg)
            args = tuple(self.redact(arg) for arg in args)
            super().log(level, msg, *args, **kwargs)

    def redact(self, value: object) -> object:
        if isinstance(value, (str, bytes)) and self.secret in str(value):
            return str(value).replace(self.secret, "[api key]")
        return value


class Engine:
    """Runs the machine: feeds it inputs and carries out its actions. One per stream."""

    def __init__(
        self,
        *,
        url: str,
        api_key: str,
        machine: core.StreamMachine,
        reader: Reader,
        sink: Sink,
        settled: Settled,
    ) -> None:
        self.url = url
        self.api_key = api_key
        self.machine = machine
        self.reader = reader
        self.sink = sink
        self.settled = settled
        self.inputs: asyncio.Queue[core.Input | _Crashed] = asyncio.Queue()
        self.socket: ClientConnection | None = None
        self.socket_generation = 0
        self.tasks: set[asyncio.Task[Any]] = set()
        # One handle per timer name: starting a timer replaces (cancels) the one before.
        self.timers: dict[str, asyncio.TimerHandle] = {}
        self.dropped: list[Event] | None = None
        self.finished = False
        self.logger = _Redacted(logging.getLogger("websockets.client"), api_key)

    def feed(self, event: core.Input) -> None:
        self.inputs.put_nowait(event)

    async def run(self) -> None:
        self.feed(core.Start())
        try:
            next_event: core.Input | None = None
            while not self.finished:
                event = next_event or await self.inputs.get()
                next_event = None
                if isinstance(event, _Crashed):
                    raise event.error
                self.dropped = None
                for action in self.machine.handle(event):
                    await self.do(action)
                if self.dropped is not None:
                    # The queue overflowed: the rest of this batch is dropped with it, and the
                    # machine hears of it before any other input, so no event reaches the
                    # caller (or moves the checkpoint) ahead of the resync that replaces them.
                    next_event = core.Overflowed(tuple(self.dropped))
                if isinstance(event, core.Close):
                    self.finish(None)
        except Exception as error:
            # A failure of the SDK's own: end the stream with it, so the caller hears of it.
            self.finish(unexpected(error))
        finally:
            await self.shut()

    def finish(self, error: PipsocketError | None) -> None:
        if not self.finished:
            self.finished = True
            self.sink.put(_End(error))

    async def shut(self) -> None:
        for handle in self.timers.values():
            handle.cancel()
        for task in list(self.tasks):
            task.cancel()
        await self.drop_socket()

    def spawn(self, work: Awaitable[Any]) -> None:
        task = asyncio.ensure_future(work)
        self.tasks.add(task)
        task.add_done_callback(self.supervise)

    def supervise(self, task: asyncio.Task[Any]) -> None:
        """A spawned task (opening, listening, a read) that fails unexpectedly ends the stream:
        its own expected failures are already inputs to the machine."""
        self.tasks.discard(task)
        if task.cancelled():
            return
        error = task.exception()
        if isinstance(error, Exception):
            self.inputs.put_nowait(_Crashed(error))

    async def do(self, action: core.Action) -> None:
        match action:
            case core.Connect():
                self.socket_generation += 1
                self.spawn(self.open(self.socket_generation))
            case core.Disconnect():
                await self.drop_socket()
            case core.Send(text=text):
                if self.socket is not None:
                    with contextlib.suppress(ConnectionClosed):
                        await self.socket.send(text)
            case core.StartTimer(name=name, seconds=seconds, generation=generation):
                previous = self.timers.get(name)
                if previous is not None:
                    previous.cancel()
                timer = core.Timer(name, generation)
                self.timers[name] = asyncio.get_running_loop().call_later(seconds, self.feed, timer)
            case core.Read():
                self.spawn(self.read(action))
            case core.Emit(event=event):
                if self.dropped is not None:
                    self.dropped.append(event)
                else:
                    dropped = self.sink.put(event)
                    if dropped:
                        self.dropped = dropped
            case core.Notify(body=body):
                self.settled.saw(body)
            case core.Fail(error=error):
                self.finish(error)

    async def open(self, generation: int) -> None:
        try:
            socket = await connect(
                self.url,
                additional_headers={"Authorization": f"Bearer {self.api_key}"},
                user_agent_header=USER_AGENT,
                ping_interval=None,
                open_timeout=OPEN_TIMEOUT,
                logger=self.logger,
            )
        except InvalidStatus as refused:
            status = refused.response.status_code
            self.feed(core.Refused(status, handshake_error(status, bytes(refused.response.body))))
            return
        except InvalidURI as error:
            self.feed(core.Refused(0, StreamHandshakeError(str(error), status=None)))
            return
        except (OSError, TimeoutError, ConnectionClosed, Exception) as error:
            self.feed(core.Unreachable(error))
            return
        if generation != self.socket_generation or self.finished:
            await socket.close()
            return
        self.socket = socket
        self.feed(core.Opened())
        self.spawn(self.listen(socket, generation))

    async def listen(self, socket: ClientConnection, generation: int) -> None:
        try:
            async for raw in socket:
                text = raw if isinstance(raw, str) else raw.decode("utf-8", "replace")
                if generation != self.socket_generation:
                    return
                self.feed(parse(text))
        except ConnectionClosed:
            pass
        if generation == self.socket_generation:
            self.feed(core.Closed())

    async def drop_socket(self) -> None:
        socket, self.socket = self.socket, None
        self.socket_generation += 1
        if socket is not None:
            with contextlib.suppress(Exception):
                await socket.close()

    async def read(self, read: core.Read) -> None:
        try:
            value = await self.reader(read)
        except PipsocketError as error:
            self.feed(core.Answered(read, error=error))
        except Exception as error:
            failure = StreamError(f"A read the stream made failed: {error}")
            failure.__cause__ = error
            self.feed(core.Answered(read, error=failure))
        else:
            self.feed(core.Answered(read, value=value))


def unexpected(error: Exception) -> StreamError:
    failure = StreamError(f"The stream stopped unexpectedly: {error!r}")
    failure.__cause__ = error
    return failure


def parse(text: str) -> core.Input:
    try:
        body = json.loads(text)
    except ValueError:
        return core.Text(text)
    if isinstance(body, dict):
        return core.Message(body)  # pyright: ignore[reportUnknownArgumentType]
    return core.Text(text)


def handshake_error(status: int, raw: bytes) -> PipsocketError:
    """The error for a refused upgrade. A 401 or 404 is the API's refusal, as on REST (the key
    and account checks are the same); anything else is StreamHandshakeError with the status."""
    try:
        body = json.loads(raw)
    except (ValueError, RecursionError):
        body = None
    if status in (401, 404):
        if isinstance(body, dict) and isinstance(body.get("error"), str):  # pyright: ignore[reportUnknownMemberType]
            return refusal_from_body(status, body, raw)
        kind = AuthenticationError if status == 401 else NotFound
        message = f"The stream's upgrade was refused ({status})."
        return kind(message, status=status, code=None, body=body, raw=raw)
    return StreamHandshakeError(f"The stream's upgrade was refused ({status}).", status=status)


def stream_url(options: Options, account: str) -> str:
    return f"{options.stream_url}/v1/accounts/{segment(account)}/stream"


def new_machine(
    options: Options, ticks: Iterable[str], snapshot: bool, queue_size: int
) -> core.StreamMachine:
    if isinstance(ticks, str):
        raise ValueError("ticks is a list of symbols, not one string.")
    if queue_size < 1:
        raise ValueError("queue_size is 1 or more.")
    return core.StreamMachine(
        ticks=ticks, read_state=snapshot, queue_limit=queue_size, random=options.random
    )


def sync_reader(account: Any) -> Callable[[core.Read], Any]:
    """The REST reads the machine asks for, made with a sync account handle."""

    def read(request: core.Read) -> Any:
        if request.kind == "request":
            return account.requests.get(request.request_id).raw
        return read_with(account, request)

    return read


def async_reader(account: Any) -> Reader:
    """The REST reads the machine asks for, made with an async account handle."""

    async def read(request: core.Read) -> Any:
        if request.kind == "request":
            return (await account.requests.get(request.request_id)).raw
        return await read_with(account, request)

    return read


def read_with(account: Any, request: core.Read) -> Any:
    """Makes a snapshot or history read on the handle; an async handle returns an awaitable."""
    if request.kind == "account":
        return account.get()
    if request.kind == "positions":
        return account.positions.list()
    if request.kind == "orders":
        return account.orders.list()
    if request.kind == "deals":
        return account.history.deals(request.from_, request.to)
    if request.kind == "history_orders":
        return account.history.orders(request.from_, request.to)
    raise ValueError(f"Unknown read: {request.kind}")


# The async shell.


class _AsyncSink:
    def __init__(self, limit: int) -> None:
        self.items: deque[Event | _End] = deque()
        self.limit = limit
        self.closed = False
        self.ready = asyncio.Event()

    def put(self, item: Event | _End) -> list[Event]:
        if self.closed:
            return []  # the caller closed the stream: nothing more is delivered
        if not isinstance(item, _End) and len(self.items) >= self.limit:
            dropped = [event for event in self.items if not isinstance(event, _End)] + [item]
            self.items.clear()
            return dropped
        self.items.append(item)
        self.ready.set()
        return []

    def close(self) -> None:
        """The caller closed the stream: nothing still queued, or put later, is delivered."""
        self.closed = True
        self.items.clear()
        self.items.append(_End(None))
        self.ready.set()

    async def get(self) -> Event | _End:
        while not self.items:
            self.ready.clear()
            await self.ready.wait()
        return self.items.popleft()


class AsyncStream:
    """The account's events, as `async with acct.stream(ticks=[...]) as events`, then
    `async for event in events`. Events are the API's messages as they are, plus `resync`."""

    def __init__(
        self,
        *,
        url: str,
        options: Options,
        machine: core.StreamMachine,
        reader: Reader,
        streams: list[Any],
    ) -> None:
        self._sink = _AsyncSink(machine.queue_limit)
        self._settled = Settled()
        self._engine = Engine(
            url=url,
            api_key=options.api_key,
            machine=machine,
            reader=reader,
            sink=self._sink,
            settled=self._settled,
        )
        self._streams = streams
        self._task: asyncio.Task[None] | None = None
        self._ended: _End | None = None

    async def __aenter__(self) -> AsyncStream:
        self._task = asyncio.ensure_future(self._engine.run())
        self._streams.append(self)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            await self.close()
        except Exception:
            if exc is None:
                raise
            # An exception is already unwinding the caller's block: it isn't replaced.

    def __aiter__(self) -> AsyncStream:
        return self

    async def __anext__(self) -> Event:
        if self._ended is not None:
            return self._end()
        item = await self._sink.get()
        if isinstance(item, _End):
            self._ended = item
            return self._end()
        return item

    def _end(self) -> Event:
        assert self._ended is not None
        if self._ended.error is not None:
            raise self._ended.error
        raise StopAsyncIteration

    async def set_ticks(self, symbols: Iterable[str]) -> None:
        """Ask for ticks of these symbols (the whole set, up to 100; [] stops them)."""
        if isinstance(symbols, str):
            raise ValueError("symbols is a list of symbols, not one string.")
        self._engine.feed(core.SetTicks(tuple(symbols)))

    async def close(self) -> None:
        """Close the stream. Iteration ends at once: events still queued are not delivered.
        Waiting wait() calls carry on polling over REST."""
        if self in self._streams:
            self._streams.remove(self)
        if self._ended is None:
            self._sink.close()
        if self._task is None:
            return
        self._engine.feed(core.Close())
        # Shielded: if the caller is cancelled meanwhile, that cancellation is the caller's to
        # see, and the engine, already told to close, finishes shutting down on its own.
        await asyncio.shield(self._task)

    def _feed(self, event: core.Input) -> None:
        self._engine.feed(event)


# The sync shell.


class _ThreadSink:
    def __init__(self, limit: int) -> None:
        self.items: deque[Event | _End] = deque()
        self.limit = limit
        self.closed = False
        self.ready = threading.Condition()

    def put(self, item: Event | _End) -> list[Event]:
        with self.ready:
            if self.closed:
                return []  # the caller closed the stream: nothing more is delivered
            if not isinstance(item, _End) and len(self.items) >= self.limit:
                dropped = [event for event in self.items if not isinstance(event, _End)] + [item]
                self.items.clear()
                return dropped
            self.items.append(item)
            self.ready.notify_all()
            return []

    def close(self) -> None:
        """The caller closed the stream: nothing still queued, or put later, is delivered."""
        with self.ready:
            self.closed = True
            self.items.clear()
            self.items.append(_End(None))
            self.ready.notify_all()

    def get(self) -> Event | _End:
        with self.ready:
            while not self.items:
                self.ready.wait()
            return self.items.popleft()


class Stream:
    """The account's events, as `with acct.stream(ticks=[...]) as events`, then
    `for event in events`. The socket is read on a background thread."""

    def __init__(
        self,
        *,
        url: str,
        options: Options,
        machine: core.StreamMachine,
        reader: Callable[[core.Read], Any],
        streams: list[Any],
    ) -> None:
        self._sink = _ThreadSink(machine.queue_limit)
        self._settled = Settled()
        self._url = url
        self._options = options
        self._machine = machine
        self._reader = reader
        self._streams = streams
        self._loop: asyncio.AbstractEventLoop | None = None
        self._engine: Engine | None = None
        self._thread: threading.Thread | None = None
        self._ended: _End | None = None

    def __enter__(self) -> Stream:
        started = threading.Event()
        self._thread = threading.Thread(target=self._run, args=(started,), daemon=True)
        self._thread.start()
        started.wait()
        self._streams.append(self)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def _run(self, started: threading.Event) -> None:
        try:
            loop = asyncio.new_event_loop()
        except Exception as error:  # out of file descriptors, say
            self._sink.put(_End(unexpected(error)))
            started.set()
            return
        self._loop = loop

        async def read(request: core.Read) -> Any:
            return await asyncio.to_thread(self._reader, request)

        async def main() -> None:
            self._engine = Engine(
                url=self._url,
                api_key=self._options.api_key,
                machine=self._machine,
                reader=read,
                sink=self._sink,
                settled=self._settled,
            )
            started.set()
            await self._engine.run()

        try:
            loop.run_until_complete(main())
        except Exception as error:
            # The engine ends itself on its own failures; this is anything around it.
            self._sink.put(_End(unexpected(error)))
        finally:
            started.set()
            loop.close()

    def _feed(self, event: core.Input) -> None:
        loop, engine = self._loop, self._engine
        if loop is None or engine is None or loop.is_closed():
            return
        with contextlib.suppress(RuntimeError):
            loop.call_soon_threadsafe(engine.feed, event)

    def __iter__(self) -> Stream:
        return self

    def __next__(self) -> Event:
        if self._ended is None:
            item = self._sink.get()
            if isinstance(item, _End):
                self._ended = item
            else:
                return item
        if self._ended.error is not None:
            raise self._ended.error
        raise StopIteration

    def set_ticks(self, symbols: Iterable[str]) -> None:
        """Ask for ticks of these symbols (the whole set, up to 100; [] stops them)."""
        if isinstance(symbols, str):
            raise ValueError("symbols is a list of symbols, not one string.")
        self._feed(core.SetTicks(tuple(symbols)))

    def close(self) -> None:
        """Close the stream. Iteration ends at once, from any thread (a `for` loop blocked
        waiting for an event wakes and stops): events still queued are not delivered. Waiting
        wait() calls carry on polling over REST."""
        if self in self._streams:
            self._streams.remove(self)
        if self._ended is None:
            self._sink.close()
        self._feed(core.Close())
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join()


# What requests.wait() and the trade calls use from the streams open on their account handle.


def watch(stream: Stream | AsyncStream, request_id: str, watcher: Watcher) -> Callable[[], None]:
    """Call `watcher` with each recorded-request body the stream sees for this id (on the
    stream's thread, for a sync stream), and look the id up on every resync. Returns the undo."""
    if stream._settled.watch(request_id, watcher):  # pyright: ignore[reportPrivateUsage]
        stream._feed(core.Watch(request_id))  # pyright: ignore[reportPrivateUsage]

    def unwatch() -> None:
        if stream._settled.unwatch(request_id, watcher):  # pyright: ignore[reportPrivateUsage]
            stream._feed(core.Unwatch(request_id))  # pyright: ignore[reportPrivateUsage]

    return unwatch


def settled(streams: Iterable[Stream | AsyncStream], request_id: str) -> Event | None:
    """The request in a final state, if an open stream has seen it settle."""
    for stream in list(streams):
        body = stream._settled.final(request_id)  # pyright: ignore[reportPrivateUsage]
        if body is not None:
            return body
    return None


def report(streams: Iterable[Stream | AsyncStream], error: PipsocketError) -> None:
    """A read outside the streams failed: a 401 or a gone account stops them too."""
    if core.fatal(error):
        for stream in list(streams):
            stream._feed(core.Fatal(error))  # pyright: ignore[reportPrivateUsage]
