"""What the sync and async clients share: options, headers, and reading httpx's outcomes."""

from __future__ import annotations

import os
import platform
import random
import time
from collections.abc import Callable
from dataclasses import dataclass

import httpx

from ._core import Answer, BodyLost, NoAnswer, Settings, Spec
from ._version import __version__
from .errors import PipsocketError

DEFAULT_BASE_URL = "https://api.pipsocket.com"
DEFAULT_STREAM_URL = "wss://wss.pipsocket.com"
DEFAULT_TIMEOUT = 40.0
DEFAULT_MAX_RETRIES = 2
DEFAULT_RETRY_DEADLINE = 60.0
KEY_ENV = "PIPSOCKET_KEY"

USER_AGENT = (
    f"pipsocket-python/{__version__} "
    f"{platform.python_implementation()}/{platform.python_version()} ({platform.system()})"
)

# httpx raises these before anything is written: the connection never opened.
_NOT_WRITTEN = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.PoolTimeout,
    httpx.ProxyError,
    httpx.UnsupportedProtocol,
)


@dataclass(frozen=True)
class Options:
    api_key: str
    base_url: str
    stream_url: str
    timeout: float
    max_retries: int
    retry_deadline: float
    clock: Callable[[], float]
    random: Callable[[], float]

    def settings(self, max_retries: int | None) -> Settings:
        retries = self.max_retries if max_retries is None else check_retries(max_retries)
        return Settings(retries, self.retry_deadline, self.clock, self.random)

    def attempt_timeout(self, timeout: float | None) -> float:
        return self.timeout if timeout is None else check_seconds("timeout", timeout)


def options(
    api_key: str | None,
    base_url: str | None,
    stream_url: str | None,
    timeout: float,
    max_retries: int,
    retry_deadline: float,
    clock: Callable[[], float] | None,
    rand: Callable[[], float] | None,
) -> Options:
    key = api_key if api_key is not None else os.environ.get(KEY_ENV)
    if not key:
        raise PipsocketError(
            f"No API key: pass api_key=, or set the {KEY_ENV} environment variable."
        )
    return Options(
        api_key=key,
        base_url=(base_url or DEFAULT_BASE_URL).rstrip("/"),
        stream_url=(stream_url or DEFAULT_STREAM_URL).rstrip("/"),
        timeout=check_seconds("timeout", timeout),
        max_retries=check_retries(max_retries),
        retry_deadline=check_seconds("retry_deadline", retry_deadline),
        clock=clock or time.monotonic,
        random=rand or random.random,
    )


def check_retries(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:  # pyright: ignore[reportUnnecessaryIsInstance]
        raise ValueError(f"max_retries is a whole number, 0 or more, not {value!r}.")
    return value


def check_seconds(name: str, value: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not value > 0:  # pyright: ignore[reportUnnecessaryIsInstance]
        raise ValueError(f"{name} is a number of seconds above 0, not {value!r}.")
    return float(value)


def build_request(
    client: httpx.Client | httpx.AsyncClient, spec: Spec, opts: Options, timeout: float
) -> httpx.Request:
    """The same request for every attempt of a call: method, URL, body and key don't change."""
    headers = {
        "Authorization": f"Bearer {opts.api_key}",
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
    }
    if spec.body is not None:
        headers["Content-Type"] = "application/json"
    if spec.idempotency_key is not None:
        headers["Idempotency-Key"] = spec.idempotency_key
    return client.build_request(
        spec.method,
        opts.base_url + spec.path,
        params=list(spec.query) or None,
        content=spec.body,
        headers=headers,
        timeout=httpx.Timeout(timeout),
    )


def no_answer(error: httpx.TransportError) -> NoAnswer:
    return NoAnswer(
        error=error,
        written=not isinstance(error, _NOT_WRITTEN),
        timed_out=isinstance(error, httpx.TimeoutException),
    )


def answer(response: httpx.Response, content: bytes) -> Answer:
    return Answer(status=response.status_code, headers=response.headers, content=content)


def body_lost(response: httpx.Response, error: Exception) -> BodyLost:
    return BodyLost(status=response.status_code, error=error)
