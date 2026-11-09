# Conformance cases

`call-cases.json` describes, without any programming language, how a Pipsocket SDK must turn the
API's answers into results, retries and errors. The Python and TypeScript SDKs both run every case, so
they behave the same.

The most important rule it pins down: a retry never changes the `Idempotency-Key`, and once an attempt
may have reached the account's host, the call never reports that nothing was sent.

## Running a case

Each case scripts the answers to one SDK call. A test harness:

1. Builds a client with a fake transport, a fake clock and a fake sleep (no real waiting, no network).
   `defaults` are the client's settings; a case's `options` override them.
2. Makes the call named by `call` (see `calls` for its method, path and body), passing the case's
   `idempotency_key` if it has one.
3. Answers attempt *n* with `attempts[n]`:
   - `respond`: an HTTP answer with `status`, optional `headers`, and one of `body` (JSON; `{"use":
     "name"}` means `bodies.name`), `text` (the raw body, not JSON) or `body_error: true` (the status
     and headers arrive, then reading the body fails).
   - `fail: "connect"`: the connection never opened, so nothing was written.
   - `fail: "after_write"`: the request was written, then the connection timed out or was reset.
   - `elapsed`: seconds the fake clock moves forward while this attempt runs.
4. Fails the test if the SDK makes more attempts than the script has.

## What every case checks

- The number of attempts equals `expect.attempts`.
- Every attempt has the same method, URL and body.
- For a call with `keyed: true`, every attempt carries the same `Idempotency-Key`: the caller's own when
  the case gives one, otherwise one the SDK made (a UUID).
- `expect.sleeps`, when given: the wait before each retry is within `min` and `max` seconds.

## Expected results

- `return.trade_request`: the call returns a trade request with these fields (`kind` is the closed
  discriminator of the TypeScript SDK; the Python SDK checks `state` only). `idempotency_key` is always
  set on the answer to a keyed call; the case names it only when the caller chose it.
- `return.json`: the call returns exactly this body. `return.json_has`: an object with these keys.
- `raise.error`: the error's class, by its name in the SDK. `ArgumentError` means the language's own
  error for a bad argument (`ValueError` in Python, `TypeError` in TypeScript), raised before anything is
  sent.
- `raise.code`, `raise.status`: the API's `error` code and the HTTP status on the error.
- `raise.cause`, `raise.cause_code`: for `OutcomeUnknown`, the class and code of the error it wraps.
  `OutcomeUnknown` always carries the idempotency key.
- `raise.retry_after`, `raise.blocking`, `raise.kill_switch`: fields of that name on the error
  (`blocking` lists request ids).

## Changing this file

The two SDK repositories keep identical copies. Change both together.
