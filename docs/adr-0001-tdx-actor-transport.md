# ADR-0001: Use Actor-Owned TDX Transport Slots

## Status

Accepted

## Context

AxData's TDX transport previously used a shared socket protected by locks,
separate reader and heartbeat threads, and a round-robin connection pool. The
model made socket ownership, admission, reconnect, and shutdown difficult to
reason about under concurrent collectors.

## Decision

Each TDX pool slot is an actor-owned `SocketTransport`: one daemon actor thread
owns one socket and performs all connect, send, receive, heartbeat, reconnect,
and close operations. Callers submit synchronous request tickets. The pool
admits tickets with a bounded FIFO queue and leases idle slots until the request
finishes.

The public `TdxClient`, provider request methods, and transport protocol remain
unchanged. This is an internal runtime replacement.

## Consequences

Positive:

- Socket access has one owner, removing reader/heartbeat/socket lock races.
- Pool admission is bounded and fairer than round-robin selection.
- Reconnect and shutdown are serialized with request execution.
- The existing synchronous API remains compatible.

Negative:

- Each configured pool slot owns one actor thread.
- A single slot remains serial by design; parallelism comes from pool slots.
- In-flight cancellation closes or times out the slot rather than interrupting
  arbitrary protocol parsing.

## Risks And Follow-Ups

- Add deterministic tests for partial frames, actor shutdown, pool admission,
  reconnect, and bounded push delivery.
- Add run-level cancellation propagation from Collector Scheduler into provider
  requests.
- Benchmark pool sizes and tail latency before changing defaults.
