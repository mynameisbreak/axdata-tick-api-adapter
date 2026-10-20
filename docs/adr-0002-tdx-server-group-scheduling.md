# ADR-0002: Schedule TDX Batches Across Per-Server Actor Pools

## Status

Accepted

## Context

The Actor transport gives each pool slot exclusive ownership of one socket.
Request options, however, previously multiplied `source_server_count` by
`connections_per_server` and created one pool with many candidate hosts. That
made the requested topology misleading: ten servers by eight connections
became one 80-slot failover pool, and realtime quote batches were still issued
serially by the quote API.

Public TDX servers also fail independently. A multi-server request must keep a
slow or unavailable server from invalidating work assigned to healthy servers,
without moving host health policy into the socket Actor.

## Decision

Add a provider-owned server-group scheduler above `PooledSocketTransport`.
The configured host count and `connections_per_server` are upper bounds rather
than an eager allocation. For `B` protocol batches, `H` candidate hosts, and a
per-host limit `C`, the scheduler activates `min(B, H)` hosts and opens
`min(C, ceil(B / active_hosts))` Actor/socket slots per active host. Protocol
batches are then distributed round-robin across per-host executors. Each
executor is bounded by that host's planned connection count, so one server
cannot consume another server's worker allocation.

Servers that fail initial connection are excluded for that request. The
scheduler activates later candidate hosts as replacements without exceeding
the workload-derived active-host limit. A failed batch is transferred to
another connected server at most once. Results retain
their original batch indexes and quote rows are merged in requested-code order
with duplicate codes removed. All temporary clients are closed on success or
failure.

The meanings of connection options are:

- `hosts` and `source_server_count`: candidate servers and maximum server count.
- `connections_per_server`: maximum Actor/socket slots in each server's pool.
- `pool_size`: compatibility size for an ordinary single failover client.

Configured server groups are limited to 128 total slots, and actual allocation
is further capped by the current workload. Small quote requests and requests
using an externally injected client stay on the ordinary client path.

Request metadata reports the configured maximum, workload-derived planned host
count, healthy host count, configured per-host maximum, and actual per-host
connection count separately.

## Consequences

Positive:

- Configured topology matches runtime topology.
- Small workloads no longer eagerly connect every configured Actor/socket slot.
- Batch snapshots use multiple servers and multiple Actor slots concurrently.
- Connection failure degrades capacity instead of failing the whole request.
- Input ordering and bounded retry behavior are deterministic.

Negative:

- Explicit large-batch requests create and close one client per selected host.
- More configured slots consume more threads, sockets, and server resources.
- One retry improves transient reliability but cannot mask sustained server
  failure.

## Follow-Ups

- Reuse the same server-group execution primitive for K-line batches after its
  result model is aligned with per-code pagination metadata.
- Add server health cooldown and latency scoring only with production evidence;
  request-local failure isolation remains the default.
- Benchmark conservative defaults before changing collector profiles.
