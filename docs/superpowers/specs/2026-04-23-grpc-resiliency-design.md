# gRPC connection resiliency design

## Problem statement

`durabletask-python` already has basic gRPC retry policy configuration,
keepalive channel settings, and worker-side reconnect logic. It does not yet
have the stronger connection-healing behavior added in
`durabletask-dotnet` PR 708:

- worker-side silent-disconnect detection for long-lived work-item streams
- consistent transport-failure classification
- client-side channel recreation after repeated transport failures
- shared backoff and threshold logic across connection-owning components

The current gap shows up most clearly when a channel becomes stale or
half-open. The worker may continue retrying around a poisoned stream without a
clear distinction between graceful close and silent disconnect, and clients may
keep reusing a bad channel until the application recreates the client.

## Goals

- Detect and heal stale or silently disconnected gRPC connections in the worker
  and in sync and async clients.
- Enable the new behavior by default with conservative values and explicit
  override and disable knobs.
- Preserve existing protocol behavior and support for caller-supplied channels.
- Keep low-level gRPC channel options separate from SDK-managed resiliency
  policy.
- Add focused regression tests for failure classification, backoff, and channel
  recreation.

## Non-goals

- Redesign the public orchestration APIs or the sidecar protocol.
- Add general channel pooling or multi-endpoint load-balancing support.
- Automatically recreate caller-supplied channels in this iteration.
- Expand every possible raw gRPC channel knob as part of this work.

## Proposed public API

Add two new option dataclasses in `durabletask.grpc_options`.

### `GrpcWorkerResiliencyOptions`

Used by `TaskHubGrpcWorker` and Azure Managed worker wrappers.

| Field | Default | Meaning |
| --- | --- | --- |
| `hello_timeout_seconds` | `30.0` | Deadline for the initial `Hello` handshake on a fresh connection. |
| `silent_disconnect_timeout_seconds` | `120.0` | Maximum idle period on the `GetWorkItems` stream before the worker treats the connection as stale. A value `<= 0` disables silent-disconnect detection. |
| `channel_recreate_failure_threshold` | `5` | Number of consecutive transport-shaped failures before the worker recreates an SDK-owned channel. A value `<= 0` disables recreation. |
| `reconnect_backoff_base_seconds` | `1.0` | Base delay for reconnect backoff. |
| `reconnect_backoff_cap_seconds` | `30.0` | Maximum reconnect delay. |

### `GrpcClientResiliencyOptions`

Used by `TaskHubGrpcClient`, `AsyncTaskHubGrpcClient`, and Azure Managed client
wrappers.

| Field | Default | Meaning |
| --- | --- | --- |
| `channel_recreate_failure_threshold` | `5` | Number of consecutive transport-shaped unary RPC failures before recreating an SDK-owned channel. A value `<= 0` disables recreation. |
| `min_recreate_interval_seconds` | `30.0` | Minimum interval between channel recreation attempts. |

### Constructor changes

Add a new optional `resiliency_options` parameter to these constructors:

- `TaskHubGrpcWorker`
- `TaskHubGrpcClient`
- `AsyncTaskHubGrpcClient`
- `DurableTaskSchedulerWorker`
- `DurableTaskSchedulerClient`
- `AsyncDurableTaskSchedulerClient`

If the parameter is omitted, the SDK uses the defaults above. This keeps the
new behavior enabled by default while still allowing targeted disablement.

`GrpcChannelOptions` remains the place for raw gRPC transport settings such as
keepalive and retry service config. Resiliency policy stays separate because it
controls SDK behavior, not just channel construction.

## Runtime design

### Shared internal helpers

Add a small internal module dedicated to resiliency primitives. It should stay
transport-focused and reusable by the worker and clients.

Planned responsibilities:

- full-jitter exponential backoff calculation
- transport-failure classification helpers
- consecutive-failure tracking with reset semantics
- small immutable state objects where atomic swap is needed

The worker and client should share the same definition of
"transport-shaped failure" instead of maintaining separate ad hoc rules.

### Worker behavior

The worker keeps its current high-level reconnect loop but replaces the
connection-health logic with clearer internal pieces.

#### Fresh connection establishment

When the worker creates an SDK-owned channel, it:

1. builds the channel and stub as it does today
2. sends `Hello` with `hello_timeout_seconds`
3. treats `UNAVAILABLE` and `DEADLINE_EXCEEDED` on that handshake as transport
   failures

Successful `Hello` resets the worker reconnect attempt counter.

#### Stream monitoring

Wrap the `GetWorkItems` stream in an internal monitor that tracks two things:

- whether any message has ever been observed on the stream
- whether the stream has remained idle longer than
  `silent_disconnect_timeout_seconds`

The monitor reports one of these outcomes:

- `shutdown`: worker shutdown was requested
- `message_received`: at least one message arrived and normal processing
  continues
- `graceful_close_before_first_message`: peer closed the stream before the
  worker observed any message
- `graceful_close_after_message`: peer closed the stream after at least one
  message was observed
- `silent_disconnect`: the stream remained idle past the configured timeout

The outer worker loop uses those outcomes as follows:

- `message_received`: reset health counters
- `graceful_close_before_first_message`: immediately reset the current stream
  and force a fresh SDK-owned channel on the next connect attempt
- `graceful_close_after_message`: immediately reset the current stream and
  reconnect without incrementing the transport-failure counter
- `silent_disconnect`: count as channel poison
- `shutdown`: exit cleanly

This keeps rolling upgrades and normal peer-driven reconnects from inflating
the failure threshold while still forcing SDK-owned workers to establish a
fresh channel after graceful stream closures.

#### Failure counting and recreation

The worker increments the consecutive-failure counter only for
transport-shaped failures:

- `UNAVAILABLE`
- `DEADLINE_EXCEEDED`
- explicit silent-disconnect timeout

It does not increment the counter for errors that channel recreation is
unlikely to fix, such as:

- `UNAUTHENTICATED`
- `NOT_FOUND`
- orchestration or activity execution failures
- graceful stream closures before or after work items

When the threshold is reached and the worker owns the channel, it recreates the
channel and stub. Graceful stream closures also force an immediate fresh
SDK-owned channel even though they do not increment the threshold. When the
worker does not own the channel, it keeps retrying the existing transport and
logs that the channel could not be recreated.

### Client behavior

Both sync and async clients route unary RPCs through a small internal invoker
helper instead of calling generated stub methods directly.

The helper:

- invokes the target unary RPC
- classifies the outcome
- updates a shared failure counter
- schedules channel recreation when the threshold is crossed

#### Counted failures

Count these failures toward the client recreation threshold:

- `UNAVAILABLE`
- `DEADLINE_EXCEEDED` for ordinary unary calls

Do not count deadline failures for long-poll methods because those calls are
expected to wait:

- `wait_for_orchestration_start`
- `wait_for_orchestration_completion`
- async variants of those methods

Successful replies and application-level RPC errors reset the failure counter,
because they prove the underlying transport is still usable.

#### Channel recreation mechanics

When the threshold is reached and the client owns the channel:

1. enforce `min_recreate_interval_seconds`
2. allow only one recreation in flight at a time
3. build a fresh channel and stub with the existing host, interceptors, secure
   channel flag, and `GrpcChannelOptions`
4. atomically swap the active channel and stub
5. retire the previous channel after a short grace period

The failing RPC still fails normally. The recreated channel benefits later RPCs.

If the caller supplied the channel, the client still tracks and logs transport
failures but does not attempt replacement.

### Retiring replaced channels

Closing the old channel immediately after a successful swap risks interrupting
in-flight work that captured the old stub before the swap. To avoid that, the
SDK keeps replaced SDK-owned channels alive for a short grace period and then
closes them.

The implementation can use a small internal scheduler that is appropriate for
the transport:

- sync clients and the worker: daemon timer or background thread
- async clients: background task plus `asyncio.sleep`

All retired channels are also closed during final client or worker shutdown.

## File-level implementation plan

### `durabletask/grpc_options.py`

- add `GrpcWorkerResiliencyOptions`
- add `GrpcClientResiliencyOptions`
- add validation for positive durations when enabled

### `durabletask/internal/grpc_resiliency.py`

Add shared internals for:

- backoff calculation
- failure classification
- failure-threshold tracking
- small helper types used by worker and client code

### `durabletask/worker.py`

- accept `resiliency_options`
- replace the current ad hoc reconnect bookkeeping with the shared helpers
- add hello deadline handling
- add stream-outcome monitoring
- recreate SDK-owned channels when the threshold is crossed

### `durabletask/client.py`

- accept `resiliency_options` in sync and async clients
- centralize unary RPC invocation through internal helpers
- add single-flight channel recreation and cooldown logic
- retain current ownership semantics for caller-supplied channels

### Azure Managed wrappers

Thread the new `resiliency_options` parameter through:

- `DurableTaskSchedulerWorker`
- `DurableTaskSchedulerClient`
- `AsyncDurableTaskSchedulerClient`

No Azure-specific recreation behavior is required in this iteration because the
wrappers already build SDK-owned channels through the base client and worker
constructors.

## Testing strategy

Add focused unit tests for the new behavior.

### Options and helper tests

- new resiliency option validation
- full-jitter backoff bounds and cap behavior
- failure counter reset and threshold logic
- transport-failure classification rules

### Worker tests

- hello deadline failure counts toward recreation
- silent-disconnect timeout is detected and classified
- graceful stream closes force a fresh SDK-owned connection without increasing
  the failure counter
- user-supplied channels are not recreated or closed

### Client tests

- repeated `UNAVAILABLE` failures trigger recreation for SDK-owned channels
- long-poll `DEADLINE_EXCEEDED` does not count toward recreation
- application-level RPC errors reset the counter
- recreation is single-flight and cooldown-limited
- replaced channels are closed after the grace period
- caller-supplied channels are observed but not replaced

### Regression coverage

Existing client and worker tests should continue to pass without requiring users
to opt into the new behavior.

## Compatibility and rollout

- The change is backward compatible because all new constructor parameters are
  optional.
- The new behavior is enabled by default for SDK-owned channels only.
- Caller-supplied channels preserve existing ownership and lifecycle behavior.
- No protocol changes are required between the Python SDK and the sidecar.
- The changelog should describe the new automatic healing of stale gRPC worker
  and client connections and the new resiliency option types.

## Decision summary

Implement parity-inspired connection healing from `durabletask-dotnet` PR 708
by adding explicit worker stream monitoring, shared failure classification, and
client-side channel recreation for SDK-owned channels. Keep raw gRPC channel
configuration separate from SDK resiliency policy and leave broader channel
pooling and user-supplied channel recreation out of this iteration.
