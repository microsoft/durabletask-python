# Large Payload Support: Implementation Proposals for durabletask-python

> Companion to [large-payload-feature-comparison.md](large-payload-feature-comparison.md),
> which details the .NET SDK's existing feature and the full gap analysis.

## 1. Context

The .NET SDK ships large payload externalization as a separate NuGet package
(`Microsoft.DurableTask.Extensions.AzureBlobPayloads`). It relies on a gRPC
interceptor pattern to transparently upload oversized payloads to Azure Blob
Storage and replace them with opaque tokens before they reach the wire.

This document evaluates **four packaging and structural approaches** for
bringing the same capability to the Python SDK, weighing developer
experience, dependency hygiene, cross-SDK interoperability, and
maintainability.

---

## 2. Shared Technical Requirements (All Approaches)

Regardless of the packaging model, the implementation needs these pieces:

### 2.1 Core Abstractions (belong in `durabletask` core)

| Component | Description |
|---|---|
| `PayloadStore` (ABC) | Abstract base class with `upload(payload) -> token`, `download(token) -> payload`, `is_known_token(value) -> bool` |
| `LargePayloadOptions` (dataclass) | Threshold bytes (default 900KB), max payload bytes (default 10MB), compression flag, container name |
| Capability advertisement | Worker sets `capabilities=[WORKER_CAPABILITY_LARGE_PAYLOADS]` on `GetWorkItemsRequest` when a payload store is configured |

### 2.2 Blob Storage Implementation

| Component | Description |
|---|---|
| `BlobPayloadStore(PayloadStore)` | Azure Blob implementation using `azure-storage-blob`; GZip compression; token format `blob:v1:<container>:<name>` for cross-SDK compatibility |

### 2.3 gRPC Interceptor

| Component | Description |
|---|---|
| `PayloadExternalizationInterceptor` | Sync + async gRPC interceptor that walks protobuf message fields, calling `MaybeExternalize` on outbound payloads and `MaybeResolve` on inbound payloads |

The interceptor needs to handle both `UnaryUnary` calls (most client
operations) and `UnaryStream` calls (the `GetWorkItems` streaming RPC used
by workers). The Python SDK already supports passing custom interceptors
to both `TaskHubGrpcWorker` and `TaskHubGrpcClient` via their `interceptors`
constructor parameter.

### 2.4 Proto Field Coverage

The interceptor must handle the same protobuf message fields as the .NET
`AzureBlobPayloadsSideCarInterceptor`. At minimum:

**Outbound (externalize):** `CreateInstanceRequest.input`,
`RaiseEventRequest.input`, `ActivityResponse.result`,
`OrchestratorResponse` action inputs/outputs,
`EntityBatchResult`/`EntityBatchRequest` state and operation fields.

**Inbound (resolve):** `GetInstanceResponse` state fields,
`WorkItem` activity/orchestration/entity payloads, all `HistoryEvent`
subtypes with data fields, `QueryInstancesResponse`,
`GetEntityResponse`/`QueryEntitiesResponse`.

---

## 3. Approach A — Separate Python Package

> *Mirrors the .NET model exactly.*

### Structure

```text
durabletask-python/
├── durabletask/                          # core SDK (existing)
│   ├── internal/
│   │   ├── payload_store.py              # PayloadStore ABC (NEW)
│   │   └── ...
│   └── ...
├── durabletask-azuremanaged/             # Azure managed provider (existing)
├── durabletask-azureblobpayloads/        # NEW separate package
│   ├── pyproject.toml                    # depends on durabletask, azure-storage-blob
│   ├── durabletask/
│   │   └── azureblobpayloads/
│   │       ├── __init__.py
│   │       ├── blob_payload_store.py     # BlobPayloadStore implementation
│   │       ├── interceptor.py            # PayloadExternalizationInterceptor
│   │       └── options.py                # LargePayloadOptions
│   └── ...
└── ...
```

### pyproject.toml (new package)

```toml
[project]
name = "durabletask.azureblobpayloads"
dependencies = [
    "durabletask>=1.4.0",
    "azure-storage-blob>=12.0.0",
]
```

### Usage

```python
from durabletask.azureblobpayloads import BlobPayloadStore, LargePayloadOptions

options = LargePayloadOptions(
    connection_string="DefaultEndpointsProtocol=https;...",
    threshold_bytes=500_000,
    compress=True,
)
store = BlobPayloadStore(options)
interceptor = store.create_interceptor()   # returns configured gRPC interceptor

worker = TaskHubGrpcWorker(
    host_address="localhost:4001",
    interceptors=[interceptor],
)
```

### Pros

- Zero impact on core package size and deps.
- Matches the .NET precedent.
- Follows the existing pattern in this repo (`durabletask-azuremanaged`).
- Independent release cadence.

### Cons

- Extra package to install.
- Discoverability — users may not find the feature without searching.
- Worker capability advertisement — the core SDK's worker needs changes to
  accept a payload store or flag and set `WORKER_CAPABILITY_LARGE_PAYLOADS`.
- Duplicated interceptor chain if also using `durabletask-azuremanaged`.

### Verdict

**Recommended for most scenarios.**

---

## 4. Approach B — Optional Extras in Core (`pip install durabletask[blobpayloads]`)

> *Single package, optional dependency group.*

### Structure

```text
durabletask/
├── __init__.py
├── client.py
├── worker.py
├── internal/
│   ├── payload_store.py              # PayloadStore ABC
│   └── ...
├── blobpayloads/                     # NEW subpackage (guarded import)
│   ├── __init__.py
│   ├── blob_payload_store.py
│   ├── interceptor.py
│   └── options.py
└── ...
```

### pyproject.toml changes (core)

```toml
[project.optional-dependencies]
opentelemetry = [
    "opentelemetry-api>=1.0.0",
    "opentelemetry-sdk>=1.0.0",
]
blobpayloads = [
    "azure-storage-blob>=12.0.0",
]
```

### Usage

```python
# pip install durabletask[blobpayloads]
from durabletask.blobpayloads import BlobPayloadStore, LargePayloadOptions

options = LargePayloadOptions(connection_string="...")
store = BlobPayloadStore(options)
interceptor = store.create_interceptor()

worker = TaskHubGrpcWorker(interceptors=[interceptor])
```

### Import Guard Pattern

The submodule uses a try/except pattern identical to the existing
OpenTelemetry integration in `durabletask/internal/tracing.py`:

```python
# durabletask/blobpayloads/__init__.py
try:
    from azure.storage.blobs import BlobServiceClient  # noqa: F401
except ImportError:
    raise ImportError(
        "The 'azure-storage-blob' package is required for large payload "
        "support. Install it with: pip install durabletask[blobpayloads]"
    )
```

### Pros

- Single `pip install` command with extras tag.
- Better discoverability than separate package.
- Follows the existing OpenTelemetry pattern in the SDK.
- No cross-package version sync.

### Cons

- Blob storage code ships inside core even when unused.
- Tight coupling to core release cadence.
- Azure-specific code inside the core package (philosophical departure).
- Doesn't match .NET precedent.

### Verdict

**Good alternative if discoverability and simplicity are priorities.**

---

## 5. Approach C — On by Default (All Dependencies Included)

> *Always installed, always configured, zero opt-in.*

### pyproject.toml

```toml
[project]
dependencies = [
    "grpcio",
    "protobuf",
    "asyncio",
    "packaging",
    "azure-storage-blob>=12.0.0",    # always installed
]
```

### Deeper Integration Variant

This approach also opens the door to building externalization directly into
the worker and client constructors:

```python
class TaskHubGrpcWorker:
    def __init__(
        self,
        *,
        host_address: Optional[str] = None,
        # ... existing params ...
        large_payload_options: Optional[LargePayloadOptions] = None,  # NEW
    ):
        if large_payload_options:
            store = BlobPayloadStore(large_payload_options)
            payload_interceptor = store.create_interceptor()
            # Prepend to interceptor chain automatically
```

### Pros

- Zero configuration beyond providing credentials.
- Feature always available — easier to document.

### Cons

- Forces `azure-storage-blob` + entire Azure dependency tree on every user.
  This pulls in `azure-core`, `azure-identity` (transitively),
  `cryptography`, `cffi`, `typing-extensions`, and more.
- Breaks the zero-Azure-dependency model of the core package. Currently even
  `azure-identity` is only required by `durabletask-azuremanaged`.
- Not every deployment uses Azure Blob Storage. Future payload stores (S3,
  GCS, local disk) would each add their own dependencies.
- Doesn't match .NET precedent at all.

### Verdict

**Not recommended.** Dependency cost too high for a feature not all users
need.

---

## 6. Approach D — Hybrid: ABC in Core + Separate Azure Package

> *Maximum flexibility, maximum packages.*

### Structure

```text
durabletask-python/
├── durabletask/                          # core SDK
│   ├── internal/
│   │   └── payload_store.py              # PayloadStore ABC + LargePayloadOptions
│   └── ...
├── durabletask-azuremanaged/             # Azure managed provider (existing)
├── durabletask-azureblobpayloads/        # Azure Blob implementation (separate pkg)
│   └── ...
```

The core SDK exposes the ABC and options classes. The worker/client accept
a `PayloadStore` instance directly:

```python
worker = TaskHubGrpcWorker(
    payload_store=my_store_instance,      # Any PayloadStore subclass
)
```

This enables users or third parties to implement alternative stores without
any Azure dependency.

### Pros

- Most extensible. Any storage backend (S3, GCS, Redis, local) can implement
  `PayloadStore`.
- Core stays dependency-free. ABC + dataclass cost nothing.
- Azure Blob support is a first-party separate package but not the only
  option.

### Cons

- Three packages to think about (core + managed + blob payloads).
- Over-engineered for current needs. No known demand for non-Azure payload
  stores.
- ABC in core may be premature. If the `PayloadStore` interface needs to
  change, it's a core SDK breaking change.

### Verdict

**Consider for the long term, but likely premature today.** Refactoring from
Approach A to D is straightforward if needed.

---

## 7. Comparison Matrix

| Criteria | A: Separate Pkg | B: Optional Extras | C: On by Default | D: Hybrid |
|---|---|---|---|---|
| Core dep impact | None | None (optional) | Heavy | None |
| Matches .NET | Yes | No | No | Partially |
| Discoverability | Low | Medium | High | Low |
| Install cmd | `pip install durabletask.azureblobpayloads` | `pip install durabletask[blobpayloads]` | `pip install durabletask` | `pip install durabletask.azureblobpayloads` |
| Repo pattern match | Yes (`azuremanaged`) | Yes (`opentelemetry`) | No | Partially |
| Release coupling | Independent | Coupled | Coupled | Independent |
| Extensibility | Moderate | Low | Low | High |
| Risk to users | None | None | Dep bloat | None |

---

## 8. Recommendation

**Approach A (Separate Package)** is the recommended path:

1. **Proven pattern.** The repo already uses this model for
   `durabletask-azuremanaged`, and the .NET SDK uses it for the same feature.
2. **Zero risk to existing users.** No new dependencies, no import changes,
   no behavior changes for anyone who doesn't opt in.
3. **Clean boundaries.** Azure-specific storage code stays outside the core
   SDK.
4. **Future-proof.** If a non-Azure store is needed later, Approach A
   naturally evolves into Approach D by extracting the ABC to core.

However, the `PayloadStore` ABC and the `LargePayloadOptions` dataclass
should live in core from day one (even if only used by the external
package), because:

- The worker needs to accept a store to set capability flags.
- The interceptor-based architecture requires the store interface to be
  importable without the Azure dependency.

This is a minimal addition to core: one file with an ABC, a dataclass, and
no new dependencies.

### Suggested Implementation Order

1. **Core SDK changes (`durabletask`):**
   - Add `PayloadStore` ABC and `LargePayloadOptions` dataclass to
     `durabletask/internal/`.
   - Modify `TaskHubGrpcWorker` to accept an optional `payload_store`
     parameter and set `WORKER_CAPABILITY_LARGE_PAYLOADS` on
     `GetWorkItemsRequest` when present.
   - Export `PayloadStore` and `LargePayloadOptions` from the public API.

2. **New package (`durabletask-azureblobpayloads`):**
   - `BlobPayloadStore(PayloadStore)` using `azure-storage-blob`.
   - `PayloadExternalizationInterceptor` (sync + async gRPC interceptors).
   - Helper to wire interceptor + store to worker/client in one call.

3. **Integration with `durabletask-azuremanaged`:**
   - Provide a convenience method or docs showing how to combine the DTS
     auth interceptor with the payload interceptor.

4. **Testing:**
   - Unit tests with a mock payload store.
   - Integration tests hitting Azurite (Azure Storage emulator).

5. **Documentation and examples.**

---

## 9. Open Questions

1. **Should `durabletask-azuremanaged` automatically wire up blob payloads?**
   This would simplify the DTS user experience but tightly couples the two
   concerns.

2. **Should the worker auto-create the blob container on first use (as .NET
   does), or require pre-provisioning?** Auto-creation is convenient but
   requires write permissions beyond data upload.

3. **What is the default threshold?** The .NET SDK uses 900KB (just under
   the 1MiB gRPC default limit). The same default is recommended for
   interop, but should be validated against the Python gRPC library's
   default message size limit.

4. **Should compression be opt-in or opt-out?** .NET defaults to compression
   on (GZip). Matching this default ensures cross-SDK blob compatibility.

5. **Token format interoperability.** If a .NET worker externalizes a payload
   and a Python worker processes the orchestration (or vice versa), both SDKs
   must understand each other's tokens. Using the same
   `blob:v1:<container>:<name>` format is essential.
