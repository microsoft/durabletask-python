# Large Payload / External Payload Storage: .NET SDK vs Python SDK

## 1. Executive Summary

The **durabletask-dotnet** SDK (v1.16.2+) provides a fully implemented
**large payload externalization** feature via the
`Microsoft.DurableTask.Extensions.AzureBlobPayloads` extension package.
This feature transparently offloads payloads exceeding a configurable size
threshold to Azure Blob Storage, replacing them with opaque reference tokens
in gRPC messages.

The **durabletask-python** SDK has **no implementation** of this feature.
While the protobuf definitions include a `WORKER_CAPABILITY_LARGE_PAYLOADS`
capability enum value, the Python SDK neither advertises that capability nor
includes any payload externalization logic.

---

## 2. Feature Overview (.NET SDK)

### 2.1 Problem Statement

Durable Task orchestrations pass data between activities, sub-orchestrations,
and external events via serialized payloads embedded in gRPC protobuf
messages. When these payloads grow large (e.g., multi-megabyte JSON objects),
they can:

- Exceed gRPC message size limits
- Increase memory pressure on workers and the sidecar/backend
- Slow down orchestration replay due to large history sizes
- Raise storage costs when the backend persists history inline

### 2.2 Solution Architecture (.NET)

The .NET SDK solves this with a **gRPC interceptor + pluggable payload store**
pattern, delivered as a separate NuGet extension package
(`Microsoft.DurableTask.Extensions.AzureBlobPayloads`).

```text
                     Externalize (Upload)
                    ┌──────────────────────┐
                    │                      ▼
┌─────────┐   ┌────┴───────────┐   ┌──────────────┐   ┌──────────────┐
│  Worker  │──▶│ gRPC Interceptor │──▶│  Sidecar /   │──▶│   Backend    │
│  /Client │   │ (Externalize /  │   │    DTS       │   │   Storage    │
│          │◀──│  Resolve)       │◀──│              │◀──│              │
└─────────┘   └────┬───────────┘   └──────────────┘   └──────────────┘
                    │                      ▲
                    └──────────────────────┘
                      Resolve (Download)
                              │
                    ┌─────────▼─────────┐
                    │  Azure Blob Store  │
                    │  (PayloadStore)    │
                    └───────────────────┘
```

**Key flow:**

1. **Outbound (Externalize):** Before a gRPC request leaves the SDK, the
   interceptor scans protobuf message fields for payloads exceeding the
   threshold. Oversized payloads are uploaded to Azure Blob Storage and
   replaced with an opaque token (e.g., `blob:v1:<container>:<blobName>`).
2. **Inbound (Resolve):** When a gRPC response arrives, the interceptor
   detects any known payload tokens and downloads/rehydrates the original
   payload from blob storage before the SDK processes the message.

### 2.3 Key Components (.NET)

| Component | Location | Purpose |
|---|---|---|
| `LargePayloadStorageOptions` | `src/Extensions/AzureBlobPayloads/Options/` | Configuration: threshold (default 900KB), max size (10MB), container name, auth, compression |
| `PayloadStore` (abstract) | `src/Extensions/AzureBlobPayloads/PayloadStore/` | Abstract interface: `UploadAsync`, `DownloadAsync`, `IsKnownPayloadToken` |
| `BlobPayloadStore` | `src/Extensions/AzureBlobPayloads/PayloadStore/` | Azure Blob Storage implementation with GZip compression, retry policies |
| `PayloadInterceptor<T1,T2>` (abstract) | `src/Extensions/AzureBlobPayloads/Interceptors/` | Base gRPC interceptor with `MaybeExternalizeAsync` / `MaybeResolveAsync` helpers |
| `AzureBlobPayloadsSideCarInterceptor` | `src/Extensions/AzureBlobPayloads/Interceptors/` | Concrete interceptor mapping all protobuf message types to externalize/resolve |
| DI Extensions | `src/Extensions/AzureBlobPayloads/DependencyInjection/` | `UseExternalizedPayloads()` on client and worker builders; `AddExternalizedPayloadStore()` on service collection |

### 2.4 Configuration Options (.NET)

```csharp
public sealed class LargePayloadStorageOptions
{
    // Payloads >= this size are externalized. Default: 900,000 bytes.
    // Max: 1 MiB (1,048,576 bytes).
    public int ExternalizeThresholdBytes { get; set; } = 900_000;

    // Hard cap for any single payload. Default: 10 MB.
    public int MaxExternalizedPayloadBytes { get; set; } = 10 * 1024 * 1024;

    // Azure Storage connection string (option 1)
    public string ConnectionString { get; set; }

    // Azure Storage Account URI + TokenCredential (option 2)
    public Uri? AccountUri { get; set; }
    public TokenCredential? Credential { get; set; }

    // Blob container name. Default: "durabletask-payloads"
    public string ContainerName { get; set; } = "durabletask-payloads";

    // GZip compress payloads before upload. Default: true
    public bool CompressPayloads { get; set; } = true;
}
```

### 2.5 Usage Example (.NET)

```csharp
// Host setup
builder.Services.AddExternalizedPayloadStore(options =>
{
    options.ConnectionString = "DefaultEndpointsProtocol=https;...";
    options.ExternalizeThresholdBytes = 500_000;
    options.CompressPayloads = true;
});

builder.Services.AddDurableTaskWorker(b =>
{
    b.UseGrpc();
    b.UseExternalizedPayloads();  // Hooks up the interceptor
    b.AddTasks(r => r.AddOrchestrator<MyOrchestrator>());
});

builder.Services.AddDurableTaskClient(b =>
{
    b.UseGrpc();
    b.UseExternalizedPayloads();  // Same for client
});
```

### 2.6 Protobuf Messages Handled

The `AzureBlobPayloadsSideCarInterceptor` externalizes/resolves payloads in
the following protobuf message types:

**Outbound (Request → Externalize):**

- `CreateInstanceRequest.Input`
- `RaiseEventRequest.Input`
- `TerminateRequest.Output`
- `SuspendRequest.Reason` / `ResumeRequest.Reason`
- `SignalEntityRequest.Input`
- `ActivityResponse.Result`
- `OrchestratorResponse` (custom status, action inputs/outputs for
  `CompleteOrchestration`, `ScheduleTask`, `CreateSubOrchestration`,
  `SendEvent`, `SendEntityMessage`)
- `EntityBatchResult` (entity state, operation results, action inputs)
- `EntityBatchRequest` (entity state, operation inputs)

**Inbound (Response → Resolve):**

- `GetInstanceResponse.OrchestrationState` (input, output, custom status)
- `QueryInstancesResponse` (same fields per instance)
- `GetEntityResponse` / `QueryEntitiesResponse` (serialized state)
- `WorkItem` (activity input, orchestration history events, entity state)
- All `HistoryEvent` subtypes (ExecutionStarted, TaskCompleted,
  SubOrchestrationInstanceCompleted, EventRaised, ContinueAsNew, etc.)

### 2.7 Token Format

Externalized payloads are referenced by tokens with the format:

```text
blob:v1:<containerName>:<guidBlobName>
```

The `IsKnownPayloadToken` method recognizes tokens starting with `blob:v1:`.

### 2.8 Worker Capability Advertisement

When externalized payloads are enabled, the worker announces
`WorkerCapability.LargePayloads` in the `GetWorkItemsRequest.capabilities`
field. This tells the backend/sidecar that the worker can handle externalized
references.

---

## 3. Current State (Python SDK)

### 3.1 What Exists

| Area | Status | Details |
|---|---|---|
| Protobuf capability enum | Defined | `WORKER_CAPABILITY_LARGE_PAYLOADS = 3` exists in `orchestrator_service_pb2.pyi` |
| Capability advertisement | **Not implemented** | `GetWorkItemsRequest` in `worker.py` never sets the `capabilities` field |
| Payload externalization | **Not implemented** | No logic to detect oversized payloads and offload to external storage |
| Payload store abstraction | **Not implemented** | No equivalent of `PayloadStore` / `BlobPayloadStore` |
| gRPC interceptor for payloads | **Not implemented** | Existing interceptors (`grpc_interceptor.py`) only handle metadata headers |
| Configuration options | **Not implemented** | No threshold, container, or compression settings |
| DataConverter abstraction | **Not implemented** | Serialization is hardcoded via `shared.to_json()` / `shared.from_json()` |
| Compression | **Not implemented** | No GZip or other compression for payloads |
| `fetch_payloads` query parameter | Implemented | Client supports `fetch_payloads` for skipping inline payloads at query time |

### 3.2 Serialization Architecture

The Python SDK uses a fixed JSON serialization path with no abstraction layer:

```text
Python Object
    ↓  shared.to_json() [json.dumps with InternalJSONEncoder]
JSON String
    ↓  Assigned to protobuf StringValue fields
Protobuf Message
    ↓  gRPC Binary Serialization
Wire
```

There is no configurable `DataConverter` equivalent and no interception
point to redirect large payloads to external storage.

### 3.3 gRPC Interceptor Architecture

The Python SDK's gRPC interceptors (`DefaultClientInterceptorImpl` and
`DefaultAsyncClientInterceptorImpl`) are limited to injecting custom
metadata headers. They do not inspect or transform protobuf message bodies.

---

## 4. Gap Analysis

| Capability | .NET SDK | Python SDK | Gap |
|---|---|---|---|
| Payload size threshold detection | ✅ Configurable (default 900KB, max 1MiB) | ❌ None | **Full gap** |
| External blob storage upload/download | ✅ `BlobPayloadStore` with Azure Blob SDK | ❌ None | **Full gap** |
| Pluggable payload store abstraction | ✅ `PayloadStore` abstract class | ❌ None | **Full gap** |
| gRPC interceptor for externalization | ✅ `PayloadInterceptor` + `AzureBlobPayloadsSideCarInterceptor` | ❌ None | **Full gap** |
| Compression (GZip) | ✅ Configurable, default on | ❌ None | **Full gap** |
| Worker capability advertisement | ✅ `WorkerCapability.LargePayloads` | ❌ Proto enum defined but never used | **Full gap** |
| Configurable authentication (conn string / managed identity) | ✅ Both supported | ❌ N/A | **Full gap** |
| Retry policies for blob operations | ✅ Exponential backoff, 8 retries | ❌ N/A | **Full gap** |
| Per-field payload externalization | ✅ Covers all protobuf message input/output fields | ❌ N/A | **Full gap** |
| DI / builder extensions | ✅ `UseExternalizedPayloads()`, `AddExternalizedPayloadStore()` | ❌ N/A | **Full gap** |
| Token format for external references | ✅ `blob:v1:<container>:<blobName>` | ❌ N/A | **Full gap** |
| Pluggable serialization (DataConverter) | ✅ `DataConverter` abstract class | ❌ Hardcoded JSON | **Full gap** |
| Max payload size enforcement | ✅ 10MB default hard cap with clear error | ❌ None | **Full gap** |
| `fetch_payloads` query control | ✅ Supported | ✅ Supported | **Parity** |

---

## 5. Implementation Considerations for the Python SDK

### 5.1 Proposed Architecture

To achieve parity with the .NET SDK, the Python SDK would need the following
components:

```text
┌────────────────────────────┐
│    PayloadStore (ABC)      │   Abstract base with upload/download/is_token
├────────────────────────────┤
│  BlobPayloadStore          │   Azure Blob Storage implementation
│  (azure-storage-blob)     │    - GZip compression
│                            │    - Token encoding (blob:v1:...)
│                            │    - Retry logic
└────────────────────────────┘

┌──────────────────────────────┐
│  PayloadInterceptor          │   gRPC client interceptor
│  (grpc.UnaryUnaryClientI...) │   - Outbound: externalize large fields
│                              │   - Inbound: resolve token references
└──────────────────────────────┘

┌──────────────────────────────┐
│  LargePayloadStorageOptions  │   Configuration dataclass
│                              │   - threshold_bytes (default 900KB)
│                              │   - max_payload_bytes (default 10MB)
│                              │   - container_name
│                              │   - connection_string / credential
│                              │   - compress (default True)
└──────────────────────────────┘
```

### 5.2 Key Implementation Steps

1. **Define `PayloadStore` abstract base class** with `upload`, `download`,
   and `is_known_token` methods.

2. **Implement `BlobPayloadStore`** using `azure-storage-blob` package with
   GZip compression support, exponential retry, and the same token format
   (`blob:v1:<container>:<name>`).

3. **Create a gRPC interceptor** (Python gRPC interceptor API) that wraps
   outbound requests and inbound responses, externalizing/resolving payload
   fields in all relevant protobuf message types.

4. **Advertise `WORKER_CAPABILITY_LARGE_PAYLOADS`** in the
   `GetWorkItemsRequest.capabilities` field when the feature is enabled.

5. **Add `LargePayloadStorageOptions`** as a configuration dataclass with
   threshold, max size, container, and auth settings.

6. **Deliver as a separate package** (e.g., `durabletask-azureblobpayloads`)
   to mirror the .NET extension model and avoid adding `azure-storage-blob`
   as a core dependency.

### 5.3 Packaging Considerations

| Approach | Pros | Cons |
|---|---|---|
| **Separate package** (`durabletask-azureblobpayloads`) | Matches .NET model; no extra deps in core | Extra package to install |
| **Optional extras** (`pip install durabletask[blobpayloads]`) | Single package, optional feature | Harder to separate cleanly |
| **In `durabletask-azuremanaged`** | Natural fit for DTS users | Couples blob storage to managed backend |

### 5.4 Compatibility Notes

- The token format (`blob:v1:...`) should be identical across SDKs so that
  a .NET client can resolve payloads externalized by a Python worker and
  vice versa.
- GZip compression settings should be interoperable.
- The capability advertisement protocol is already defined in the shared
  protobuf schema.

---

## 6. References

- .NET PR: [Large payload azure blob externalization support (#468)](https://github.com/microsoft/durabletask-dotnet/pull/468)
- .NET Changelog entry: v1.16.2 — "Large payload azure blob externalization support"
- .NET Extension package: `Microsoft.DurableTask.Extensions.AzureBlobPayloads`
- Python proto capability: `WORKER_CAPABILITY_LARGE_PAYLOADS = 3`
  in `durabletask/internal/orchestrator_service_pb2.pyi`
