# Plan: Python History Export Parity

Add orchestration history export to durabletask-python in two layers: first expose the existing sidecar capabilities that are already present in protobuf but missing from the Python client and test backend; then build a higher-level export job workflow modeled on durabletask-dotnet ExportHistory, with Azure Blob as the first destination via an optional extension. This keeps the core SDK transport-focused while still achieving full feature parity.

## Target Scope

**Full parity path**: core retrieval/list/rewind APIs plus a higher-level job-based export workflow modeled on the .NET implementation.

**Recommended packaging split**: Core SDKowns retrieval/list/rewind and generic history serialization helpers; Azure Blob export workflow lives behind optional dependencies in an extension-style module.

**Initial destination scope**: Azure Blob only, matching durabletask-dotnet's current export package. Do not generalize destination providers until a second real provider exists.

**Initial format scope**: JSON and JSONL gzip with explicit schema versioning. Defer CSV, Parquet, and import/replay-from-export features.

## Phase 1: Core History Foundations

### 1.1 Add Core Client APIs
**Files**: [durabletask/client.py](durabletask/client.py#L213) and [durabletask/client.py](durabletask/client.py#L428)

Add sync and async methods to both `TaskHubGrpcClient` and `AsyncTaskHubGrpcClient`:
- `get_orchestration_history(instance_id) -> Iterable[HistoryEvent]` — stream all HistoryEvent messages for an instance.
- `list_instance_ids(runtime_status, completed_time_from, completed_time_to, page_size) -> Page[List[str]]` — paginate terminal instance IDs by completion-time window and status filter.
- `rewind_orchestration(instance_id, reason) -> None` — rewind a failed orchestration (if backend supports).

Implementation should:
- Reuse the existing gRPC stubs already declared in [durabletask/internal/orchestrator_service_pb2_grpc.py](durabletask/internal/orchestrator_service_pb2_grpc.py).
- Handle gRPC error codes (NOT_FOUND, UNIMPLEMENTED, CANCELLED, INTERNAL) and map to Python exceptions.
- For streamed history, aggregate HistoryChunk messages and yield or return individual HistoryEvent objects.
- De-externalize nested payload tokens if a payload_store is configured (reuse logic from [durabletask/payload/helpers.py](durabletask/payload/helpers.py)).
- Log operations consistently with existing client methods.

### 1.2 Implement In-Memory Backend History Support
**Files**: [durabletask/testing/in_memory_backend.py](durabletask/testing/in_memory_backend.py#L1086)

Implement two currently-stubbed gRPC servicer methods:
- `StreamInstanceHistory(request: StreamInstanceHistoryRequest, context)` — yield HistoryChunk messages containing events from the instance's history list, paginating or chunking as needed.
- `ListInstanceIds(request: ListInstanceIdsRequest, context)` — iterate stored instances, filter by terminal status and completion-time window, and return a paginated response with continuation token.

Optionally decide whether to implement `RewindInstance`:
- **Recommendation**: Mark as explicitly unsupported initially (abort with UNIMPLEMENTED); add later if demand is high or if rewound instances are needed for tests.
- If implemented, reset the instance's history to exclude the failed events and restart with a new execution ID.

### 1.3 Add History Helper Utilities
**Files**: New module `durabletask/internal/history_helpers.py` or extend [durabletask/payload/helpers.py](durabletask/payload/helpers.py)

Provide internal helpers for:
- **Payload de-externalization in history**: walk nested HistoryEvent fields and replace payload tokens with original data if a store is configured.
- **Event-to-dict conversion**: convert a HistoryEvent protobuf to a serializable dict for JSON export (used later).
- **Event filtering**: filter a list of HistoryEvent by type, timestamp range, or other criteria (optional; can be deferred to export layer).

### 1.4 Settle on Public History Return Type
**Decision Point**

Options:
1. **Raw protobuf** (recommended for Phase 1): Return `Iterable[pb.HistoryEvent]` to callers. Low risk of churn, users who need export can call helper utilities. Matches .NET baseline.
2. **Python dataclass wrapper** (higher initial investment): Define an `HistoryEventData` class and convert all HistoryEvent messages to it. Better UX but requires more upfront design.
3. **Both** (post-Phase 1): Start with raw protobuf; add a Python wrapper class in Phase 2 if export code needs the conversion anyway.

**Recommendation**: Start with raw protobuf. Keep the public API minimal and transport-focused. Add serialization helpers (internal) that the export layer can use.

### 1.5 Update Tests
**Files**: [tests/durabletask/test_client.py](tests/durabletask/test_client.py), [tests/durabletask/test_orchestration_executor.py](tests/durabletask/test_orchestration_executor.py), new test file for history retrieval

Add tests for:
- **Client API tests**: Verify `get_orchestration_history()`, `list_instance_ids()` make correct gRPC requests, handle streaming/pagination, de-externalize payloads (reuse FakePayloadStore from large_payload tests), and map errors.
- **Backend tests**: Verify in-memory history streaming returns events in order, ListInstanceIds paginates correctly by status/time, and continuation tokens work.
- **Error handling**: Verify NOT_FOUND, UNIMPLEMENTED, CANCELLED, INTERNAL errors are mapped appropriately.

### 1.6 Update Core Changelog
**Files**: [CHANGELOG.md](CHANGELOG.md#L7)

Under `## Unreleased`, add:
```
ADDED

- Added `get_orchestration_history(instance_id)` and async variant to both gRPC client classes for streaming instance history events.
- Added `list_instance_ids(runtime_status, completed_time_from, completed_time_to, ...)` to support filtering terminal instances by completion time and status. Supports pagination via continuation tokens.
- Added `rewind_orchestration(instance_id, reason)` and async variant for rewinding failed orchestrations (backend support may vary).
- In-memory backend now implements `StreamInstanceHistory` and `ListInstanceIds` gRPC methods for testing.
- Added internal history utility functions for payload de-externalization and event serialization.
```

---

## Phase 2: Export Job Workflow

### 2.1 Package Structure
**New module**: `durabletask/extensions/history_export/` or a new separate package (if isolated from core)

**Recommendation**: Follow the existing extension pattern. Place under `durabletask/extensions/` as a submodule with optional (but recommended) Azure Blob dependencies:

```
durabletask/
└── extensions/
    └── history_export/
        ├── __init__.py
        ├── client.py                    # ExportHistoryClient, ExportHistoryJobClient
        ├── models/
        │   ├── __init__.py
        │   ├── export_job_state.py
        │   ├── export_checkpoint.py
        │   ├── export_destination.py
        │   ├── export_filter.py
        │   ├── export_format.py
        │   ├── export_failure.py
        │   ├── export_job_description.py
        │   ├── export_job_configuration.py
        │   ├── export_job_status.py      # Enum: Active, Completed, Failed
        │   ├── export_mode.py            # Enum: Batch, Continuous
        │   └── export_job_creation_options.py
        ├── entity.py                    # ExportJob durable entity
        ├── serialization.py             # JSON/JSONL serialization logic
        └── orchestrations/
            ├── __init__.py
            ├── export_job_orchestrator.py
            └── activities/
                ├── __init__.py
                ├── list_terminal_instances.py
                ├── export_instance_history.py
                └── helpers.py

tests/
└── durabletask/
    └── extensions/
        └── history_export/
            ├── test_export_client.py
            ├── test_export_models.py
            ├── test_export_entity.py
            ├── test_export_orchestrator.py
            ├── test_export_activities.py
            └── test_serialization.py
```

### 2.2 Models and Data Types
**Files**: `durabletask/extensions/history_export/models/`

Define the following (inspired by durabletask-dotnet ExportHistory models):

```python
# export_mode.py
class ExportMode(Enum):
    BATCH = 1       # Export a fixed time window, then complete
    CONTINUOUS = 2  # Tail terminal instances continuously

# export_format.py
class ExportFormatKind(Enum):
    JSON = 1   # Array of events, uncompressed
    JSONL = 2  # One event per line, gzip compressed

@dataclass
class ExportFormat:
    kind: ExportFormatKind = ExportFormatKind.JSONL
    schema_version: str = "1.0"

# export_destination.py
@dataclass
class ExportDestination:
    container: str           # Azure Blob container name
    prefix: Optional[str] = None  # Optional blob prefix

# export_filter.py
@dataclass
class ExportFilter:
    completed_time_from: datetime     # Inclusive lower bound
    completed_time_to: Optional[datetime] = None  # Inclusive upper bound
    runtime_status: Optional[List[OrchestrationStatus]] = None  # Filter by status

# export_checkpoint.py
@dataclass
class ExportCheckpoint:
    last_instance_key: Optional[str] = None  # Continuation token for ListInstanceIds

# export_failure.py
@dataclass
class ExportFailure:
    instance_id: str
    reason: str
    attempt_count: int
    last_attempt: datetime

# export_job_status.py
class ExportJobStatus(Enum):
    ACTIVE = "Active"
    COMPLETED = "Completed"
    FAILED = "Failed"

# export_job_state.py
@dataclass
class ExportJobState:
    status: ExportJobStatus
    config: Optional['ExportJobConfiguration'] = None
    checkpoint: Optional[ExportCheckpoint] = None
    created_at: Optional[datetime] = None
    last_modified_at: Optional[datetime] = None
    last_checkpoint_time: Optional[datetime] = None
    last_error: Optional[str] = None
    scanned_instances: int = 0
    exported_instances: int = 0
    orchestrator_instance_id: Optional[str] = None

# export_job_configuration.py
@dataclass
class ExportJobConfiguration:
    mode: ExportMode
    filter: ExportFilter
    destination: ExportDestination
    format: ExportFormat
    max_parallel_exports: int = 32
    max_instances_per_batch: int = 100

# export_job_creation_options.py
@dataclass
class ExportJobCreationOptions:
    mode: ExportMode
    completed_time_from: datetime
    completed_time_to: Optional[datetime]  # Required for Batch, None for Continuous
    destination: Optional[ExportDestination]
    job_id: Optional[str] = None
    format: ExportFormat = field(default_factory=lambda: ExportFormat())
    runtime_status: Optional[List[OrchestrationStatus]] = None  # Defaults to terminal statuses
    max_instances_per_batch: int = 100

# export_job_description.py
@dataclass
class ExportJobDescription:
    job_id: str
    status: ExportJobStatus
    created_at: Optional[datetime]
    last_modified_at: Optional[datetime]
    config: Optional[ExportJobConfiguration]
    orchestrator_instance_id: Optional[str]
    scanned_instances: int
    exported_instances: int
    last_error: Optional[str]
    checkpoint: Optional[ExportCheckpoint]
    last_checkpoint_time: Optional[datetime]
```

### 2.3 Durable Entity for Job State
**Files**: `durabletask/extensions/history_export/entity.py`

Implement `ExportJob` as a durable entity with operations:
- `create(context, creation_options: ExportJobCreationOptions)` — initialize job state and validate transitions.
- `get(context, _=None) -> ExportJobState` — fetch current state.
- `run(context, _=None)` — signal to start the export orchestrator.
- `commit_checkpoint(context, request: CommitCheckpointRequest)` — update progress, checkpoint, status.
- `mark_as_completed(context, _=None)` — transition to Completed.
- `mark_as_failed(context, error_message)` — transition to Failed.
- `delete(context, _=None)` — delete the entity.

Include transition validation: define which operations are valid from which states (similar to ExportJobTransitions in .NET).

### 2.4 Export Client
**Files**: `durabletask/extensions/history_export/client.py`

Provide two public classes:

```python
class ExportHistoryClient:
    def __init__(self, durable_task_client: TaskHubGrpcClient, storage_options: ExportHistoryStorageOptions):
        ...
    
    async def create_job_async(self, options: ExportJobCreationOptions) -> ExportHistoryJobClient:
        """Create a new export job."""
        ...
    
    async def get_job_async(self, job_id: str) -> ExportJobDescription:
        """Fetch a job by ID."""
        ...
    
    async def list_jobs_async(self, filter: Optional[ExportJobQuery] = None) -> AsyncIterable[ExportJobDescription]:
        """List all export jobs, optionally filtered."""
        ...
    
    def get_job_client(self, job_id: str) -> ExportHistoryJobClient:
        """Get a client for a specific job."""
        ...

class ExportHistoryJobClient:
    def __init__(self, job_id: str, ...):
        ...
    
    async def create_async(self, options: ExportJobCreationOptions) -> None:
        """Create the export job."""
        ...
    
    async def describe_async(self) -> ExportJobDescription:
        """Get job status."""
        ...
    
    async def delete_async(self) -> None:
        """Delete the job and terminate its orchestrator."""
        ...
```

Implementation:
- Use durable entities via `durable_task_client.signal_entity()` and `get_entity()` to manage job state.
- Wrap the entity ID as `ExportJob@{job_id}`.
- Schedule an orchestrator named `ExportJobOrchestrator` with a fixed instance ID pattern (e.g., `ExportJob-{job_id}`) to ensure one orchestrator per job.

### 2.5 Activities
**Files**: `durabletask/extensions/history_export/orchestrations/activities/`

Implement two activities:

#### 2.5.1 ListTerminalInstancesActivity
Input: `ListTerminalInstancesRequest(completed_time_from, completed_time_to, runtime_status, last_instance_key, max_instances_per_batch)`
Output: `InstancePage(instance_ids: List[str], next_checkpoint: ExportCheckpoint)`

Logic:
- Call the core client's `list_instance_ids()` with the filter parameters.
- Return a page of instance IDs and a checkpoint for the next call.

#### 2.5.2 ExportInstanceHistoryActivity
Input: `ExportRequest(instance_id, destination, format)`
Output: `ExportResult(instance_id, success, error)`

Logic:
- Fetch the instance's history using the core client's `get_orchestration_history()`.
- Fetch metadata using `get_orchestration_state()`.
- Serialize the history and metadata to the specified format (JSON or JSONL gzip).
- Upload to Azure Blob Storage.
- Return success/failure result.

### 2.6 Export Orchestrator
**Files**: `durabletask/extensions/history_export/orchestrations/export_job_orchestrator.py`

Orchestrate the export workflow:

Input: `ExportJobRunRequest(job_entity_id, processed_cycles)`

Logic:
1. Fetch job state from entity. If not Active, exit.
2. Call `ListTerminalInstancesActivity` to get a page of instance IDs.
3. If no instances and mode is Continuous, sleep and retry. If Batch, exit.
4. Call `ExportInstanceHistoryActivity` for each instance in parallel (bounded by `max_parallel_exports`).
5. With exponential backoff, retry failed exports up to 3 times.
6. Commit checkpoint if successful; record failures and stay at current checkpoint if batch fails.
7. If processed_cycles > 5, continue-as-new to reset history and prevent bloat.
8. Mark job as Completed or Failed based on final result.

### 2.7 Serialization
**Files**: `durabletask/extensions/history_export/serialization.py`

Provide functions:

```python
def serialize_history(
    instance_id: str,
    metadata: OrchestrationState,
    history: Iterable[pb.HistoryEvent],
    format: ExportFormat,
) -> bytes:
    """Serialize history and metadata to JSON or JSONL gzip."""
    ...

def event_to_dict(event: pb.HistoryEvent) -> dict:
    """Convert protobuf HistoryEvent to serializable dict."""
    ...
```

Implementation:
- For JSON: return an array of event dicts with metadata.
- For JSONL: return one event per line, gzip compressed.
- Preserve all event fields and handle polymorphic event types (use protobuf reflection or explicit converters).
- Skip internal fields (e.g., timestamps for WorkItem processing).

### 2.8 Azure Blob Storage Upload
**Files**: Same activity file or new `durabletask/extensions/history_export/orchestrations/azure_storage.py`

Use `azure.storage.blob.BlobClient` to upload serialized data:
- Generate a deterministic blob name (e.g., hash of completed time + instance ID).
- Include instance ID as blob metadata.
- Handle connection strings from `ExportHistoryStorageOptions`.

### 2.9 Tests
**Files**: `tests/durabletask/extensions/history_export/`

Add tests for:
- Export client creation, job listing, job description.
- Export job entity lifecycle (create, run, checkpoint, complete, fail, delete).
- Activity logic (ListTerminalInstancesActivity, ExportInstanceHistoryActivity).
- Orchestrator flow (paging, retries, continues-as-new, checkpoint commits).
- Serialization (JSON and JSONL formats, large nested payloads, polymorphic events).
- Azure Blob integration (mocked or using Azure Test Containers if the repo has that pattern).
- Batch vs. Continuous modes.
- Error transitions and recovery.

### 2.10 Update Extension Changelog
**Files**: New `durabletask/extensions/history_export/CHANGELOG.md` or extend core [CHANGELOG.md](CHANGELOG.md)

Document the new export extension, models, and client APIs.

---

## Verification Checklist

### Phase 1
- [ ] Unit tests for `get_orchestration_history()`, `list_instance_ids()`, `rewind_orchestration()` (sync and async).
- [ ] Backend tests for in-memory StreamInstanceHistory and ListInstanceIds.
- [ ] Error mapping tests (NOT_FOUND, UNIMPLEMENTED, CANCELLED, INTERNAL).
- [ ] Payload de-externalization tests (use FakePayloadStore).
- [ ] Pylance diagnostics on modified client and backend files.
- [ ] `flake8` on modified Python files.
- [ ] Targeted pytest for client, backend, and large_payload tests.

### Phase 2
- [ ] Unit tests for all model classes (dataclass validation, enum values).
- [ ] Entity tests (create, get, signal operations, state transitions).
- [ ] Activity tests (ListTerminalInstancesActivity, ExportInstanceHistoryActivity).
- [ ] Orchestrator tests (paging, retries, continues-as-new, checkpointing).
- [ ] Serialization tests (JSON, JSONL gzip, nested payloads, polymorphic events).
- [ ] Integration tests (end-to-end export with mock or test Azure Blob).
- [ ] Batch and Continuous mode tests.
- [ ] Pylance diagnostics on export extension files.
- [ ] `flake8` on all new files.
- [ ] Targeted pytest for export extension tests.
- [ ] Optional: Azure Test Containers integration (if repo has pattern).

---

## Further Considerations

1. **Public history return type**: Keeping raw protobuf HistoryEvent in Phase 1 allows Phase 2 serialization logic to reuse the message introspection without forcing a new public model. Consider wrapping in Phase 2 if users ask.

2. **Export package naming**: Avoid adding many methods directly to the core `TaskHubGrpcClient`. Instead, keep export under a clear namespace (`extensions.history_export` or a separate distribution) to signal that it is optional and not part of the core SDK.

3. **Continuous export semantics**: Follow the .NET pattern closely:
   - Tail terminal instances from a completion-time watermark.
   - Persist checkpoint to entity state to survive orchestrator restarts.
   - Use periodic continue-as-new or equivalent restart behavior if Python orchestrator history gets too large.
   - Sleep between empty pages to avoid busy-waiting.

4. **Dependency management**: The export extension should declare optional Azure Storage dependencies (e.g., `pip install durabletask[history-export]` pulls in `azure-storage-blob`).

5. **Rewind support**: Rewind is a lower-priority feature. Consider leaving it unsupported in the in-memory backend for now and adding it only if users need it or if it's required for export testing.

6. **Future extensions**: Design the destination abstraction (ExportDestination, upload logic) so it can evolve to support other backends (S3, GCS, SFTP, local filesystem) without core changes. For now, ship only Azure Blob.
