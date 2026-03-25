---
description: >-
  Update the protobuf generated Python files from the latest
  microsoft/durabletask-protobuf definitions. Use when the proto definitions
  need to be refreshed or regenerated.
---

# Update Protobuf Definitions

This skill regenerates the Python protobuf files in `durabletask/internal/`
from the latest proto source at
<https://github.com/microsoft/durabletask-protobuf>.

## Prerequisites

- Python 3.11 must be available on the system. Verify with `py -3.11 --version`
  (Windows) or `python3.11 --version` (Linux/macOS). If it is not installed,
  stop and ask the user to install it — do **not** use a different version.
- Internet access is required to download the proto file and query the GitHub
  API.

## Steps

### 1. Set up the `.proto_venv` environment (skip if it already exists)

Check whether `.proto_venv/` already exists at the repo root and whether
`grpcio-tools==1.65.4` is installed in it:

```bash
# Windows
.proto_venv\Scripts\pip.exe list 2>$null | Select-String grpcio-tools

# Bash
.proto_venv/bin/pip list 2>/dev/null | grep grpcio-tools
```

If the venv **does not exist** or `grpcio-tools` is missing, create/recreate it:

```bash
# Windows
py -3.11 -m venv .proto_venv
.proto_venv\Scripts\python.exe -m pip install grpcio-tools==1.65.4

# Bash
python3.11 -m venv .proto_venv
.proto_venv/bin/python -m pip install grpcio-tools==1.65.4
```

> [!NOTE]
> Do **not** delete `.proto_venv` after use. It is reused across runs to avoid
> reinstalling `grpcio-tools` each time. The directory is already in
> `.gitignore`.

### 2. Download the latest proto file

Download from the `main` branch of `microsoft/durabletask-protobuf`:

```bash
# Windows (PowerShell)
Invoke-WebRequest `
  -Uri "https://raw.githubusercontent.com/microsoft/durabletask-protobuf/refs/heads/main/protos/orchestrator_service.proto" `
  -OutFile "durabletask/internal/orchestrator_service.proto"

# Bash
curl -o durabletask/internal/orchestrator_service.proto \
  https://raw.githubusercontent.com/microsoft/durabletask-protobuf/refs/heads/main/protos/orchestrator_service.proto
```

### 3. Regenerate the Python files

Run `grpc_tools.protoc` from the **repo root**:

```bash
# Windows
.proto_venv\Scripts\python.exe -m grpc_tools.protoc --proto_path=. --python_out=. --pyi_out=. --grpc_python_out=. ./durabletask/internal/orchestrator_service.proto

# Bash
.proto_venv/bin/python -m grpc_tools.protoc --proto_path=. --python_out=. --pyi_out=. --grpc_python_out=. ./durabletask/internal/orchestrator_service.proto
```

This produces three files in `durabletask/internal/`:

- `orchestrator_service_pb2.py`
- `orchestrator_service_pb2_grpc.py`
- `orchestrator_service_pb2.pyi`

### 4. Update `PROTO_SOURCE_COMMIT_HASH`

Query the GitHub API for the latest commit that touched the proto file and
**overwrite** `durabletask/internal/PROTO_SOURCE_COMMIT_HASH` with that single
hash:

```bash
# Windows (PowerShell)
$response = Invoke-RestMethod `
  -Uri "https://api.github.com/repos/microsoft/durabletask-protobuf/commits?path=protos/orchestrator_service.proto&sha=main&per_page=1"
$response[0].sha | Out-File -FilePath "durabletask/internal/PROTO_SOURCE_COMMIT_HASH" -NoNewline -Encoding ascii

# Bash
curl -s -H "Accept: application/vnd.github.v3+json" \
  "https://api.github.com/repos/microsoft/durabletask-protobuf/commits?path=protos/orchestrator_service.proto&sha=main&per_page=1" \
  | jq -r '.[0].sha' > durabletask/internal/PROTO_SOURCE_COMMIT_HASH
```

The file should contain exactly one commit hash with no trailing newline.

### 5. Clean up the downloaded proto file

Delete the `.proto` source file — only the generated Python files are kept in
the repo:

```bash
# Windows
Remove-Item durabletask/internal/orchestrator_service.proto

# Bash
rm durabletask/internal/orchestrator_service.proto
```

### 6. Verify

Confirm the generated modules import successfully using the project's main
venv:

```bash
python -c "from durabletask.internal import orchestrator_service_pb2; print('pb2 OK'); from durabletask.internal import orchestrator_service_pb2_grpc; print('pb2_grpc OK')"
```

## Generated files

| File | Description |
|---|---|
| `durabletask/internal/orchestrator_service_pb2.py` | Message classes |
| `durabletask/internal/orchestrator_service_pb2_grpc.py` | gRPC stubs |
| `durabletask/internal/orchestrator_service_pb2.pyi` | Type stubs |
| `durabletask/internal/PROTO_SOURCE_COMMIT_HASH` | Source commit hash |
