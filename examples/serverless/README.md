# DTS serverless activities sample

This sample mirrors the .NET serverless sample with two customer-owned pieces:

1. A **declarer app** (`main_app.py`) that declares which activity should run
   serverlessly, starts the orchestration, and waits for the result.
2. A **remote worker image** (`remote_worker.py` plus `Containerfile`) that
   DTS starts in a sandbox to execute the declared activity.
3. A tiny shared module (`activity_names.py`) that keeps the declarer and remote
   worker on the same activity name constants.

Reference .NET template:
<https://github.com/microsoft/durabletask-dotnet/compare/wangbill/serverless-private-preview>
under `samples/serverless`.

> [!NOTE]
> Until the serverless extension is published in a preview package, the worker
> image installs the SDK from this source tree. After publication, replace that
> Containerfile step with `pip install durabletask.azuremanaged==<preview-version>`.

## Environment variables

Set these before running the declarer app:

```powershell
$env:DTS_ENDPOINT = "<scheduler endpoint>"
$env:DTS_TASK_HUB = "<task hub name>"
$env:DTS_WORKER_PROFILE_ID = "default"
$env:DTS_SERVERLESS_CONTAINER_IMAGE = "<public container image reference>"
```

After pushing the remote worker image, set `DTS_SERVERLESS_CONTAINER_IMAGE` to
the pushed image reference. `RemoteWorkerProfile.configure()` declares CPU,
memory, max concurrency, customer environment variables, and serverless activity
names with `options.add_activity(...)`. The declarer and remote worker both use
`activity_names.py` so they stay in sync.

The remote worker code cannot pass DTS runtime settings to the SDK. In a
sandbox, `ServerlessWorker()` reads `DTS_ENDPOINT`,
`DTS_TASK_HUB`, `DTS_WORKER_PROFILE_ID`, `DTS_SERVERLESS_MAX_ACTIVITIES`,
`DTS_SUBSTRATE`, and `DTS_SANDBOX_ID` from environment variables injected by
DTS. The worker reports its registered activity names when it connects, and
DTS validates they match the declaration before advertising worker capacity.

## Build the remote worker image

From the repository root:

```powershell
docker build `
  -f examples\serverless\Containerfile `
  -t <public container image reference> `
  .
docker push <public container image reference>
```

Private preview requires the image to be publicly pullable by ADC/DTS.

## Run the declarer app

Install local packages from the repository root:

```powershell
pip install -e . -e .\durabletask-azuremanaged
```

Then run:

```powershell
python examples\serverless\main_app.py
```

The declarer app registers the serverless activity metadata, starts
`hello_orchestrator`, and the remote worker sandbox executes `remote_hello`.
The result includes `SERVERLESS_SAMPLE_MARKER=serverless-python-sample-marker`,
proving the customer environment variable declared on the worker profile reached
the sandbox.
