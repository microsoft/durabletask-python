# Durable Task Scheduler sandbox activities sample

This sample mirrors the .NET sandbox sample with three customer-owned pieces:

1. A **declarer app** (`main_app.py`) that declares which activity should run
  in a sandbox, starts the orchestration, and waits for the result.
2. A **remote worker image** (`remote_worker.py` plus `Containerfile`) that
   Durable Task Scheduler starts in a sandbox to execute the declared activity.
3. A tiny shared module (`activity_names.py`) that keeps the declarer and remote
   worker on the same activity name constants.

> [!NOTE]
> Until the sandbox extension is published in a preview package, the worker
> image installs the SDK from this source tree. After publication, replace that
> Containerfile step with `pip install durabletask.azuremanaged==<preview-version>`.

## Environment variables

Set these before running the declarer app:

Bash:

~~~bash
export DTS_ENDPOINT="<scheduler endpoint>"
export DTS_TASK_HUB="<task hub name>"
export DTS_WORKER_PROFILE_ID="default"
export DTS_SANDBOX_CONTAINER_IMAGE="<container image reference>"
export DTS_SANDBOX_IMAGE_PULL_UMI_CLIENT_ID="<image-pull UMI client ID>"
export DTS_SANDBOX_SCHEDULER_UMI_CLIENT_ID="<scheduler UMI client ID>"
~~~

PowerShell:

~~~powershell
$env:DTS_ENDPOINT = "<scheduler endpoint>"
$env:DTS_TASK_HUB = "<task hub name>"
$env:DTS_WORKER_PROFILE_ID = "default"
$env:DTS_SANDBOX_CONTAINER_IMAGE = "<container image reference>"
$env:DTS_SANDBOX_IMAGE_PULL_UMI_CLIENT_ID = "<image-pull UMI client ID>"
$env:DTS_SANDBOX_SCHEDULER_UMI_CLIENT_ID = "<scheduler UMI client ID>"
~~~

After pushing the remote worker image, set `DTS_SANDBOX_CONTAINER_IMAGE` to
the pushed image reference. `RemoteWorkerProfile.configure()` declares CPU,
memory, max concurrency, customer environment variables, and sandbox activity
names with `options.add_activity(...)`. The declarer and remote worker both use
`activity_names.py` so they stay in sync.

The remote worker code cannot pass Durable Task Scheduler runtime settings to the SDK. In a
sandbox, `SandboxWorker()` reads `DTS_ENDPOINT`,
`DTS_TASK_HUB`, `DTS_WORKER_PROFILE_ID`, `DTS_SANDBOX_MAX_ACTIVITIES`,
`DTS_SANDBOX_PROVIDER`, and `DTS_SANDBOX_ID` from environment variables injected by
Durable Task Scheduler. The worker reports its registered activity names when it connects, and
Durable Task Scheduler validates they match the declaration before advertising worker capacity.

## Build the remote worker image

From the repository root:

Bash:

~~~bash
docker build \
  -f examples/sandboxes/Containerfile \
  -t <public container image reference> \
  .
docker push <public container image reference>
~~~

PowerShell:

~~~powershell
docker build `
  -f examples\sandboxes\Containerfile `
  -t <public container image reference> `
  .
docker push <public container image reference>
~~~

Private preview requires the image to be publicly pullable by the sandbox platform.

## Run the declarer app

Install local packages from the repository root:

Bash:

~~~bash
pip install -e . -e ./durabletask-azuremanaged
~~~

PowerShell:

~~~powershell
pip install -e . -e .\durabletask-azuremanaged
~~~

Then run:

Bash:

~~~bash
python examples/sandboxes/main_app.py
~~~

PowerShell:

~~~powershell
python examples\sandboxes\main_app.py
~~~

The declarer app registers the sandbox activity metadata, starts
`hello_orchestrator`, and the remote worker sandbox executes `remote_hello`.
The result includes `SANDBOX_SAMPLE_MARKER=sandboxes-python-sample-marker`,
proving the customer environment variable declared on the worker profile reached
the sandbox.
