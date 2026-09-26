# Getting Started

## Configure Azure Managed Authentication

Pass an Azure `TokenCredential` to the Azure Managed client or worker. Use
`resource_id` to override the resource audience for token requests:

```python
from azure.identity import AzureAuthorityHosts, DefaultAzureCredential
from durabletask.azuremanaged import DurableTaskSchedulerClient

credential = DefaultAzureCredential(
    authority=AzureAuthorityHosts.AZURE_GOVERNMENT,
)

client = DurableTaskSchedulerClient(
    host_address="https://myaccount.usgovvirginia.durabletask.azure.us",
    taskhub="my-task-hub",
    token_credential=credential,
    resource_id="https://durabletask.azure.us",
)
```

The same `resource_id` parameter is available on `DurableTaskSchedulerWorker`,
`AsyncDurableTaskSchedulerClient`, and the preview `SandboxActivitiesClient` and
`SandboxWorker`. The async client requires an async credential, such as
`azure.identity.aio.DefaultAzureCredential`. Sandbox workers continue to use
their runtime-injected endpoint and managed identity.

An explicit resource ID takes precedence over `REGION_NAME`. If `resource_id`
is `None` or `""`, the SDK resolves the default when the client or worker is
constructed:

| `REGION_NAME` | Default resource ID |
| --- | --- |
| Starts with `usgov` or `usdod` (case-insensitive) | `https://durabletask.azure.us` |
| Any other value, empty, or unset | `https://durabletask.io` |

For example, to select the government default without passing `resource_id`:

Bash:

```bash
export REGION_NAME=usgovvirginia
```

PowerShell:

```powershell
$env:REGION_NAME = "usgovvirginia"
```

The SDK trims surrounding whitespace and trailing slashes, removes an existing
`/.default` suffix (case-insensitive), and appends `/.default` for the token
request. For example, both `https://durabletask.azure.us/` and
`https://durabletask.azure.us/.default` request
`https://durabletask.azure.us/.default`. Nonempty inputs that become empty after
normalization, such as whitespace, `///`, or `/.default`, raise `ValueError`.

### Credential Ownership and Authority Host

`DurableTaskSchedulerClient`, `AsyncDurableTaskSchedulerClient`,
`SandboxActivitiesClient`, and `DurableTaskSchedulerWorker` require a
`token_credential` argument. They use the caller-created credential directly;
they do not construct `DefaultAzureCredential` or another credential on the
caller's behalf. Passing `None` disables SDK token authentication rather than
creating a default credential. A caller-supplied channel remains responsible
for its own authentication.

Credentials may come from Azure Identity or implement the Azure Core
`TokenCredential` / `AsyncTokenCredential` protocol themselves. Token requests
use `get_token(scope)`; the protocol has no supported per-request authority
override. Configure `authority` when creating a compatible Azure Identity
credential, as in the example above. Omitting it preserves the credential's
default behavior, including `AZURE_AUTHORITY_HOST` where applicable.

The preview `SandboxWorker` is the only Azure Managed runtime path that creates
an Azure Identity credential internally. It creates `ManagedIdentityCredential`
using the injected `DTS_UMI_CLIENT_ID`. Managed identities use their hosting
environment's identity endpoint and ignore the authority setting, so this path
does not require an authority-host option either.

> [!NOTE]
> The resource audience, service endpoint, and credential authority/cloud are
> separate settings. Neither `resource_id` nor `REGION_NAME` changes the endpoint
> or the credential's authority. Configure the credential and any underlying
> developer tools for the target cloud separately. Other clouds or custom
> audiences require an explicit resource ID; they are not inferred from the endpoint.

## Run the Order Processing Example

- Check out the [Durable Task Scheduler
  example](../examples/dts/sub-orchestrations-with-fan-out-fan-in/README.md)
 for detailed instructions on running the order processing example.

## Explore Other Samples

- Visit the [examples](../examples/dts/) directory to find a variety of sample orchestrations and
  learn how to run them.
