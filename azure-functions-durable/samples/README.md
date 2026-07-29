# Azure Functions Durable 2.x samples

These samples use the Azure Functions Python V2 programming model and the
durabletask-native APIs provided by `azure-functions-durable` 2.x.

| Sample | Demonstrates |
| --- | --- |
| [Function chaining](function-chaining/) | Calling activities in sequence |
| [Fan-out/fan-in](fan-out-fan-in/) | Running activities in parallel and aggregating their results |
| [Human interaction](human-interaction/) | Waiting for an external event with a durable timeout |
| [Durable entities](durable-entities/) | Defining and calling a class-based entity |

## Prerequisites

- Python 3.13 or later
- [Azure Functions Core Tools 4.x](https://learn.microsoft.com/azure/azure-functions/functions-run-local)
- [Azurite](https://learn.microsoft.com/azure/storage/common/storage-use-azurite)

The samples use the preview Azure Functions extension bundle required by the
Durable Functions 2.x provider. Their `local.settings.json` files connect to
Azurite.

## Run a sample

Start Azurite, then create a virtual environment and install the selected
sample's dependencies.

### Bash

```bash
cd azure-functions-durable/samples/function-chaining
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
func start
```

### PowerShell

```powershell
Set-Location azure-functions-durable\samples\function-chaining
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
func start
```

Replace `function-chaining` with the directory for another sample.

## Invoke the samples

### Function chaining

```bash
curl -i -X POST http://localhost:7071/api/orchestrators/hello_cities
```

```powershell
Invoke-WebRequest `
  -Method Post `
  -Uri http://localhost:7071/api/orchestrators/hello_cities
```

### Fan-out/fan-in

```bash
curl -i -X POST http://localhost:7071/api/fan-out-fan-in \
  -H "Content-Type: application/json" \
  -d '[1, 2, 3, 4, 5]'
```

```powershell
Invoke-WebRequest `
  -Method Post `
  -Uri http://localhost:7071/api/fan-out-fan-in `
  -ContentType application/json `
  -Body '[1, 2, 3, 4, 5]'
```

### Human interaction

Start the orchestration and copy its instance ID from the response:

```bash
curl -i -X POST http://localhost:7071/api/approvals
```

```powershell
Invoke-WebRequest -Method Post -Uri http://localhost:7071/api/approvals
```

Send an approval before the one-minute timeout:

```bash
curl -i -X POST http://localhost:7071/api/approvals/INSTANCE_ID \
  -H "Content-Type: application/json" \
  -d '{"approved": true}'
```

```powershell
Invoke-WebRequest `
  -Method Post `
  -Uri http://localhost:7071/api/approvals/INSTANCE_ID `
  -ContentType application/json `
  -Body '{"approved": true}'
```

### Durable entities

Start an orchestration that adds five to a counter entity and returns its new
value:

```bash
curl -i -X POST http://localhost:7071/api/counters/my-counter \
  -H "Content-Type: application/json" \
  -d '{"amount": 5}'
```

```powershell
Invoke-WebRequest `
  -Method Post `
  -Uri http://localhost:7071/api/counters/my-counter `
  -ContentType application/json `
  -Body '{"amount": 5}'
```

Each starter returns a standard Durable Functions HTTP management payload. Use
its `statusQueryGetUri` to inspect the orchestration result.
