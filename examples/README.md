# Examples

This directory contains examples of how to author durable orchestrations using the Durable Task Python SDK in conjunction with the Durable Task Scheduler (DTS).

## Prerequisites
If using a deployed Durable Task Scheduler:
 - [Azure CLI](https://learn.microsoft.com/cli/azure/install-azure-cli)
 - [`az durabletask` CLI extension](https://learn.microsoft.com/en-us/cli/azure/durabletask?view=azure-cli-latest)

## Running the Examples
There are two separate ways to run an example:

- Using the Emulator (recommended for learning and development)
- Using a deployed Scheduler and Taskhub in Azure 

### Running with the Emulator
We recommend using the emulator for learning and development as it's faster to set up and doesn't require any Azure resources. The emulator simulates a scheduler and taskhub, packaged into an easy-to-use Docker container.

1. Install Docker: If it is not already installed.

2. Pull the Docker Image for the Emulator:
```bash
docker pull mcr.microsoft.com/dts/dts-emulator:v0.0.6
```

3. Run the Emulator: Wait a few seconds for the container to be ready.
```bash
docker run --name dtsemulator -d -p 8080:8080 mcr.microsoft.com/dts/dts-emulator:v0.0.6
```

4. Install the Required Packages:
```bash
pip install -r requirements.txt
```

Note: The example code has been updated to use the default emulator settings automatically (endpoint: http://localhost:8080, taskhub: default). You don't need to set any environment variables.

### Running with a Deployed Scheduler and Taskhub Resource in Azure
For production scenarios or when you're ready to deploy to Azure, you can create a taskhub using the Azure CLI:

1. Create a Scheduler:
```bash
az durabletask scheduler create --resource-group <testrg> --name <testscheduler> --location <eastus> --ip-allowlist "[0.0.0.0/0]" --sku-capacity 1 --sku-name "Dedicated" --tags "{'myattribute':'myvalue'}"
```

2. Create Your Taskhub:
```bash
az durabletask taskhub create --resource-group <testrg> --scheduler-name <testscheduler> --name <testtaskhub>
```

3. Retrieve the Endpoint for the Scheduler: Locate the taskhub in the Azure portal to find the endpoint.

4. Set the Environment Variables:
Bash:
```bash
export TASKHUB=<taskhubname>
export ENDPOINT=<taskhubEndpoint>
```
Powershell:
```powershell
$env:TASKHUB = "<taskhubname>"
$env:ENDPOINT = "<taskhubEndpoint>"
```

5. Install the Required Packages:
```bash
pip install -r requirements.txt
```

### Running the Examples
You can now execute any of the examples in this directory using Python:

```bash
python3 example_file.py
```

### Review Orchestration History and Status in the Durable Task Scheduler Dashboard
To access the Durable Task Scheduler Dashboard, follow these steps:

- **Using the Emulator**: By default, the dashboard runs on portal 8082. Navigate to http://localhost:8082 and click on the default task hub.

- **Using a Deployed Scheduler**: Navigate to the Scheduler resource. Then, go to the Task Hub subresource that you are using and click on the dashboard URL in the top right corner.

```sh
python3 activity_sequence.py
```
