# Examples

This directory contains examples of how to author durable orchestrations using the Durable Task Python SDK in conjunction with the Durable Task Scheduler (DTS).

## Prerequisites

All the examples assume that you have a Durable Task Scheduler taskhub created.

The simplest way to create a taskhub is by using the az cli commands:

1. Create a scheduler:
    az durabletask scheduler create --resource-group <testrg> --name <testscheduler> --location <eastus> --ip-allowlist "[0.0.0.0/0]" --sku-capacity 1 --sku-name "Dedicated" --tags "{}"

2. Create your taskhub
    az durabletask taskhub create --resource-group <testrg> --scheduler-name <testscheduler> --name <testtaskhub>

3. Retrieve the endpoint for the scheduler. This can be done by locating the taskhub in the portal.

4. Set the appropriate environment variables for the TASKHUB and ENDPOINT

```sh
export TASKHUB=<taskhubname>
```

```sh
export ENDPOINT=<taskhubEndpoint>
```

5. Since the samples rely on azure identity, ensure the package is installed and up-to-date

```sh
python3 -m pip install azure-identity
```

6. Install the correct pacakges from the top level of this repository, i.e. durabletask-python/

```sh
python3 -m pip install .
```

7. Install the DTS specific packages from the durabletask-python/durabletask-azuremanaged directory

```sh
pip3 install -e .
```

8. Grant yourself the DurableTaskDataContributor role over your scheduler

## Running the examples

Now, you can simply execute any of the examples in this directory using `python3`:

```sh
python3 dts_activity_sequence.py
```
