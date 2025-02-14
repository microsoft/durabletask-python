# Examples

This directory contains examples of how to author durable orchestrations using the Durable Task Python SDK in conjunction with the Durable Task Scheduler (DTS).

## Prerequisites

All the examples assume that you have a Durable Task Scheduler taskhub created.

The simplest way to create a taskhub is by using the az cli commands:

1. Create a scheduler:
    az durabletask scheduler create --resource-group <testrg> --name <testscheduler> --location <eastus> --ip-allowlist "[0.0.0.0/0]" --sku-capacity 1 --sku-name "Dedicated" --tags "{}"

1. Create your taskhub

    ```bash
    az durabletask taskhub create --resource-group <testrg> --scheduler-name <testscheduler> --name <testtaskhub>
    ```

1. Retrieve the endpoint for the scheduler. This can be done by locating the taskhub in the portal.

1. Set the appropriate environment variables for the TASKHUB and ENDPOINT

    ```bash
    export TASKHUB=<taskhubname>
    export ENDPOINT=<taskhubEndpoint>
    ```

1. Since the samples rely on azure identity, ensure the package is installed and up-to-date

    ```bash
    python3 -m pip install azure-identity
    ```

1. Install the correct packages from the top level of this repository, i.e. durabletask-python/

    ```bash
    python3 -m pip install .
    ```

1. Install the DTS specific packages from the durabletask-python/durabletask-azuremanaged directory

    ```bash
    pip3 install -e .
    ```

1. Grant yourself the `Durable Task Data Contributor` role over your scheduler

## Running the examples

Now, you can simply execute any of the examples in this directory using `python3`:

```sh
python3 dts_activity_sequence.py
```
