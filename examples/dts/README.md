# Examples

This directory contains examples of how to author durable orchestrations using the Durable Task Python SDK in conjunction with the Durable Task Scheduler (DTS). Please note that the installation instructions provided below will use the version of DTS directly from the your branch rather than installing through PyPI.

## Prerequisites

There are 2 separate ways to run an example:
1. Using the emulator.
2. Using a real scheduler and taskhub.

All the examples by defualt assume that you have a Durable Task Scheduler taskhub created.

## Running with a scheduler and taskhub resource
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

## Running with the emulator
The emulator is a simulation of a scheduler and taskhub. It is the 'backend' of the durabletask-azuremanaged system packaged up into an easy to use docker container. For these steps, it is assumed that you are using port 8080.

In order to use the emulator for the examples, perform the following steps:
1. Install docker if it is not already installed.

2. Pull down the docker image for the emulator:
 `docker pull mcr.microsoft.com/dts/dts-emulator:v0.0.4`

3. Run the emulator and wait a few seconds for the container to be ready:
`docker run --name dtsemulator -d -p 8080:8080 mcr.microsoft.com/dts/dts-emulator:v0.0.4`

4. Set the environment variables that are referenced and used in the examples:
    1. If you are using windows powershell:
    `$env:TASKHUB="default"`
    `$env:ENDPOINT="http://localhost:8080"`
    2. If you are using bash:
    `export TASKHUB=default`
    `export ENDPOINT=http://localhost:8080`

5. Finally, edit the examples to change the `token_credential` input of both the `DurableTaskSchedulerWorker` and `DurableTaskSchedulerClient` to a value of `None` 


## Running the examples

Now, you can simply execute any of the examples in this directory using `python3`:

```sh
python3 dts_activity_sequence.py
```
