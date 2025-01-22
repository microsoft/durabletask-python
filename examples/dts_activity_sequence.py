import os
from azure.identity import DefaultAzureCredential

"""End-to-end sample that demonstrates how to configure an orchestrator
that calls an activity function in a sequence and prints the outputs."""
from durabletask import client, task, worker


def hello(ctx: task.ActivityContext, name: str) -> str:
    """Activity function that returns a greeting"""
    return f'Hello {name}!'


def sequence(ctx: task.OrchestrationContext, _):
    """Orchestrator function that calls the 'hello' activity function in a sequence"""
    # call "hello" activity function in a sequence
    result1 = yield ctx.call_activity(hello, input='Tokyo')
    result2 = yield ctx.call_activity(hello, input='Seattle')
    result3 = yield ctx.call_activity(hello, input='London')

    # return an array of results
    return [result1, result2, result3]


# Read the environment variable
taskhub_name = os.getenv("TASKHUB")

# Check if the variable exists
if taskhub_name:
    print(f"The value of TASKHUB is: {taskhub_name}")
else:
    print("TASKHUB is not set. Please set the TASKHUB environment variable to the name of the taskhub you wish to use")
    print("If you are using windows powershell, run the following: $env:TASKHUB=\"<taskhubname>\"")
    print("If you are using bash, run the following: export TASKHUB=\"<taskhubname>\"")
    exit()

# Read the environment variable
endpoint = os.getenv("ENDPOINT")

# Check if the variable exists
if endpoint:
    print(f"The value of ENDPOINT is: {endpoint}")
else:
    print("ENDPOINT is not set. Please set the ENDPOINT environment variable to the endpoint of the taskhub")
    print("If you are using windows powershell, run the following: $env:ENDPOINT=\"<taskhubEndpoint>\"")
    print("If you are using bash, run the following: export ENDPOINT=\"<taskhubEndpoint>\"")
    exit()


default_credential = DefaultAzureCredential()
# Define the scope for Azure Resource Manager (ARM)
arm_scope = "https://durabletask.io/.default"

# Retrieve the access token
access_token = "Bearer " + default_credential.get_token(arm_scope).token
# create a client, start an orchestration, and wait for it to finish
metaData: list[tuple[str, str]] = [
    ("taskhub", taskhub_name), # Hardcode for now, just the taskhub name
    ("authorization", access_token) # use azure identity sdk for python
]
# configure and start the worker
with worker.TaskHubGrpcWorker(host_address=endpoint, metadata=metaData, secure_channel=True) as w:
    w.add_orchestrator(sequence)
    w.add_activity(hello)
    w.start()

    c = client.TaskHubGrpcClient(host_address=endpoint, metadata=metaData, secure_channel=True)
    instance_id = c.schedule_new_orchestration(sequence)
    state = c.wait_for_orchestration_completion(instance_id, timeout=45)
    if state and state.runtime_status == client.OrchestrationStatus.COMPLETED:
        print(f'Orchestration completed! Result: {state.serialized_output}')
    elif state:
        print(f'Orchestration failed: {state.failure_details}')
