"""End-to-end sample that demonstrates how to configure an orchestrator
that calls an activity function in a sequence and prints the outputs."""
import os
from durabletask import client, task
from externalpackages.durabletaskscheduler.durabletask_scheduler_worker import DurableTaskSchedulerWorker
from externalpackages.durabletaskscheduler.durabletask_scheduler_client import DurableTaskSchedulerClient

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
    print("ENDPOINT is not set. Please set the ENDPOINT environment variable to the endpoint of the scheduler")
    print("If you are using windows powershell, run the following: $env:ENDPOINT=\"<schedulerEndpoint>\"")
    print("If you are using bash, run the following: export ENDPOINT=\"<schedulerEndpoint>\"")
    exit()


# configure and start the worker
with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True, use_managed_identity=False, client_id="", taskhub=taskhub_name) as w:
    w.add_orchestrator(sequence)
    w.add_activity(hello)
    w.start()

    # Construct the client and run the orchestrations
    c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True, taskhub=taskhub_name)
    instance_id = c.schedule_new_orchestration(sequence)
    state = c.wait_for_orchestration_completion(instance_id, timeout=60)
    if state and state.runtime_status == client.OrchestrationStatus.COMPLETED:
        print(f'Orchestration completed! Result: {state.serialized_output}')
    elif state:
        print(f'Orchestration failed: {state.failure_details}')
