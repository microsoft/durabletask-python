"""End-to-end sample that demonstrates how to configure an orchestrator
that a dynamic number activity functions in parallel, waits for them all
to complete, and prints an aggregate summary of the outputs."""
import random
import time
import os
from durabletask import client, task
from durabletask import client, task
import DurableTaskSchedulerWorker
import DurableTaskSchedulerClient


def get_work_items(ctx: task.ActivityContext, _) -> list[str]:
    """Activity function that returns a list of work items"""
    # return a random number of work items
    count = random.randint(2, 10)
    print(f'generating {count} work items...')
    return [f'work item {i}' for i in range(count)]


def process_work_item(ctx: task.ActivityContext, item: str) -> int:
    """Activity function that returns a result for a given work item"""
    print(f'processing work item: {item}')

    # simulate some work that takes a variable amount of time
    time.sleep(random.random() * 5)

    # return a result for the given work item, which is also a random number in this case
    return random.randint(0, 10)


def orchestrator(ctx: task.OrchestrationContext, _):
    """Orchestrator function that calls the 'get_work_items' and 'process_work_item'
    activity functions in parallel, waits for them all to complete, and prints
    an aggregate summary of the outputs"""

    work_items: list[str] = yield ctx.call_activity(get_work_items)

    # execute the work-items in parallel and wait for them all to return
    tasks = [ctx.call_activity(process_work_item, input=item) for item in work_items]
    results: list[int] = yield task.when_all(tasks)

    # return an aggregate summary of the results
    return {
        'work_items': work_items,
        'results': results,
        'total': sum(results),
    }


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
with DurableTaskSchedulerWorker(host_address=endpoint, secure_channel=True, taskhub=taskhub_name) as w:
    w.add_orchestrator(orchestrator)
    w.add_activity(process_work_item)
    w.add_activity(get_work_items)
    w.start()

    # create a client, start an orchestration, and wait for it to finish
    c = DurableTaskSchedulerClient(host_address=endpoint, secure_channel=True, taskhub=taskhub_name)
    instance_id = c.schedule_new_orchestration(orchestrator)
    state = c.wait_for_orchestration_completion(instance_id, timeout=30)
    if state and state.runtime_status == client.OrchestrationStatus.COMPLETED:
        print(f'Orchestration completed! Result: {state.serialized_output}')
    elif state:
        print(f'Orchestration failed: {state.failure_details}')
    exit()