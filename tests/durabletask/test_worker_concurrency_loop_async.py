# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import asyncio

from durabletask.worker import ConcurrencyOptions, TaskHubGrpcWorker


class DummyStub:
    def __init__(self):
        self.completed = []

    def CompleteOrchestratorTask(self, res):
        self.completed.append(('orchestrator', res))

    def CompleteActivityTask(self, res):
        self.completed.append(('activity', res))


class DummyRequest:
    def __init__(self, kind, instance_id):
        self.kind = kind
        self.instanceId = instance_id
        self.orchestrationInstance = type('O', (), {'instanceId': instance_id})
        self.name = 'dummy'
        self.taskId = 1
        self.input = type('I', (), {'value': ''})
        self.pastEvents = []
        self.newEvents = []

    def HasField(self, field):
        return (field == 'orchestratorRequest' and self.kind == 'orchestrator') or \
               (field == 'activityRequest' and self.kind == 'activity')

    def WhichOneof(self, _):
        return f'{self.kind}Request'


class DummyCompletionToken:
    pass


def test_worker_concurrency_loop_async():
    options = ConcurrencyOptions(
        maximum_concurrent_activity_work_items=2,
        maximum_concurrent_orchestration_work_items=1,
        maximum_thread_pool_workers=2,
    )
    grpc_worker = TaskHubGrpcWorker(concurrency_options=options)
    stub = DummyStub()

    async def dummy_orchestrator(req, stub, completionToken):
        await asyncio.sleep(0.1)
        stub.CompleteOrchestratorTask('ok')

    async def cancel_dummy_orchestrator(req, stub, completionToken):
        pass

    async def dummy_activity(req, stub, completionToken):
        await asyncio.sleep(0.1)
        stub.CompleteActivityTask('ok')

    async def cancel_dummy_activity(req, stub, completionToken):
        pass

    # Patch the worker's _execute_orchestrator and _execute_activity
    grpc_worker._execute_orchestrator = dummy_orchestrator.__get__(grpc_worker, TaskHubGrpcWorker)
    grpc_worker._cancel_orchestrator = cancel_dummy_orchestrator.__get__(grpc_worker, TaskHubGrpcWorker)
    grpc_worker._execute_activity = dummy_activity.__get__(grpc_worker, TaskHubGrpcWorker)
    grpc_worker._cancel_activity = cancel_dummy_activity.__get__(grpc_worker, TaskHubGrpcWorker)

    orchestrator_requests = [DummyRequest('orchestrator', f'orch{i}') for i in range(3)]
    activity_requests = [DummyRequest('activity', f'act{i}') for i in range(4)]

    async def run_test():
        # Clear stub state before each run
        stub.completed.clear()
        grpc_worker._async_worker_manager.prepare_for_run()
        worker_task = asyncio.create_task(grpc_worker._async_worker_manager.run())
        # Need to yield to that thread in order to let it start up on the second run
        startup_attempts = 0
        while grpc_worker._async_worker_manager._shutdown and startup_attempts < 10:
            await asyncio.sleep(0.1)
            startup_attempts += 1
        for req in orchestrator_requests:
            grpc_worker._async_worker_manager.submit_orchestration(dummy_orchestrator, cancel_dummy_orchestrator, req, stub, DummyCompletionToken())
        for req in activity_requests:
            grpc_worker._async_worker_manager.submit_activity(dummy_activity, cancel_dummy_activity, req, stub, DummyCompletionToken())
        await asyncio.sleep(1.0)
        orchestrator_count = sum(1 for t, _ in stub.completed if t == 'orchestrator')
        activity_count = sum(1 for t, _ in stub.completed if t == 'activity')
        assert orchestrator_count == 3, f"Expected 3 orchestrator completions, got {orchestrator_count}"
        assert activity_count == 4, f"Expected 4 activity completions, got {activity_count}"
        grpc_worker._async_worker_manager._shutdown = True
        await worker_task
    asyncio.run(run_test())
    asyncio.run(run_test())


def _start_manager_and_wait_for_queues(manager):
    """Start the manager loop and wait until its queues are bound to this loop."""
    worker_task = asyncio.create_task(manager.run())

    async def wait_for_queues():
        for _ in range(100):
            if manager.activity_queue is not None:
                return
            await asyncio.sleep(0.01)
        raise RuntimeError("Worker manager queues were never initialized")

    return worker_task, wait_for_queues()


def test_async_worker_manager_bounds_task_allocation():
    """Work item tasks must be allocated only up to the concurrency limit.

    The concurrency semaphore is acquired before a task is created, so a burst
    of queued work items must not allocate one asyncio.Task per queued item.
    """
    limit = 2
    total_items = 25
    options = ConcurrencyOptions(
        maximum_concurrent_activity_work_items=limit,
        maximum_concurrent_orchestration_work_items=limit,
        maximum_concurrent_entity_work_items=limit,
        maximum_thread_pool_workers=limit,
    )
    manager = TaskHubGrpcWorker(concurrency_options=options)._async_worker_manager

    state = {"allocated": 0, "peak_allocated": 0, "running": 0, "peak_running": 0}
    completed = []
    original_process_work_item = manager._process_work_item

    def counting_process_work_item(*args, **kwargs):
        # The coroutine object is created synchronously by asyncio.create_task,
        # so this counts allocated work item tasks, not just started ones.
        state["allocated"] += 1
        state["peak_allocated"] = max(state["peak_allocated"], state["allocated"])
        inner = original_process_work_item(*args, **kwargs)

        async def tracked():
            try:
                return await inner
            finally:
                state["allocated"] -= 1

        return tracked()

    manager._process_work_item = counting_process_work_item

    async def slow_work(idx):
        state["running"] += 1
        state["peak_running"] = max(state["peak_running"], state["running"])
        await asyncio.sleep(0.02)
        state["running"] -= 1
        completed.append(idx)

    async def cancel_work(idx):
        pass

    async def run_test():
        manager.prepare_for_run()
        worker_task, wait_for_queues = _start_manager_and_wait_for_queues(manager)
        await wait_for_queues
        assert manager.activity_queue is not None
        for i in range(total_items):
            manager.submit_activity(slow_work, cancel_work, i)
        await asyncio.wait_for(manager.activity_queue.join(), timeout=60)
        manager.shutdown()
        await asyncio.wait_for(worker_task, timeout=60)

    asyncio.run(run_test())

    assert sorted(completed) == list(range(total_items))
    assert state["peak_allocated"] <= limit, (
        f"Expected at most {limit} concurrently allocated work item tasks, "
        f"got {state['peak_allocated']}"
    )
    # The concurrency limit must still be fully usable.
    assert state["peak_running"] == limit, (
        f"Expected {limit} concurrently running work items, got {state['peak_running']}"
    )
    assert state["allocated"] == 0


def test_async_worker_manager_calls_task_done_exactly_once_on_failure():
    """Failing work items must still mark the queue item done exactly once."""
    total_items = 6
    options = ConcurrencyOptions(
        maximum_concurrent_activity_work_items=2,
        maximum_concurrent_orchestration_work_items=2,
        maximum_concurrent_entity_work_items=2,
        maximum_thread_pool_workers=2,
    )
    manager = TaskHubGrpcWorker(concurrency_options=options)._async_worker_manager
    cancelled = []
    task_done_calls = []

    async def failing_work(idx):
        raise RuntimeError(f"boom {idx}")

    async def cancel_work(idx):
        cancelled.append(idx)

    async def run_test():
        manager.prepare_for_run()
        worker_task, wait_for_queues = _start_manager_and_wait_for_queues(manager)
        await wait_for_queues
        queue = manager.activity_queue
        assert queue is not None
        original_task_done = queue.task_done

        def counting_task_done():
            task_done_calls.append(1)
            original_task_done()

        queue.task_done = counting_task_done  # type: ignore[method-assign]
        for i in range(total_items):
            manager.submit_activity(failing_work, cancel_work, i)
        await asyncio.wait_for(queue.join(), timeout=60)
        manager.shutdown()
        await asyncio.wait_for(worker_task, timeout=60)

    asyncio.run(run_test())

    assert sorted(cancelled) == list(range(total_items))
    assert len(task_done_calls) == total_items
