#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Example demonstrating the in-memory backend for testing orchestrations.

This example shows how to:
1. Create and start an in-memory backend
2. Connect a client and worker to it
3. Define and run a simple orchestration
4. Verify the results
"""

from durabletask.testing import create_test_backend
from durabletask.client import TaskHubGrpcClient, OrchestrationStatus
from durabletask.worker import TaskHubGrpcWorker


def main():
    # Create and start the in-memory backend on port 50051
    print("Starting in-memory backend...")
    backend = create_test_backend(port=50051)

    try:
        # Create client and worker
        print("Creating client and worker...")
        client = TaskHubGrpcClient(host_address="localhost:50051")
        worker = TaskHubGrpcWorker(host_address="localhost:50051")

        # Define an orchestrator
        def greet_orchestrator(ctx, name: str):
            """Orchestrator that greets someone using activities."""
            greeting = yield ctx.call_activity(get_greeting, input=name)
            punctuation = yield ctx.call_activity(add_punctuation, input=greeting)
            return punctuation

        # Define activities
        def get_greeting(ctx, name: str):
            """Activity that generates a greeting."""
            return f"Hello, {name}"

        def add_punctuation(ctx, text: str):
            """Activity that adds punctuation."""
            return f"{text}!"

        # Register orchestrators and activities
        worker.add_orchestrator(greet_orchestrator)
        worker.add_activity(get_greeting)
        worker.add_activity(add_punctuation)

        # Start the worker
        print("Starting worker...")
        worker.start()

        try:
            # Schedule an orchestration
            print("\nScheduling orchestration...")
            instance_id = client.schedule_new_orchestration(
                greet_orchestrator,
                input="World"
            )
            print(f"Orchestration scheduled with ID: {instance_id}")

            # Wait for completion
            print("Waiting for orchestration to complete...")
            state = client.wait_for_orchestration_completion(instance_id, timeout=10)

            # Display results
            print("\n" + "=" * 50)
            print("Orchestration Results:")
            print("=" * 50)
            if state is None:
                print("\n✗ Orchestration state is None (timed out?)")
            else:
                print(f"Instance ID: {state.instance_id}")
                print(f"Name: {state.name}")
                print(f"Status: {state.runtime_status}")
                print(f"Output: {state.serialized_output}")
                print(f"Created At: {state.created_at}")
                print(f"Last Updated: {state.last_updated_at}")
                print("=" * 50)

                # Verify the result
                if state.runtime_status == OrchestrationStatus.COMPLETED:
                    print("\n✓ Orchestration completed successfully!")
                else:
                    print(f"\n✗ Orchestration did not complete successfully: {state.runtime_status}")

        finally:
            print("\nStopping worker...")
            worker.stop()

    finally:
        print("Stopping backend...")
        backend.stop()
        print("Done!")


if __name__ == "__main__":
    main()
