# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import os

from durabletask import ConcurrencyOptions
from durabletask.worker import TaskHubGrpcWorker


def test_default_concurrency_options():
    """Test that default concurrency options work correctly."""
    options = ConcurrencyOptions()
    processor_count = os.cpu_count() or 1
    expected_default = 100 * processor_count

    assert options.maximum_concurrent_activity_work_items == expected_default
    assert options.maximum_concurrent_orchestration_work_items == expected_default
    assert options.max_total_workers == expected_default


def test_custom_concurrency_options():
    """Test that custom concurrency options work correctly."""
    options = ConcurrencyOptions(
        maximum_concurrent_activity_work_items=50,
        maximum_concurrent_orchestration_work_items=25,
    )

    assert options.maximum_concurrent_activity_work_items == 50
    assert options.maximum_concurrent_orchestration_work_items == 25
    assert options.max_total_workers == 50  # Max of both values


def test_partial_custom_options():
    """Test that partially specified options use defaults for unspecified values."""
    processor_count = os.cpu_count() or 1
    expected_default = 100 * processor_count

    options = ConcurrencyOptions(
        maximum_concurrent_activity_work_items=30
        # Leave other options as default
    )

    assert options.maximum_concurrent_activity_work_items == 30
    assert options.maximum_concurrent_orchestration_work_items == expected_default
    assert (
        options.max_total_workers == expected_default
    )  # Should be the default since it's larger


def test_max_total_workers_calculation():
    """Test that max_total_workers returns the maximum of all concurrency limits."""
    # Case 1: Activity is highest
    options1 = ConcurrencyOptions(
        maximum_concurrent_activity_work_items=100,
        maximum_concurrent_orchestration_work_items=50,
    )
    assert options1.max_total_workers == 100

    # Case 2: Orchestration is highest
    options2 = ConcurrencyOptions(
        maximum_concurrent_activity_work_items=25,
        maximum_concurrent_orchestration_work_items=100,
    )
    assert options2.max_total_workers == 100


def test_worker_with_concurrency_options():
    """Test that TaskHubGrpcWorker accepts concurrency options."""
    options = ConcurrencyOptions(
        maximum_concurrent_activity_work_items=10,
        maximum_concurrent_orchestration_work_items=20,
    )

    worker = TaskHubGrpcWorker(concurrency_options=options)

    assert worker.concurrency_options == options


def test_worker_default_options():
    """Test that TaskHubGrpcWorker uses default options when no parameters are provided."""
    worker = TaskHubGrpcWorker()

    processor_count = os.cpu_count() or 1
    expected_default = 100 * processor_count

    assert (
        worker.concurrency_options.maximum_concurrent_activity_work_items == expected_default
    )
    assert (
        worker.concurrency_options.maximum_concurrent_orchestration_work_items == expected_default
    )


def test_concurrency_options_property_access():
    """Test that the concurrency_options property works correctly."""
    options = ConcurrencyOptions(
        maximum_concurrent_activity_work_items=15,
        maximum_concurrent_orchestration_work_items=25,
    )

    worker = TaskHubGrpcWorker(concurrency_options=options)
    retrieved_options = worker.concurrency_options

    # Should be the same object
    assert retrieved_options is options

    # Should have correct values
    assert retrieved_options.maximum_concurrent_activity_work_items == 15
    assert retrieved_options.maximum_concurrent_orchestration_work_items == 25


def test_edge_cases():
    """Test edge cases like zero or very large values."""
    # Test with zeros (should still work)
    options_zero = ConcurrencyOptions(
        maximum_concurrent_activity_work_items=0,
        maximum_concurrent_orchestration_work_items=0,
    )
    assert options_zero.max_total_workers == 0

    # Test with very large values
    options_large = ConcurrencyOptions(
        maximum_concurrent_activity_work_items=999999,
        maximum_concurrent_orchestration_work_items=1,
    )
    assert options_large.max_total_workers == 999999
    assert options_large.max_total_workers == 999999
