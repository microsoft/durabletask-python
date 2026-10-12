# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# pyright: reportPrivateUsage=false

from dataclasses import dataclass
from datetime import datetime, timedelta
from itertools import permutations
from typing import Any

import pytest

from durabletask import task
from durabletask.internal import orchestrator_service_pb2 as pb


@pytest.mark.parametrize("order", list(permutations(range(3))))
def test_shared_pending_tasks(order: tuple[int, ...]) -> None:
    children = [task.CompletableTask[str]() for _ in range(3)]
    left = task.when_all([children[0], children[1]])
    right = task.when_all([children[0], children[2]])
    root = task.when_all([left, right])

    for index in order:
        children[index].complete("ABC"[index])

    assert root.is_complete
    assert root.get_result() == [["A", "B"], ["A", "C"]]
    assert left.pending_tasks == right.pending_tasks == 0


@pytest.mark.parametrize("precompleted", [False, True])
def test_duplicate_tasks_preserve_positions(precompleted: bool) -> None:
    first = task.CompletableTask[str]()
    second = task.CompletableTask[str]()
    if precompleted:
        first.complete("A")
    group = task.when_all([first, second, first])
    assert group.pending_tasks == (1 if precompleted else 3)

    second.complete("B")
    if not precompleted:
        first.complete("A")

    assert group.is_complete
    assert group.pending_tasks == 0
    assert group.get_completed_tasks() == 3
    assert group.get_result() == ["A", "B", "A"]


@pytest.mark.parametrize("precompleted", [False, True])
def test_only_duplicate_tasks(precompleted: bool) -> None:
    child = task.CompletableTask[str]()
    if precompleted:
        child.complete("V")
    group = task.when_all([child, child])
    if not precompleted:
        child.complete("V")
    assert group.is_complete
    assert group.get_result() == ["V", "V"]


def test_shared_duplicate_failure_waits_for_all_children() -> None:
    failed = task.CompletableTask[str]()
    later_failure = task.CompletableTask[str]()
    sibling = task.CompletableTask[str]()
    left = task.when_all([failed, later_failure, failed])
    right = task.when_all([failed, sibling])
    root = task.when_all([left, right])

    failed.fail("first failure", ValueError("first failure"))
    assert left.pending_tasks == right.pending_tasks == 1
    assert not left.is_failed
    assert not right.is_failed
    assert not root.is_complete

    later_failure.fail("later failure", ValueError("later failure"))
    assert left.is_failed
    assert not root.is_complete
    sibling.complete("ok")
    assert root.is_complete
    assert root.get_exception() is failed.get_exception()


def test_shared_nested_composite_and_duplicate_notifications() -> None:
    first = task.CompletableTask[str]()
    second = task.CompletableTask[str]()
    shared = task.when_all([first, second])
    left = task.when_all([shared, shared])
    right = task.when_all([shared])
    root = task.when_all([left, right])

    first.complete("A")
    second.complete("B")
    left.on_child_completed(shared)

    assert root.get_result() == [[["A", "B"], ["A", "B"]], [["A", "B"]]]
    assert left.get_completed_tasks() == 2


@pytest.mark.parametrize("race_first", [False, True])
def test_overlapping_all_and_any(race_first: bool) -> None:
    shared = task.CompletableTask[str]()
    sibling = task.CompletableTask[str]()
    loser = task.CompletableTask[str]()
    if race_first:
        race = task.when_any([shared, loser, shared])
        group = task.when_all([shared, sibling])
    else:
        group = task.when_all([shared, sibling])
        race = task.when_any([shared, loser, shared])
    loser_group = task.when_all([loser])
    root_children: list[task.Task[Any]] = [race, group]
    root = task.when_all(root_children)

    shared.complete("A")
    assert race.get_result() is shared
    assert id(race) not in loser._parents
    assert loser._parents[id(loser_group)] is loser_group
    assert not root.is_complete
    sibling.complete("B")
    loser.complete("C")
    assert root.get_result() == [shared, ["A", "B"]]
    assert loser_group.get_result() == ["C"]


@pytest.mark.parametrize("winner_index", [0, 1])
def test_precompleted_race_does_not_subscribe_to_losers(winner_index: int) -> None:
    children = [task.CompletableTask[str]() for _ in range(3)]
    children[winner_index].complete("winner")
    race = task.when_any(children)
    assert race.get_result() is children[winner_index]
    assert all(id(race) not in child._parents for child in children)
    assert not race._pending_children


def test_shared_race_returns_winning_direct_child() -> None:
    timer = task.TimerTask()
    work = task.CompletableTask[str]()
    first_children: list[task.Task[Any]] = [timer, work]
    first_race = task.when_any(first_children)
    second_children: list[task.Task[Any]] = [first_race, timer]
    second_race = task.when_any(second_children)

    timer.complete(None)

    assert first_race.get_result() is timer
    assert second_race.get_result() is first_race
    work.complete("done")
    assert first_race.get_result() is timer


@pytest.mark.parametrize("failed", [False, True])
def test_shared_retryable_terminal_completion(failed: bool) -> None:
    policy = task.RetryPolicy(
        first_retry_interval=timedelta(seconds=1), max_number_of_attempts=2)
    child = task.RetryableTask[str](
        policy, pb.OrchestratorAction(), datetime.now(), is_sub_orch=False)
    left = task.when_all([child, child])
    right = task.when_all([child])
    root = task.when_all([left, right])

    child.increment_attempt_count()
    assert child.compute_next_delay() is None
    assert not root.is_complete
    if failed:
        child.fail("exhausted", ValueError("exhausted"))
        assert root.is_complete
        assert root.get_exception() is child.get_exception()
    else:
        child.complete("retried")
        assert root.get_result() == [["retried", "retried"], ["retried"]]


@pytest.mark.parametrize("group_count", [1, 2])
def test_cancel_callback_errors_do_not_block_other_observers(group_count: int) -> None:
    child = task.CancellableTask[str]()
    broken = [task.when_all([child, child]) for _ in range(group_count)]
    stranded_parents = [task.when_any([group]) for group in broken]
    race = task.when_any([child])
    healthy = task.when_all([race])

    with pytest.raises(Exception) as raised:
        child.cancel()

    if group_count == 1:
        assert isinstance(raised.value, task.TaskCancelledError)
    else:
        assert isinstance(raised.value, task._TaskCompletionError)
        assert len(raised.value.errors) == group_count
        assert all(isinstance(error, task.TaskCancelledError)
                   for error in raised.value.errors)
    assert child.is_cancelled
    assert not child.cancel()
    assert race.get_result() is child
    assert healthy.get_result() == [child]
    assert all(group.is_complete for group in broken)
    assert all(not parent.is_complete for parent in stranded_parents)
    assert not child._parents


def test_throwing_cancel_handler_leaves_task_pending() -> None:
    child = task.CancellableTask[str]()
    left = task.when_all([child])
    right = task.when_any([child])
    error = RuntimeError("cancel handler failed")

    def cancel_handler() -> None:
        raise error

    child.set_cancel_handler(cancel_handler)
    with pytest.raises(RuntimeError) as raised:
        child.cancel()
    assert raised.value is error
    assert not child.is_complete
    assert not left.is_complete
    assert not right.is_complete
    child.complete("ok")
    assert left.get_result() == ["ok"]
    assert right.get_result() is child


def test_duplicate_callback_is_ignored_while_pending() -> None:
    first = task.CompletableTask[str]()
    second = task.CompletableTask[str]()
    group = task.when_all([first, second, first])
    first.complete("A")
    group.on_child_completed(first)
    assert group.pending_tasks == 1
    second.complete("B")
    assert group.get_result() == ["A", "B", "A"]


def test_empty_composite_behavior_is_unchanged() -> None:
    groups: list[task.CompositeTask[Any]] = [task.when_all([]), task.when_any([])]
    assert all(not group.is_complete for group in groups)


def test_distinct_equal_unhashable_tasks_and_parents() -> None:
    @dataclass(init=False)
    class EqualTask(task.CompletableTask[str]):
        pass

    @dataclass(init=False)
    class EqualGroup(task.WhenAllTask[str]):
        pass

    first = EqualTask()
    second = EqualTask()
    assert first == second
    left = EqualGroup([first, second, first])
    right = EqualGroup([first])
    assert left == right
    root = task.when_all([left, right])

    first.complete("A")
    assert left.pending_tasks == 1
    assert not root.is_complete
    second.complete("B")
    assert root.get_result() == [["A", "B", "A"], ["A"]]
