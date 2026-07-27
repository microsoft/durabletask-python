# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for :meth:`durabletask.history.HistoryEvent.to_dict`.

``to_dict`` walks the dataclass graph in a single pass. It used to be
implemented as ``_to_serializable(asdict(self))``, which deep-copied the
whole event into a throwaway structure and then walked that copy again.
The tests below pin the output of the current implementation to that
original two-pass form so the optimization stays behavior-preserving.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, cast

import pytest

from durabletask import history, task

_TS = datetime(2025, 1, 2, 3, 4, 5, 678901, tzinfo=timezone.utc)
_NAIVE_TS = datetime(2024, 12, 31, 23, 59, 58)
_FIRE_AT = datetime(2025, 6, 7, 8, 9, 10, tzinfo=timezone.utc)


def _legacy_to_serializable(value: Any) -> Any:
    """The pre-optimization value walker, kept verbatim as a test oracle."""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, list):
        return [_legacy_to_serializable(item) for item in cast(list[Any], value)]
    if isinstance(value, dict):
        return {
            key: _legacy_to_serializable(item)
            for key, item in cast(dict[Any, Any], value).items()
        }
    return value


def _legacy_to_dict(event: history.HistoryEvent) -> dict[str, Any]:
    """The pre-optimization ``to_dict`` body, kept verbatim as a test oracle."""
    return _legacy_to_serializable(asdict(event))


def _failure(message: str = 'boom') -> task.FailureDetails:
    return task.FailureDetails(message, 'RuntimeError', 'Traceback...')


def _trace_context() -> history.TraceContext:
    return history.TraceContext(
        trace_parent='00-trace-span-01',
        span_id='span-id',
        trace_state='state=1',
    )


def _parent_instance() -> history.ParentInstanceInfo:
    return history.ParentInstanceInfo(
        task_scheduled_id=7,
        name='ParentOrch',
        version='2.0',
        orchestration_instance=history.OrchestrationInstance(
            instance_id='parent-instance',
            execution_id='parent-execution',
        ),
    )


def _orchestration_state() -> dict[str, Any]:
    """A deeply nested ``dict[str, Any]`` payload, as produced by MessageToDict."""
    return {
        'instanceId': 'abc',
        'orchestrationStatus': 'ORCHESTRATION_STATUS_RUNNING',
        'createdTimestamp': _TS,
        'retryCount': 3,
        'completed': False,
        'progress': 0.75,
        'output': None,
        'tags': {'a': '1', 'b': '2'},
        'nested': {
            'level2': {
                'level3': ['x', 'y', {'level4': _TS}],
            },
            'failure': _failure('nested failure'),
        },
        'history': [
            {'eventId': 1, 'timestamp': _TS},
            {'eventId': 2, 'timestamp': _NAIVE_TS},
            [_failure('in a list'), _TS, None, True, 1.5],
        ],
    }


def _sample_events() -> list[history.HistoryEvent]:
    """One fully-populated and one minimally-populated instance per event type."""
    return [
        history.HistoryEvent(event_id=0, timestamp=_TS),
        history.ExecutionStartedEvent(
            event_id=-1,
            timestamp=_TS,
            name='MyOrch',
            version='1.0',
            input='"hello"',
            orchestration_instance=history.OrchestrationInstance(
                instance_id='inst', execution_id='exec',
            ),
            parent_instance=_parent_instance(),
            scheduled_start_timestamp=_NAIVE_TS,
            parent_trace_context=_trace_context(),
            orchestration_span_id='orch-span',
            tags={'k': 'v', 'k2': 'v2'},
        ),
        history.ExecutionStartedEvent(event_id=-1, timestamp=_TS, name='Bare'),
        history.ExecutionCompletedEvent(
            event_id=-1,
            timestamp=_TS,
            orchestration_status=3,
            result='"done"',
            failure_details=_failure(),
        ),
        history.ExecutionCompletedEvent(
            event_id=-1, timestamp=_TS, orchestration_status=1,
        ),
        history.ExecutionTerminatedEvent(
            event_id=-1, timestamp=_TS, input='"bye"', recurse=True,
        ),
        history.ExecutionTerminatedEvent(event_id=-1, timestamp=_TS),
        history.TaskScheduledEvent(
            event_id=1,
            timestamp=_TS,
            name='MyActivity',
            version='1.2.3',
            input='42',
            parent_trace_context=_trace_context(),
            tags={'t': 'v'},
        ),
        history.TaskScheduledEvent(event_id=1, timestamp=_TS, name='Bare'),
        history.TaskCompletedEvent(
            event_id=-1, timestamp=_TS, task_scheduled_id=1, result='43',
        ),
        history.TaskFailedEvent(
            event_id=-1, timestamp=_TS, task_scheduled_id=1,
            failure_details=_failure(),
        ),
        history.TaskFailedEvent(event_id=-1, timestamp=_TS, task_scheduled_id=1),
        history.SubOrchestrationInstanceCreatedEvent(
            event_id=2,
            timestamp=_TS,
            instance_id='child',
            name='ChildOrch',
            version='9',
            input='null',
            parent_trace_context=_trace_context(),
            tags={'x': 'y'},
        ),
        history.SubOrchestrationInstanceCompletedEvent(
            event_id=-1, timestamp=_TS, task_scheduled_id=2, result='"ok"',
        ),
        history.SubOrchestrationInstanceFailedEvent(
            event_id=-1, timestamp=_TS, task_scheduled_id=2,
            failure_details=_failure(),
        ),
        history.TimerCreatedEvent(event_id=3, timestamp=_TS, fire_at=_FIRE_AT),
        history.TimerFiredEvent(
            event_id=-1, timestamp=_TS, fire_at=_FIRE_AT, timer_id=3,
        ),
        history.OrchestratorStartedEvent(event_id=-1, timestamp=_TS),
        history.OrchestratorCompletedEvent(event_id=-1, timestamp=_TS),
        history.EventSentEvent(
            event_id=4, timestamp=_TS, instance_id='other', name='Ping',
            input='"data"',
        ),
        history.EventRaisedEvent(
            event_id=-1, timestamp=_TS, name='Pong', input='"data"',
        ),
        history.EventRaisedEvent(event_id=-1, timestamp=_TS, name='Pong'),
        history.GenericEvent(event_id=-1, timestamp=_TS, data='some data'),
        history.HistoryStateEvent(
            event_id=-1, timestamp=_TS, orchestration_state=_orchestration_state(),
        ),
        history.HistoryStateEvent(
            event_id=-1, timestamp=_TS, orchestration_state={},
        ),
        history.ContinueAsNewEvent(event_id=-1, timestamp=_TS, input='"again"'),
        history.ExecutionSuspendedEvent(event_id=-1, timestamp=_TS, input='"why"'),
        history.ExecutionResumedEvent(event_id=-1, timestamp=_TS, input=None),
        history.EntityOperationSignaledEvent(
            event_id=-1,
            timestamp=_TS,
            request_id='req-1',
            operation='op',
            scheduled_time=_FIRE_AT,
            input='"payload"',
            target_instance_id='@entity@key',
        ),
        history.EntityOperationSignaledEvent(
            event_id=-1, timestamp=_TS, request_id='req-1', operation='op',
        ),
        history.EntityOperationCalledEvent(
            event_id=-1,
            timestamp=_TS,
            request_id='req-2',
            operation='op',
            scheduled_time=_FIRE_AT,
            input='"payload"',
            parent_instance_id='parent',
            parent_execution_id='exec',
            target_instance_id='@entity@key',
        ),
        history.EntityOperationCompletedEvent(
            event_id=-1, timestamp=_TS, request_id='req-2', output='"result"',
        ),
        history.EntityOperationFailedEvent(
            event_id=-1, timestamp=_TS, request_id='req-2',
            failure_details=_failure(),
        ),
        history.EntityLockRequestedEvent(
            event_id=-1,
            timestamp=_TS,
            critical_section_id='cs-1',
            lock_set=['@a@1', '@b@2'],
            position=0,
            parent_instance_id='parent',
        ),
        history.EntityLockRequestedEvent(
            event_id=-1, timestamp=_TS, critical_section_id='cs-1',
            lock_set=[], position=1,
        ),
        history.EntityLockGrantedEvent(
            event_id=-1, timestamp=_TS, critical_section_id='cs-1',
        ),
        history.EntityUnlockSentEvent(
            event_id=-1,
            timestamp=_TS,
            critical_section_id='cs-1',
            parent_instance_id='parent',
            target_instance_id='@entity@key',
        ),
        history.ExecutionRewoundEvent(
            event_id=-1,
            timestamp=_TS,
            reason='manual',
            parent_execution_id='exec',
            instance_id='inst',
            parent_trace_context=_trace_context(),
            name='MyOrch',
            version='1.0',
            input='"hello"',
            parent_instance=_parent_instance(),
            tags={'r': 'w'},
        ),
        history.ExecutionRewoundEvent(event_id=-1, timestamp=_TS),
    ]


def _event_ids(events: list[history.HistoryEvent]) -> list[str]:
    seen: dict[str, int] = {}
    ids: list[str] = []
    for event in events:
        name = type(event).__name__
        seen[name] = seen.get(name, 0) + 1
        ids.append(f'{name}-{seen[name]}')
    return ids


_SAMPLE_EVENTS = _sample_events()


class TestToDictEquivalence:
    """``to_dict`` must produce exactly what the old two-pass form produced."""

    @pytest.mark.parametrize(
        'event', _SAMPLE_EVENTS, ids=_event_ids(_SAMPLE_EVENTS),
    )
    def test_matches_legacy_two_pass_output(
        self, event: history.HistoryEvent,
    ) -> None:
        actual = event.to_dict()
        expected = _legacy_to_dict(event)
        assert actual == expected
        # ``repr`` pins key ordering and value types, not just equality.
        assert repr(actual) == repr(expected)

    def test_covers_every_history_event_type(self) -> None:
        exported = {
            getattr(history, name)
            for name in history.__all__
            if isinstance(getattr(history, name), type)
        }
        declared = {
            cls for cls in exported
            if issubclass(cls, history.HistoryEvent)
        }
        covered = {type(event) for event in _SAMPLE_EVENTS}
        assert declared - covered == set()

    def test_module_level_to_dict_helper_matches(self) -> None:
        for event in _SAMPLE_EVENTS:
            assert history.to_dict(event) == _legacy_to_dict(event)


class TestToDictOutputShape:
    def test_timestamps_use_isoformat(self) -> None:
        event = history.TimerFiredEvent(
            event_id=5, timestamp=_TS, fire_at=_FIRE_AT, timer_id=3,
        )
        assert event.to_dict() == {
            'event_id': 5,
            'timestamp': _TS.isoformat(),
            'fire_at': _FIRE_AT.isoformat(),
            'timer_id': 3,
        }

    def test_field_order_follows_dataclass_declaration(self) -> None:
        event = history.TaskScheduledEvent(
            event_id=1, timestamp=_TS, name='A', version='1', input='2',
            parent_trace_context=_trace_context(), tags={'k': 'v'},
        )
        assert list(event.to_dict()) == [
            'event_id', 'timestamp', 'name', 'version', 'input',
            'parent_trace_context', 'tags',
        ]

    def test_nested_dataclasses_become_dicts(self) -> None:
        event = history.ExecutionStartedEvent(
            event_id=-1,
            timestamp=_TS,
            name='MyOrch',
            parent_instance=_parent_instance(),
        )
        assert event.to_dict()['parent_instance'] == {
            'task_scheduled_id': 7,
            'name': 'ParentOrch',
            'version': '2.0',
            'orchestration_instance': {
                'instance_id': 'parent-instance',
                'execution_id': 'parent-execution',
            },
        }

    def test_none_values_are_preserved(self) -> None:
        event = history.ExecutionStartedEvent(
            event_id=-1, timestamp=_TS, name='MyOrch',
        )
        payload = event.to_dict()
        assert payload['version'] is None
        assert payload['parent_instance'] is None
        assert payload['tags'] is None

    @pytest.mark.parametrize(
        'event', _SAMPLE_EVENTS, ids=_event_ids(_SAMPLE_EVENTS),
    )
    def test_output_is_json_serializable(
        self, event: history.HistoryEvent,
    ) -> None:
        json.dumps(event.to_dict(), sort_keys=True)


class TestToDictIsolation:
    """The returned structure must not alias the event's own containers."""

    def test_mutating_result_does_not_affect_event(self) -> None:
        event = history.ExecutionStartedEvent(
            event_id=-1,
            timestamp=_TS,
            name='MyOrch',
            tags={'k': 'v'},
        )
        payload = event.to_dict()
        payload['tags']['k'] = 'mutated'
        assert event.tags == {'k': 'v'}

    def test_nested_containers_are_rebuilt(self) -> None:
        state = _orchestration_state()
        event = history.HistoryStateEvent(
            event_id=-1, timestamp=_TS, orchestration_state=state,
        )
        payload = event.to_dict()
        assert payload['orchestration_state'] is not state
        assert payload['orchestration_state']['tags'] is not state['tags']
        assert payload['orchestration_state']['history'] is not state['history']

    def test_lock_set_list_is_rebuilt(self) -> None:
        lock_set = ['@a@1', '@b@2']
        event = history.EntityLockRequestedEvent(
            event_id=-1, timestamp=_TS, critical_section_id='cs',
            lock_set=lock_set, position=0,
        )
        payload = event.to_dict()
        assert payload['lock_set'] == lock_set
        assert payload['lock_set'] is not lock_set
