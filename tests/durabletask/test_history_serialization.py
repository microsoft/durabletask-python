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
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, NamedTuple, cast

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


@dataclass
class _Inner:
    """A dataclass the walker should convert wherever it is nested."""

    label: str
    when: datetime


class _Point(NamedTuple):
    """A namedtuple, which rebuilds differently from a plain tuple."""

    x: Any
    y: Any


class _MutableLeaf:
    """A value the walker cannot convert, so it must be copied out."""

    def __init__(self, n: int) -> None:
        self.n = n

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _MutableLeaf) and other.n == self.n

    def __repr__(self) -> str:
        return f'_MutableLeaf({self.n})'


def _state_event(value: Any) -> history.HistoryStateEvent:
    """Wrap ``value`` in the only ``dict[str, Any]`` field on any event."""
    return history.HistoryStateEvent(
        event_id=-1, timestamp=_TS, orchestration_state={'value': value},
    )


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
    """``to_dict`` must produce exactly what the old two-pass form produced.

    Tuple-bearing payloads are deliberately excluded from this suite and
    covered by :class:`TestTupleHandling` instead: the old two-pass form
    mishandled tuples, so matching it there would mean reproducing a bug.
    """

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

    @pytest.mark.parametrize(
        'leaf',
        [
            pytest.param(_MutableLeaf(1), id='custom-object'),
            pytest.param(bytearray(b'abc'), id='bytearray'),
            pytest.param({1, 2, 3}, id='set'),
            pytest.param((1, 'x'), id='tuple'),
            pytest.param(_Point(1, 2), id='namedtuple'),
        ],
    )
    def test_mutable_leaf_is_not_aliased(self, leaf: Any) -> None:
        """Values the walker does not recognize must still be copied out.

        ``dataclasses.asdict`` ended its recursion with ``copy.deepcopy``,
        so the dict it produced never shared mutable state with the event.
        The single-pass walker has to preserve that isolation, otherwise a
        caller mutating the exported dict would corrupt the live event.
        """
        event = _state_event(leaf)
        exported = event.to_dict()['orchestration_state']['value']
        assert exported == leaf
        assert exported is not leaf

    def test_mutating_exported_leaf_does_not_affect_event(self) -> None:
        leaf = _MutableLeaf(1)
        event = _state_event(leaf)

        exported = event.to_dict()['orchestration_state']['value']
        exported.n = 999

        assert leaf.n == 1
        assert event.orchestration_state['value'].n == 1

    def test_mutating_exported_bytearray_does_not_affect_event(self) -> None:
        leaf = bytearray(b'abc')
        event = _state_event(leaf)

        exported = event.to_dict()['orchestration_state']['value']
        exported.extend(b'def')

        assert leaf == bytearray(b'abc')

    def test_nested_mutable_leaf_is_not_aliased(self) -> None:
        """Isolation must hold for leaves buried inside lists and dicts."""
        leaf = _MutableLeaf(7)
        event = _state_event({'deep': [leaf]})

        exported = event.to_dict()['orchestration_state']['value']['deep'][0]
        assert exported == leaf
        assert exported is not leaf


class TestTupleHandling:
    """Tuples must recurse the same way lists do.

    The old two-pass form got this wrong in a way worth spelling out.
    ``dataclasses.asdict`` *did* rebuild tuples, converting any nested
    dataclass into a dict, but the ``_to_serializable`` pass that ran
    afterwards had no tuple branch, so datetimes sitting inside a tuple
    were never converted. The result was a value that could not be JSON
    encoded. The single-pass walker handles tuples exactly like lists,
    which diverges from the old output on purpose.
    """

    def test_tuple_containing_dataclass_is_converted(self) -> None:
        event = _state_event((_Inner('a', _TS),))
        exported = event.to_dict()['orchestration_state']['value']
        assert exported == ({'label': 'a', 'when': _TS.isoformat()},)

    def test_datetime_inside_tuple_is_converted(self) -> None:
        event = _state_event((_TS,))
        exported = event.to_dict()['orchestration_state']['value']
        assert exported == (_TS.isoformat(),)

    def test_tuple_type_is_preserved(self) -> None:
        event = _state_event((1, 'x'))
        exported = event.to_dict()['orchestration_state']['value']
        assert isinstance(exported, tuple)

    def test_namedtuple_type_is_preserved(self) -> None:
        """Rebuilding a tuple must not flatten a namedtuple into a plain one."""
        event = _state_event(_Point(1, 2))
        exported = event.to_dict()['orchestration_state']['value']
        assert isinstance(exported, _Point)
        assert exported == _Point(1, 2)

    def test_namedtuple_contents_are_converted(self) -> None:
        event = _state_event(_Point(_TS, _Inner('b', _TS)))
        exported = event.to_dict()['orchestration_state']['value']
        assert exported == _Point(
            _TS.isoformat(), {'label': 'b', 'when': _TS.isoformat()},
        )

    def test_tuple_nested_in_dict_and_list_is_converted(self) -> None:
        event = _state_event({'k': [(_Inner('c', _TS),)]})
        exported = event.to_dict()['orchestration_state']['value']
        assert exported == {'k': [({'label': 'c', 'when': _TS.isoformat()},)]}

    def test_empty_tuple_round_trips(self) -> None:
        event = _state_event(())
        assert event.to_dict()['orchestration_state']['value'] == ()

    @pytest.mark.parametrize(
        'payload',
        [
            pytest.param((_Inner('a', _TS),), id='tuple-of-dataclass'),
            pytest.param((_TS,), id='tuple-of-datetime'),
            pytest.param(_Point(_TS, 2), id='namedtuple-of-datetime'),
            pytest.param({'k': [(_TS,)]}, id='tuple-nested-in-dict-and-list'),
        ],
    )
    def test_tuple_payloads_are_json_serializable(self, payload: Any) -> None:
        """The old two-pass form produced tuples json.dumps could not encode."""
        event = _state_event(payload)
        json.dumps(event.to_dict())

    @pytest.mark.parametrize(
        'payload',
        [
            pytest.param((_Inner('a', _TS),), id='tuple-of-dataclass'),
            pytest.param((_TS,), id='tuple-of-datetime'),
        ],
    )
    def test_tuple_output_deliberately_diverges_from_legacy(
        self, payload: Any,
    ) -> None:
        """Pin the divergence so it stays intentional rather than accidental.

        The legacy output is not JSON encodable for these payloads; the
        new output is. This test fails loudly if anyone ever "restores"
        bug-for-bug parity with the old two-pass form.
        """
        event = _state_event(payload)

        legacy = _legacy_to_dict(event)
        with pytest.raises(TypeError):
            json.dumps(legacy)

        actual = event.to_dict()
        assert actual != legacy
        json.dumps(actual)
