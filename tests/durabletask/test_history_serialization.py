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
from collections import defaultdict
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


@dataclass(frozen=True)
class _FrozenKey:
    """A hashable dataclass, usable as a mapping key until ``asdict`` runs.

    ``asdict`` recurses into keys, turning this into an unhashable dict,
    so using one as a key has always raised. That failure must survive.
    """

    name: str


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


class _OddIsoDatetime(datetime):
    """A datetime subclass that overrides the method the walker calls."""

    def isoformat(self, sep: str = 'T', timespec: str = 'auto') -> str:
        return 'CUSTOM:' + super().isoformat(sep, timespec)


class _OddCopyDatetime(datetime):
    """A datetime subclass that changes when it is deep-copied.

    ``asdict`` deep-copied every leaf, so the conversion pass that ran
    afterwards called ``isoformat`` on the *copy*, not the original. That
    is only observable when copying is not the identity, which is exactly
    what this fixture makes it.
    """

    def __deepcopy__(self, memo: dict[int, Any]) -> datetime:
        return datetime(1999, 9, 9, 9, 9, 9)


class _DedupeList(list[Any]):
    """A list subclass whose constructor changes the contents.

    The constructor deliberately is *not* idempotent: it appends a marker
    every time it runs. ``asdict`` rebuilt list subclasses through their
    own constructor, so the marker appears exactly once in the legacy
    output. Walking the original in place instead would skip it, so an
    idempotent constructor here would let that regression pass unnoticed.
    """

    def __init__(self, items: Any = ()) -> None:
        seen: list[Any] = []
        for item in items:
            if item not in seen:
                seen.append(item)
        seen.append('ctor-ran')
        super().__init__(seen)


class _UpperDict(dict[Any, Any]):
    """A dict subclass whose constructor changes the mapping.

    Like :class:`_DedupeList`, this records that it ran so that skipping
    the constructor is detectable rather than silently equivalent.
    """

    def __init__(self, items: Any = ()) -> None:
        rebuilt = {
            key.upper() if isinstance(key, str) else key: value
            for key, value in dict(items).items()
        }
        rebuilt['ctor-ran'] = True
        super().__init__(rebuilt)


class _IterOnlyTuple(tuple[Any, ...]):
    """A tuple subclass that only accepts an iterator, never a sequence.

    ``asdict`` rebuilds tuple subclasses from a generator expression, so a
    constructor like this one round-trips fine. Passing an already-built
    list instead would raise, which is what makes this a regression guard.
    """

    def __new__(cls, items: Any = ()) -> '_IterOnlyTuple':
        if isinstance(items, (list, tuple)):
            raise TypeError('iterator-only constructor')
        return super().__new__(cls, items)


class _DeepCopyKey:
    """A mapping key that turns into a plain string when deep-copied."""

    def __init__(self, name: str) -> None:
        self.name = name

    def __deepcopy__(self, memo: dict[int, Any]) -> str:
        return 'deepcopied:' + self.name

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _DeepCopyKey) and other.name == self.name


_EXOTIC_PAYLOADS = [
    pytest.param(_OddIsoDatetime(2024, 1, 2, 3, 4, 5), id='datetime-subclass'),
    pytest.param(
        _OddCopyDatetime(2024, 1, 2, 3, 4, 5), id='datetime-subclass-deepcopy',
    ),
    pytest.param(_DedupeList([1, 2, 2, 3]), id='list-subclass'),
    pytest.param(_DedupeList([_TS, _TS]), id='list-subclass-of-datetime'),
    pytest.param(_UpperDict({'a': 1, 'b': 2}), id='dict-subclass'),
    pytest.param(defaultdict(int, {'a': 1}), id='defaultdict'),
    pytest.param(_IterOnlyTuple(iter([1, 2])), id='tuple-subclass-iterator-only'),
    pytest.param((1, 'x'), id='plain-tuple'),
    pytest.param((), id='empty-tuple'),
    pytest.param((_TS,), id='tuple-of-datetime'),
    pytest.param((_Inner('a', _TS),), id='tuple-of-dataclass'),
    pytest.param(_Point(1, 2), id='namedtuple'),
    pytest.param(_Point(_TS, _Inner('b', _TS)), id='namedtuple-of-datetime'),
    pytest.param({'k': [(_Inner('c', _TS),)]}, id='tuple-nested-in-dict-and-list'),
    pytest.param({_DeepCopyKey('k'): 1}, id='dict-with-deepcopy-key'),
    pytest.param({_TS: 'v'}, id='dict-with-datetime-key'),
    pytest.param({(_TS, 1): 'v'}, id='dict-with-tuple-key'),
    pytest.param({'plain': 1}, id='plain-dict'),
    pytest.param([1, 'x', None], id='plain-list'),
    pytest.param({1, 2, 3}, id='set'),
    pytest.param(bytearray(b'abc'), id='bytearray'),
    pytest.param(b'abc', id='bytes'),
    pytest.param(_MutableLeaf(1), id='custom-object'),
]


class TestLegacyCompatibility:
    """Values the fast paths do not cover must behave exactly as before.

    The optimization only walks a value in place when doing so is provably
    identical to what ``dataclasses.asdict`` produced, which is true for
    the exact built-in types and for dataclass instances. Everything else
    -- container *subclasses*, tuples, namedtuples, custom objects and
    exotic mapping keys -- carries semantics that lived inside ``asdict``
    itself: constructor round-trips, key recursion and deep-copied leaves.
    Those values are handed back to the real ``asdict``, so this suite
    pins that routing.

    The oracle here is ``asdict`` applied to the whole event, not to an
    individual value, so it never re-enters the implementation being
    tested.
    """

    @pytest.mark.parametrize('payload', _EXOTIC_PAYLOADS)
    def test_exotic_payload_matches_legacy_two_pass_output(
        self, payload: Any,
    ) -> None:
        event = _state_event(payload)

        try:
            expected = _legacy_to_dict(event)
        except Exception:  # noqa: BLE001 - see the sibling parity test
            # A few payloads cannot be serialized by ``asdict`` at all on
            # some interpreters -- ``defaultdict`` before 3.12, say. There
            # is no legacy value to compare against, so parity for those is
            # asserted by ``test_exotic_payload_raises_the_same_way_as_legacy``
            # instead of being silently weakened here.
            pytest.skip('legacy raises for this payload on this interpreter')

        actual = event.to_dict()

        assert actual == expected
        # ``==`` alone would let a namedtuple compare equal to a plain
        # tuple, so pin the exact repr as well.
        assert repr(actual) == repr(expected)

    @pytest.mark.parametrize('payload', _EXOTIC_PAYLOADS)
    def test_exotic_payload_raises_the_same_way_as_legacy(
        self, payload: Any,
    ) -> None:
        """Inputs that used to fail must still fail identically."""
        event = _state_event(payload)

        try:
            expected: Any = _legacy_to_dict(event)
        except Exception as exc:  # noqa: BLE001 - mirroring legacy behavior
            with pytest.raises(type(exc)):
                event.to_dict()
        else:
            assert event.to_dict() == expected

    def test_defaultdict_follows_the_running_interpreter(self) -> None:
        """``asdict`` changed how it rebuilds ``defaultdict`` in 3.12.

        Earlier versions rebuilt every ``dict`` subclass with
        ``type(obj)(<generator>)``, which ``defaultdict`` rejects with a
        ``TypeError``; 3.12 added a branch that forwards ``default_factory``.
        Delegating to the running interpreter's ``asdict`` inherits whichever
        applies, so this asserts the two agree rather than pinning one
        version's answer -- hand-rolling ``_asdict_inner``'s branches would
        succeed on 3.10 and 3.11 where the legacy pipeline raises.
        """
        event = _state_event(defaultdict(int, {'a': 1}))

        try:
            expected: Any = _legacy_to_dict(event)
        except TypeError:
            with pytest.raises(TypeError):
                event.to_dict()
        else:
            assert event.to_dict() == expected

    def test_dict_key_that_cannot_be_rebuilt_still_raises(self) -> None:
        """``asdict`` turned dataclass keys into unhashable dicts; keep that."""
        event = _state_event({_FrozenKey('z'): 1})

        with pytest.raises(TypeError):
            _legacy_to_dict(event)
        with pytest.raises(TypeError):
            event.to_dict()

    def test_datetime_subclass_conversion_runs_on_a_copy(self) -> None:
        """``asdict`` deep-copied before the walker called ``isoformat``."""
        event = _state_event(_OddIsoDatetime(2024, 1, 2, 3, 4, 5))

        exported = event.to_dict()['orchestration_state']['value']

        assert exported == 'CUSTOM:2024-01-02T03:04:05'

    def test_datetime_subclass_deepcopy_substitution_is_honored(self) -> None:
        """A subclass whose copy differs must be converted via that copy."""
        event = _state_event(_OddCopyDatetime(2024, 1, 2, 3, 4, 5))

        exported = event.to_dict()['orchestration_state']['value']

        assert exported == '1999-09-09T09:09:09'

    def test_list_subclass_constructor_still_runs(self) -> None:
        """The second marker only appears if the constructor re-ran.

        The instance already carries one marker from being built. The
        legacy pipeline rebuilt it through its own constructor, adding a
        second. Walking the original in place would emit only the first.
        """
        event = _state_event(_DedupeList([1, 2, 2, 3]))

        exported = event.to_dict()['orchestration_state']['value']

        assert exported == [1, 2, 3, 'ctor-ran', 'ctor-ran']

    def test_dict_subclass_constructor_still_runs(self) -> None:
        """``CTOR-RAN`` is the first marker, re-keyed by the second run."""
        event = _state_event(_UpperDict({'a': 1, 'b': 2}))

        exported = event.to_dict()['orchestration_state']['value']

        assert exported == {
            'A': 1, 'B': 2, 'CTOR-RAN': True, 'ctor-ran': True,
        }

    def test_tuple_subclass_is_rebuilt_from_an_iterator(self) -> None:
        """Handing the constructor a pre-built list instead would raise."""
        event = _state_event(_IterOnlyTuple(iter([1, 2])))

        exported = event.to_dict()['orchestration_state']['value']

        assert tuple(exported) == (1, 2)

    def test_namedtuple_type_is_preserved(self) -> None:
        event = _state_event(_Point(1, 2))

        exported = event.to_dict()['orchestration_state']['value']

        assert isinstance(exported, _Point)
        assert repr(exported) == repr(_Point(1, 2))

    def test_mapping_key_is_deep_copied_like_asdict(self) -> None:
        event = _state_event({_DeepCopyKey('k'): 1})

        exported = event.to_dict()['orchestration_state']['value']

        assert exported == {'deepcopied:k': 1}

    def test_mapping_key_is_not_converted_by_the_value_walker(self) -> None:
        """``asdict`` recursed into keys, but the walker after it did not.

        A datetime key therefore stayed a datetime rather than becoming an
        ISO string, unlike a datetime *value*.
        """
        event = _state_event({_TS: 'v'})

        exported = event.to_dict()['orchestration_state']['value']

        assert list(exported) == [_TS]


class TestTupleHandling:
    """Tuples keep their original ``asdict`` semantics.

    An earlier revision of this change added a dedicated tuple branch that
    normalized datetimes inside tuples. That was a behavior change rather
    than a performance change, so it was removed: tuples now route to the
    compatibility path and come back exactly as they did before. The
    quirk that a datetime nested in a tuple is left unconverted is
    therefore preserved, quirk and all.
    """

    def test_tuple_type_is_preserved(self) -> None:
        event = _state_event((1, 'x'))
        exported = event.to_dict()['orchestration_state']['value']
        assert isinstance(exported, tuple)

    def test_empty_tuple_round_trips(self) -> None:
        event = _state_event(())
        assert event.to_dict()['orchestration_state']['value'] == ()

    def test_dataclass_inside_tuple_becomes_a_dict(self) -> None:
        """``asdict`` recursed into tuples, so this part always worked."""
        event = _state_event((_Inner('a', _TS),))
        exported = event.to_dict()['orchestration_state']['value']
        assert exported == ({'label': 'a', 'when': _TS},)

    def test_datetime_inside_tuple_is_left_unconverted(self) -> None:
        """Pins the pre-existing quirk this change must not silently fix."""
        event = _state_event((_TS,))
        exported = event.to_dict()['orchestration_state']['value']
        assert exported == (_TS,)

    @pytest.mark.parametrize(
        'payload',
        [
            pytest.param((_TS,), id='tuple-of-datetime'),
            pytest.param((_Inner('a', _TS),), id='tuple-of-dataclass'),
        ],
    )
    def test_tuple_datetimes_remain_unencodable_like_legacy(
        self, payload: Any,
    ) -> None:
        """The quirk makes these payloads unencodable; legacy did the same.

        Fixing that is a genuine bug fix, but a user-visible one, so it
        belongs in its own change rather than riding along with a
        performance optimization.
        """
        event = _state_event(payload)

        with pytest.raises(TypeError):
            json.dumps(_legacy_to_dict(event))
        with pytest.raises(TypeError):
            json.dumps(event.to_dict())
