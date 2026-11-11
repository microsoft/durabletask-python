# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from collections.abc import Generator
from typing import cast

import pytest

from durabletask.entities import EntityInstanceId
from durabletask.internal.orchestration_entity_context import OrchestrationEntityContext


class _ProtocolAwareLocks(list[EntityInstanceId]):
    def __init__(self, locks: list[EntityInstanceId]) -> None:
        super().__init__(locks)
        self.received_exception = False
        self.closed = False
        self.iterator = self._iterate()

    def _iterate(self) -> Generator[EntityInstanceId, None, None]:
        try:
            yield from super().__iter__()
        except ValueError:
            self.received_exception = True
            raise
        finally:
            self.closed = True

    def __iter__(self) -> Generator[EntityInstanceId, None, None]:
        return self.iterator


def _context_with_locks(locks: list[EntityInstanceId]) -> OrchestrationEntityContext:
    context = OrchestrationEntityContext("owner")
    context.critical_section_id = "section"
    context.available_locks = locks
    return context


def test_available_entities_follow_critical_section_lock_order() -> None:
    first = EntityInstanceId("counter", "a")
    second = EntityInstanceId("counter", "b")
    context = OrchestrationEntityContext("owner")

    assert list(context.get_available_entities()) == []
    context.emit_acquire_message("section", [second, first])
    assert list(context.get_available_entities()) == []
    context.complete_acquire("section")
    assert list(context.get_available_entities()) == [first, second]

    assert context.validate_operation_transition(first, one_way=False) == (True, "")
    assert list(context.get_available_entities()) == [second]
    context.recover_lock_after_call(first)
    assert list(context.get_available_entities()) == [second, first]

    list(context.emit_lock_release_messages())
    assert list(context.get_available_entities()) == []


def test_available_entities_ignores_sent_values() -> None:
    locks = [EntityInstanceId("counter", key) for key in ("a", "b", "c")]
    context = _context_with_locks(locks)
    # The runtime accepts non-None sends even though the annotation requires None.
    available = cast(Generator[EntityInstanceId, object, None], context.get_available_entities())

    assert next(available) == locks[0]
    assert available.send(None) == locks[1]
    assert available.send("ignored") == locks[2]
    with pytest.raises(StopIteration):
        next(available)


def test_available_entities_does_not_forward_throw() -> None:
    first = EntityInstanceId("counter", "a")
    second = EntityInstanceId("counter", "b")
    locks = _ProtocolAwareLocks([first, second])
    available = _context_with_locks(locks).get_available_entities()

    assert next(available) == first
    with pytest.raises(ValueError, match="outer generator"):
        available.throw(ValueError("outer generator"))
    assert not locks.received_exception
    assert not locks.closed
    assert next(locks.iterator) == second
    locks.iterator.close()


def test_available_entities_does_not_forward_close() -> None:
    first = EntityInstanceId("counter", "a")
    second = EntityInstanceId("counter", "b")
    locks = _ProtocolAwareLocks([first, second])
    available = _context_with_locks(locks).get_available_entities()

    assert next(available) == first
    available.close()
    assert not locks.closed
    assert next(locks.iterator) == second
    locks.iterator.close()
