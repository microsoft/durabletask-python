# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Unit tests for the _EntityExecutor class in durabletask.worker."""
import inspect
import json
import logging
import threading

from google.protobuf import wrappers_pb2

import durabletask.internal.orchestrator_service_pb2 as pb
from durabletask import entities, worker
from durabletask.internal.entity_state_shim import StateShim
from durabletask.serialization import JsonDataConverter
from durabletask.worker import (
    TaskHubGrpcWorker,
    _EntityExecutor,
    _EntityMethodSignatureCache,
    _Registry,
)


def _make_executor(*entity_args) -> _EntityExecutor:
    """Helper to create an _EntityExecutor with registered entities."""
    registry = _Registry()
    for entity in entity_args:
        registry.add_entity(entity)
    return _EntityExecutor(registry, logging.getLogger("test"), JsonDataConverter())


def _execute(executor, entity_name, operation, encoded_input=None):
    """Helper to execute an entity operation."""
    entity_id = entities.EntityInstanceId(entity_name, "test-key")
    state = StateShim(None, JsonDataConverter())
    return executor.execute("test-orchestration", entity_id, operation, state, encoded_input)


class TestClassBasedEntityMethodDispatch:
    """Tests for class-based entity method dispatch in _EntityExecutor."""

    def test_method_with_no_input_parameter(self):
        """Methods that don't accept input should work without _=None."""
        class Counter(entities.DurableEntity):
            def get(self):
                return self.get_state(int, 0)

        executor = _make_executor(Counter)
        result = _execute(executor, "Counter", "get")
        assert result == "0"

    def test_method_with_input_parameter(self):
        """Methods that accept input should receive entity_input."""
        class Counter(entities.DurableEntity):
            def set(self, value: int):
                self.set_state(value)

        executor = _make_executor(Counter)
        result = _execute(executor, "Counter", "set", "42")
        assert result is None

    def test_method_with_input_returns_value(self):
        """Methods that accept input and return a value."""
        class Counter(entities.DurableEntity):
            def add(self, value: int):
                current = self.get_state(int, 0)
                new_value = current + value
                self.set_state(new_value)
                return new_value

        executor = _make_executor(Counter)
        result = _execute(executor, "Counter", "add", "5")
        assert result == "5"

    def test_mix_of_methods_with_and_without_input(self):
        """An entity with both input and no-input methods should work."""
        class Counter(entities.DurableEntity):
            def set(self, value: int):
                self.set_state(value)

            def get(self):
                return self.get_state(int, 0)

        executor = _make_executor(Counter)
        entity_id = entities.EntityInstanceId("Counter", "test-key")

        # set requires input
        state = StateShim(None, JsonDataConverter())
        executor.execute("test-orch", entity_id, "set", state, "10")
        state.commit()

        # get does not require input — reuse state to simulate persistence
        result = executor.execute("test-orch", entity_id, "get", state, None)
        assert result == "10"

    def test_method_with_optional_parameter_uses_default(self):
        """Methods with default parameters should use defaults when no input is provided."""
        class Counter(entities.DurableEntity):
            def add(self, value: int = 1):
                current = self.get_state(int, 0)
                new_value = current + value
                self.set_state(new_value)
                return new_value

        executor = _make_executor(Counter)

        # No input provided — should use default value of 1
        result = _execute(executor, "Counter", "add")
        assert result == "1"

    def test_method_with_optional_parameter_uses_provided_input(self):
        """Methods with default parameters should use provided input when given."""
        class Counter(entities.DurableEntity):
            def add(self, value: int = 1):
                current = self.get_state(int, 0)
                new_value = current + value
                self.set_state(new_value)
                return new_value

        executor = _make_executor(Counter)

        # Input provided — should use it instead of default
        result = _execute(executor, "Counter", "add", "5")
        assert result == "5"


class TestFunctionBasedEntityDispatch:
    """Tests for function-based entity dispatch in _EntityExecutor."""

    def test_function_entity_receives_context_and_input(self):
        """Function-based entities always receive (ctx, input)."""
        def counter(ctx: entities.EntityContext, input):
            if ctx.operation == "get":
                return ctx.get_state(int, 0)
            elif ctx.operation == "set":
                ctx.set_state(input)

        executor = _make_executor(counter)
        entity_id = entities.EntityInstanceId("counter", "test-key")
        state = StateShim(None, JsonDataConverter())

        executor.execute("test-orch", entity_id, "set", state, "42")
        state.commit()

        result = executor.execute("test-orch", entity_id, "get", state, None)
        assert result == "42"


class TestStateShimCoercion:
    """Tests for StateShim.get_state type coercion via the data converter."""

    def test_get_state_none_returns_default(self):
        state = StateShim(None, JsonDataConverter())
        assert state.get_state(int, 0) == 0

    def test_get_state_none_without_default_returns_none(self):
        state = StateShim(None, JsonDataConverter())
        assert state.get_state(int) is None

    def test_get_state_passes_through_matching_type(self):
        state = StateShim(5, JsonDataConverter())
        assert state.get_state(int) == 5

    def test_get_state_constructor_coercion(self):
        state = StateShim("5", JsonDataConverter())
        assert state.get_state(int) == 5

    def test_get_state_coerces_dataclass(self):
        from dataclasses import dataclass

        @dataclass
        class Counter:
            value: int

        # State is stored as a plain dict (as it would be after from_json).
        state = StateShim({"value": 7}, JsonDataConverter())
        result = state.get_state(Counter)
        assert isinstance(result, Counter)
        assert result.value == 7

    def test_get_state_uses_from_json_hook(self):
        class Wrapped:
            def __init__(self, n: int):
                self.n = n

            @classmethod
            def from_json(cls, data):
                return cls(data["n"])

        state = StateShim({"n": 3}, JsonDataConverter())
        result = state.get_state(Wrapped)
        assert isinstance(result, Wrapped)
        assert result.n == 3

    def test_get_state_invalid_coercion_raises(self):
        # An explicit intended_type that the state cannot be coerced to raises,
        # restoring the pre-existing strict contract for entity state access.
        import pytest

        state = StateShim("not-an-int", JsonDataConverter())
        with pytest.raises(TypeError):
            state.get_state(int)


class TestStateShimDeferredDeserialization:
    """Tests for StateShim deferring deserialization of the raw wire payload."""

    def test_constructor_does_not_deserialize_serialized_state(self):
        # A serialized payload is held verbatim until read, not eagerly parsed.
        state = StateShim('{"value": 7}', JsonDataConverter(), is_serialized=True)
        assert state._current_state == '{"value": 7}'

    def test_get_state_defers_deserialization_with_type(self):
        from dataclasses import dataclass

        @dataclass
        class Counter:
            value: int

        state = StateShim('{"value": 7}', JsonDataConverter(), is_serialized=True)
        result = state.get_state(Counter)
        assert isinstance(result, Counter)
        assert result.value == 7

    def test_get_state_no_type_returns_parsed_value(self):
        state = StateShim('{"value": 7}', JsonDataConverter(), is_serialized=True)
        assert state.get_state() == {"value": 7}

    def test_deferred_deserialization_passes_raw_string_to_converter(self):
        # A custom converter receives the original serialized string together
        # with the requested type, rather than a pre-parsed value.
        from typing import Any

        from durabletask.serialization import DataConverter

        seen: dict[str, Any] = {}

        class RecordingConverter(DataConverter):
            def serialize(self, value: Any) -> str | None:
                return None if value is None else json.dumps(value)

            def deserialize(self, data, target_type=None):
                seen["data"] = data
                seen["target_type"] = target_type
                return {"parsed": True}

            def coerce(self, value, target_type=None):
                seen["coerced"] = True
                return value

        state = StateShim('{"x": 1}', RecordingConverter(), is_serialized=True)
        state.get_state(dict)
        assert seen["data"] == '{"x": 1}'
        assert seen["target_type"] is dict
        assert "coerced" not in seen

    def test_encode_state_passes_through_unmodified_payload(self):
        # An unread/unmodified serialized payload is returned verbatim, never
        # re-serialized (which would double-encode the JSON string).
        state = StateShim('{"value": 7}', JsonDataConverter(), is_serialized=True)
        assert state.encode_state() == '{"value": 7}'

    def test_reading_does_not_trigger_double_encoding(self):
        state = StateShim('{"value": 7}', JsonDataConverter(), is_serialized=True)
        # Reading (even with a type) must not turn the payload into a live value
        # that would be re-serialized into a JSON-encoded string.
        state.get_state()
        state.get_state(dict)
        encoded = state.encode_state()
        assert encoded == '{"value": 7}'
        assert json.loads(encoded) == {"value": 7}

    def test_encode_state_serializes_live_value_after_set_state(self):
        state = StateShim('{"value": 7}', JsonDataConverter(), is_serialized=True)
        state.set_state({"value": 8})
        encoded = state.encode_state()
        assert json.loads(encoded) == {"value": 8}

    def test_encode_state_none_when_state_is_none(self):
        state = StateShim(None, JsonDataConverter(), is_serialized=True)
        assert state.encode_state() is None

    def test_commit_preserves_unmodified_payload(self):
        state = StateShim('{"value": 7}', JsonDataConverter(), is_serialized=True)
        state.commit()
        # After commit, the (unmodified) state still round-trips without
        # double-encoding.
        assert state.encode_state() == '{"value": 7}'

    def test_rollback_restores_unmodified_payload(self):
        state = StateShim('{"value": 7}', JsonDataConverter(), is_serialized=True)
        state.commit()
        state.set_state({"value": 99})
        state.rollback()
        assert state.encode_state() == '{"value": 7}'

    def test_falsy_serialized_state_is_not_dropped(self):
        # A serialized falsy value (e.g. 0) is preserved, not treated as cleared.
        state = StateShim("0", JsonDataConverter(), is_serialized=True)
        assert state.get_state(int) == 0
        assert state.encode_state() == "0"


class _CountingInspect:
    """Delegates to :mod:`inspect` while counting ``signature()`` calls."""

    def __init__(self):
        self.signature_calls: list[object] = []

    def signature(self, obj, *args, **kwargs):
        self.signature_calls.append(obj)
        return inspect.signature(obj, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(inspect, name)


class _RecordingEntityStub:
    """Captures the batch results the worker would send to the sidecar."""

    def __init__(self):
        self.completed: list[pb.EntityBatchResult] = []

    def CompleteEntityTask(self, batch_result):
        self.completed.append(batch_result)


def _entity_batch_request(instance_id: str, operations) -> pb.EntityBatchRequest:
    """Builds an entity batch request from (operation, encoded_input) pairs."""
    return pb.EntityBatchRequest(
        instanceId=instance_id,
        operations=[
            pb.OperationRequest(
                operation=name,
                requestId=str(index),
                input=(
                    wrappers_pb2.StringValue(value=encoded_input)
                    if encoded_input is not None
                    else None
                ),
            )
            for index, (name, encoded_input) in enumerate(operations)
        ],
    )


class Counter(entities.DurableEntity):
    """Entity exercising both input-argument and no-argument dispatch."""

    def add(self, value: int):
        current = self.get_state(int, 0)
        self.set_state(current + value)
        return current + value

    def get(self):
        return self.get_state(int, 0)


class TestEntityMethodSignatureCaching:
    """Reflection of an entity method signature must happen only once."""

    def test_repeated_operations_reflect_once_per_executor(self, monkeypatch):
        executor = _make_executor(Counter)
        counting = _CountingInspect()
        monkeypatch.setattr(worker, "inspect", counting)

        entity_id = entities.EntityInstanceId("Counter", "test-key")
        state = StateShim(None, JsonDataConverter())
        for _ in range(5):
            executor.execute("test-orch", entity_id, "add", state, "1")
            state.commit()

        assert len(counting.signature_calls) == 1
        assert state.get_state(int) == 5

    def test_cache_is_shared_between_executors(self, monkeypatch):
        shared_cache = _EntityMethodSignatureCache()
        registry = _Registry()
        registry.add_entity(Counter)
        counting = _CountingInspect()
        monkeypatch.setattr(worker, "inspect", counting)

        entity_id = entities.EntityInstanceId("Counter", "test-key")
        for _ in range(3):
            executor = _EntityExecutor(
                registry, logging.getLogger("test"), JsonDataConverter(),
                entity_method_cache=shared_cache,
            )
            executor.execute(
                "test-orch", entity_id, "add",
                StateShim(None, JsonDataConverter()), "1")

        assert len(counting.signature_calls) == 1

    def test_executor_without_shared_cache_still_caches(self, monkeypatch):
        # Constructing the executor with the historical three-argument form
        # keeps working and still avoids repeated reflection.
        registry = _Registry()
        registry.add_entity(Counter)
        executor = _EntityExecutor(
            registry, logging.getLogger("test"), JsonDataConverter())
        counting = _CountingInspect()
        monkeypatch.setattr(worker, "inspect", counting)

        entity_id = entities.EntityInstanceId("Counter", "test-key")
        state = StateShim(None, JsonDataConverter())
        executor.execute("test-orch", entity_id, "add", state, "1")
        executor.execute("test-orch", entity_id, "add", state, "1")

        assert len(counting.signature_calls) == 1


class TestEntityBatchSignatureCaching:
    """The worker must not recompute signatures for every batch operation."""

    @staticmethod
    def _make_worker() -> TaskHubGrpcWorker:
        instance = TaskHubGrpcWorker()
        instance.add_entity(Counter)
        return instance

    def test_batch_reflects_once_for_repeated_operations(self, monkeypatch):
        instance = self._make_worker()
        stub = _RecordingEntityStub()
        counting = _CountingInspect()
        monkeypatch.setattr(worker, "inspect", counting)

        request = _entity_batch_request("@counter@key", [("add", "1")] * 5)
        result = instance._execute_entity_batch(request, stub, b"token")

        assert [r.WhichOneof("resultType") for r in result.results] == ["success"] * 5
        assert json.loads(result.entityState.value) == 5
        assert len(counting.signature_calls) == 1
        assert len(stub.completed) == 1

    def test_cache_survives_across_batches(self, monkeypatch):
        instance = self._make_worker()
        stub = _RecordingEntityStub()
        counting = _CountingInspect()
        monkeypatch.setattr(worker, "inspect", counting)

        for _ in range(3):
            instance._execute_entity_batch(
                _entity_batch_request("@counter@key", [("add", "1")]), stub, b"token")

        assert len(counting.signature_calls) == 1

    def test_worker_cache_is_populated_by_batch(self):
        instance = self._make_worker()
        instance._execute_entity_batch(
            _entity_batch_request("@counter@key", [("add", "1")]),
            _RecordingEntityStub(), b"token")

        assert (Counter, "add") in instance._entity_method_cache

    def test_no_arg_and_input_arg_dispatch_preserved(self, monkeypatch):
        instance = self._make_worker()
        stub = _RecordingEntityStub()
        counting = _CountingInspect()
        monkeypatch.setattr(worker, "inspect", counting)

        request = _entity_batch_request(
            "@counter@key",
            [("add", "2"), ("get", None), ("add", "3"), ("get", None)],
        )
        result = instance._execute_entity_batch(request, stub, b"token")

        assert [r.success.result.value for r in result.results] == ["2", "2", "5", "5"]
        # One reflection per distinct operation, regardless of repeat count.
        assert len(counting.signature_calls) == 2

    def test_unknown_operation_failure_is_unchanged(self):
        instance = self._make_worker()
        result = instance._execute_entity_batch(
            _entity_batch_request("@counter@key", [("missing", None)]),
            _RecordingEntityStub(), b"token")

        failure = result.results[0]
        assert failure.WhichOneof("resultType") == "failure"
        details = failure.failure.failureDetails
        assert details.errorType == "builtins.AttributeError"
        assert "does not have operation 'missing'" in details.errorMessage


class TestEntityMethodSignatureCacheBounds:
    """The shared signature cache must stay bounded and thread-safe."""

    def test_get_returns_none_for_unknown_key(self):
        cache = _EntityMethodSignatureCache()
        assert cache.get((int, "missing")) is None

    def test_false_values_are_distinguishable_from_misses(self):
        cache = _EntityMethodSignatureCache()
        cache[(int, "op")] = False
        assert cache.get((int, "op")) is False
        assert (int, "op") in cache

    def test_evicts_oldest_entries_when_full(self):
        cache = _EntityMethodSignatureCache(max_size=2)
        cache[(int, "op")] = True
        cache[(str, "op")] = True
        cache[(float, "op")] = True

        assert len(cache) == 2
        assert cache.get((int, "op")) is None
        assert cache.get((str, "op")) is True
        assert cache.get((float, "op")) is True

    def test_concurrent_writes_stay_within_bound(self):
        cache = _EntityMethodSignatureCache(max_size=8)
        entity_types = [type(f"Entity{index}", (), {}) for index in range(64)]
        barrier = threading.Barrier(4)

        def writer(worker_index: int) -> None:
            barrier.wait()
            for index, entity_type in enumerate(entity_types):
                cache[(entity_type, f"op{worker_index}")] = index % 2 == 0

        threads = [threading.Thread(target=writer, args=(index,)) for index in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert len(cache) == 8
