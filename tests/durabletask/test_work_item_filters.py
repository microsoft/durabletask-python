# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import pytest

import durabletask.internal.orchestrator_service_pb2 as pb
from durabletask.worker import (
    ActivityWorkItemFilter,
    EntityWorkItemFilter,
    OrchestrationWorkItemFilter,
    TaskHubGrpcWorker,
    VersioningOptions,
    VersionMatchStrategy,
    WorkItemFilters,
    _Registry,
)


# ---------------------------------------------------------------------------
# OrchestrationWorkItemFilter / ActivityWorkItemFilter / EntityWorkItemFilter
# ---------------------------------------------------------------------------

class TestOrchestrationWorkItemFilter:
    def test_defaults(self):
        f = OrchestrationWorkItemFilter(name="MyOrch")
        assert f.name == "MyOrch"
        assert f.versions == []

    def test_with_versions(self):
        f = OrchestrationWorkItemFilter(name="MyOrch", versions=["1.0", "2.0"])
        assert f.versions == ["1.0", "2.0"]


class TestActivityWorkItemFilter:
    def test_defaults(self):
        f = ActivityWorkItemFilter(name="MyActivity")
        assert f.name == "MyActivity"
        assert f.versions == []

    def test_with_versions(self):
        f = ActivityWorkItemFilter(name="MyActivity", versions=["3.0"])
        assert f.versions == ["3.0"]


class TestEntityWorkItemFilter:
    def test_defaults(self):
        f = EntityWorkItemFilter(name="myentity")
        assert f.name == "myentity"

    def test_name_normalized_to_lowercase(self):
        f = EntityWorkItemFilter(name="Counter")
        assert f.name == "counter"

    def test_invalid_name_raises(self):
        with pytest.raises(ValueError):
            EntityWorkItemFilter(name="bad@name")

    def test_empty_name_raises(self):
        with pytest.raises(ValueError):
            EntityWorkItemFilter(name="")


# ---------------------------------------------------------------------------
# WorkItemFilters construction
# ---------------------------------------------------------------------------

class TestWorkItemFilters:
    def test_defaults_empty(self):
        filters = WorkItemFilters()
        assert filters.orchestrations == []
        assert filters.activities == []
        assert filters.entities == []

    def test_explicit_values(self):
        orch = [OrchestrationWorkItemFilter(name="Orch1")]
        act = [ActivityWorkItemFilter(name="Act1")]
        ent = [EntityWorkItemFilter(name="ent1")]
        filters = WorkItemFilters(
            orchestrations=orch, activities=act, entities=ent
        )
        assert len(filters.orchestrations) == 1
        assert filters.orchestrations[0].name == "Orch1"
        assert len(filters.activities) == 1
        assert filters.activities[0].name == "Act1"
        assert len(filters.entities) == 1
        assert filters.entities[0].name == "ent1"


# ---------------------------------------------------------------------------
# WorkItemFilters._from_registry
# ---------------------------------------------------------------------------

def _make_orchestrator(name):
    """Create a minimal orchestrator function with the given name."""
    def orchestrator(ctx, input):
        yield  # pragma: no cover
    orchestrator.__name__ = name
    return orchestrator


def _make_activity(name):
    """Create a minimal activity function with the given name."""
    def activity(ctx, input):
        return None  # pragma: no cover
    activity.__name__ = name
    return activity


def _make_entity(name):
    """Create a minimal entity function with the given name."""
    def entity(ctx, state, input):
        return None  # pragma: no cover
    entity.__name__ = name
    return entity


class TestFromRegistry:
    def test_empty_registry(self):
        reg = _Registry()
        filters = WorkItemFilters._from_registry(reg)
        assert filters.orchestrations == []
        assert filters.activities == []
        assert filters.entities == []

    def test_orchestrators_and_activities(self):
        reg = _Registry()
        reg.add_orchestrator(_make_orchestrator("Orch1"))
        reg.add_orchestrator(_make_orchestrator("Orch2"))
        reg.add_activity(_make_activity("Act1"))

        filters = WorkItemFilters._from_registry(reg)

        orch_names = {f.name for f in filters.orchestrations}
        assert orch_names == {"Orch1", "Orch2"}
        assert all(f.versions == [] for f in filters.orchestrations)

        assert len(filters.activities) == 1
        assert filters.activities[0].name == "Act1"
        assert filters.activities[0].versions == []

        assert filters.entities == []

    def test_entities(self):
        reg = _Registry()
        reg.add_entity(_make_entity("counter"), name="counter")

        filters = WorkItemFilters._from_registry(reg)

        assert len(filters.entities) == 1
        assert filters.entities[0].name == "counter"

    def test_no_versioning(self):
        """Without versioning, versions should be empty."""
        reg = _Registry()
        reg.add_orchestrator(_make_orchestrator("Orch"))
        reg.add_activity(_make_activity("Act"))

        filters = WorkItemFilters._from_registry(reg)
        assert filters.orchestrations[0].versions == []
        assert filters.activities[0].versions == []

    def test_versioning_none_strategy(self):
        """NONE match strategy should produce empty versions."""
        reg = _Registry()
        reg.add_orchestrator(_make_orchestrator("Orch"))
        reg.versioning = VersioningOptions(
            version="1.0", match_strategy=VersionMatchStrategy.NONE
        )

        filters = WorkItemFilters._from_registry(reg)
        assert filters.orchestrations[0].versions == []

    def test_versioning_current_or_older(self):
        """CURRENT_OR_OLDER match strategy should produce empty versions."""
        reg = _Registry()
        reg.add_orchestrator(_make_orchestrator("Orch"))
        reg.versioning = VersioningOptions(
            version="1.5", match_strategy=VersionMatchStrategy.CURRENT_OR_OLDER
        )

        filters = WorkItemFilters._from_registry(reg)
        assert filters.orchestrations[0].versions == []

    def test_versioning_strict(self):
        """STRICT match strategy should populate versions."""
        reg = _Registry()
        reg.add_orchestrator(_make_orchestrator("Orch"))
        reg.add_activity(_make_activity("Act"))
        reg.versioning = VersioningOptions(
            version="2.0", match_strategy=VersionMatchStrategy.STRICT
        )

        filters = WorkItemFilters._from_registry(reg)
        assert filters.orchestrations[0].versions == ["2.0"]
        assert filters.activities[0].versions == ["2.0"]

    def test_versioning_strict_no_version_string(self):
        """STRICT without a version string should produce empty versions."""
        reg = _Registry()
        reg.add_orchestrator(_make_orchestrator("Orch"))
        reg.versioning = VersioningOptions(
            version=None, match_strategy=VersionMatchStrategy.STRICT
        )

        filters = WorkItemFilters._from_registry(reg)
        assert filters.orchestrations[0].versions == []


# ---------------------------------------------------------------------------
# WorkItemFilters._to_grpc
# ---------------------------------------------------------------------------

class TestToGrpc:
    def test_empty_filters(self):
        grpc_msg = WorkItemFilters()._to_grpc()
        assert isinstance(grpc_msg, pb.WorkItemFilters)
        assert len(grpc_msg.orchestrations) == 0
        assert len(grpc_msg.activities) == 0
        assert len(grpc_msg.entities) == 0

    def test_orchestration_filter(self):
        filters = WorkItemFilters(
            orchestrations=[
                OrchestrationWorkItemFilter(name="Orch1", versions=["1.0", "2.0"]),
            ]
        )
        grpc_msg = filters._to_grpc()
        assert len(grpc_msg.orchestrations) == 1
        assert grpc_msg.orchestrations[0].name == "Orch1"
        assert list(grpc_msg.orchestrations[0].versions) == ["1.0", "2.0"]

    def test_activity_filter(self):
        filters = WorkItemFilters(
            activities=[
                ActivityWorkItemFilter(name="Act1", versions=["3.0"]),
                ActivityWorkItemFilter(name="Act2"),
            ]
        )
        grpc_msg = filters._to_grpc()
        assert len(grpc_msg.activities) == 2
        assert grpc_msg.activities[0].name == "Act1"
        assert list(grpc_msg.activities[0].versions) == ["3.0"]
        assert grpc_msg.activities[1].name == "Act2"
        assert list(grpc_msg.activities[1].versions) == []

    def test_entity_filter(self):
        filters = WorkItemFilters(
            entities=[EntityWorkItemFilter(name="counter")]
        )
        grpc_msg = filters._to_grpc()
        assert len(grpc_msg.entities) == 1
        assert grpc_msg.entities[0].name == "counter"

    def test_full_round_trip(self):
        """All three filter types convert correctly."""
        filters = WorkItemFilters(
            orchestrations=[OrchestrationWorkItemFilter(name="O", versions=["1"])],
            activities=[ActivityWorkItemFilter(name="A")],
            entities=[EntityWorkItemFilter(name="e")],
        )
        grpc_msg = filters._to_grpc()
        assert grpc_msg.orchestrations[0].name == "O"
        assert grpc_msg.activities[0].name == "A"
        assert grpc_msg.entities[0].name == "e"


# ---------------------------------------------------------------------------
# TaskHubGrpcWorker.use_work_item_filters
# ---------------------------------------------------------------------------

class TestUseWorkItemFilters:
    def test_auto_generate_default(self):
        """Calling with no arguments enables auto-generation."""
        w = TaskHubGrpcWorker()
        w.use_work_item_filters()
        assert w._auto_generate_work_item_filters is True
        assert w._work_item_filters is None

    def test_explicit_filters(self):
        """Passing a WorkItemFilters instance stores it directly."""
        w = TaskHubGrpcWorker()
        custom = WorkItemFilters(
            orchestrations=[OrchestrationWorkItemFilter(name="MyOrch")]
        )
        w.use_work_item_filters(custom)
        assert w._auto_generate_work_item_filters is False
        assert w._work_item_filters is custom
        assert len(w._work_item_filters.orchestrations) == 1

    def test_clear_filters_with_none(self):
        """Passing None clears previously set filters."""
        w = TaskHubGrpcWorker()
        # First set some filters
        w.use_work_item_filters(WorkItemFilters(
            orchestrations=[OrchestrationWorkItemFilter(name="X")]
        ))
        assert w._work_item_filters is not None

        # Now clear
        w.use_work_item_filters(None)
        assert w._auto_generate_work_item_filters is False
        assert w._work_item_filters is None

    def test_clear_auto_generate_with_none(self):
        """Passing None after auto-generate clears the auto flag."""
        w = TaskHubGrpcWorker()
        w.use_work_item_filters()
        assert w._auto_generate_work_item_filters is True

        w.use_work_item_filters(None)
        assert w._auto_generate_work_item_filters is False
        assert w._work_item_filters is None

    def test_invalid_type_raises(self):
        """Passing an unsupported type raises TypeError."""
        w = TaskHubGrpcWorker()
        with pytest.raises(TypeError, match="WorkItemFilters instance"):
            w.use_work_item_filters("invalid")  # type: ignore

    def test_raises_when_running(self):
        """Cannot change filters while worker is running."""
        w = TaskHubGrpcWorker()
        w._is_running = True
        with pytest.raises(RuntimeError, match="cannot be changed while the worker is running"):
            w.use_work_item_filters()

    def test_default_no_filters(self):
        """By default, no filters are set (opt-in model)."""
        w = TaskHubGrpcWorker()
        assert w._work_item_filters is None
        assert w._auto_generate_work_item_filters is False
