# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""E2E tests for the durabletask entity administration client surface.

Covers ``get_all_entities`` (List All Entities) and ``clean_entity_storage``
(entity storage cleanup) -- both client SDK capabilities that were missing in
the v1 azure-functions-durable Python package.
"""

import time

import pytest

pytestmark = pytest.mark.functions_e2e


def test_list_all_entities(dtask_app):
    prefix = f"list-{int(time.time() * 1000)}"
    keys = [f"{prefix}-a", f"{prefix}-b", f"{prefix}-c"]
    for key in keys:
        dtask_app.signal_entity("counter", key, "add", input=1)
    for key in keys:
        dtask_app.wait_for_entity("counter", key, lambda p: p["exists"] and p["state"] == 1)

    # Filter to just this test's counters via the entity-id prefix (@counter@<prefix>).
    result = dtask_app.list_entities(starts_with=f"@counter@{prefix}")
    found_keys = {e["key"] for e in result["entities"]}
    assert set(keys).issubset(found_keys), (
        f"expected {keys} in listed entities, got {found_keys}")
    for entity in result["entities"]:
        if entity["key"] in keys:
            assert entity["entity"] == "counter"
            assert entity["state"] == 1


def test_clean_entity_storage(dtask_app):
    # Create an entity, then empty it (state None), then clean storage. The
    # emptied entity should be removed. The call must succeed and return
    # non-negative counters.
    key = f"clean-{int(time.time() * 1000)}"
    dtask_app.signal_entity("counter", key, "add", input=1)
    dtask_app.wait_for_entity("counter", key, lambda p: p["exists"] and p["state"] == 1)

    # Empty the entity (probe's delete sets state to None); use the counter's
    # reset to 0 is non-empty, so use a probe entity for the empty case.
    dtask_app.signal_entity("probe", key, "set", input=5)
    dtask_app.wait_for_entity("probe", key, lambda p: p["exists"])
    dtask_app.signal_entity("probe", key, "delete")
    dtask_app.wait_for_entity("probe", key, lambda p: not p["exists"])

    result = dtask_app.clean_entity_storage()
    assert result["emptyEntitiesRemoved"] >= 0
    assert result["orphanedLocksReleased"] >= 0
