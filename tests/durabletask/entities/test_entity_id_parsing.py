# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import pytest
from durabletask.entities import EntityInstanceId


def test_entity_id_parsing_success():
    entity_id_str = "@MyEntity@TestInstance"
    entity_id = EntityInstanceId.parse(entity_id_str)
    assert entity_id.entity == "myentity"  # should be case-insensitive (lowercased)
    assert entity_id.key == "TestInstance"
    assert str(entity_id) == "@myentity@TestInstance"


def test_is_entity_id_name_case_insensitive():
    id1 = EntityInstanceId("MyEntity", "instance1")
    id2 = EntityInstanceId("myentity", "instance1")
    assert id1 == id2


def test_entity_id_parsing_failures():
    # Test empty string
    with pytest.raises(ValueError):
        EntityInstanceId.parse("")

    # Test invalid entity id format
    with pytest.raises(ValueError):
        EntityInstanceId.parse("invalidEntityId")

    # Test single @
    with pytest.raises(ValueError):
        EntityInstanceId.parse("@")

    # Test double @
    with pytest.raises(ValueError):
        EntityInstanceId.parse("@@")

    # Test @ with invalid placement
    with pytest.raises(ValueError):
        EntityInstanceId.parse("@invalid@")

    # Test @@ at end
    with pytest.raises(ValueError):
        EntityInstanceId.parse("@@invalid")

    # Test symbol in wrong position
    with pytest.raises(ValueError):
        EntityInstanceId.parse("invalid@symbolplacement")

    # Test multiple @ symbols
    with pytest.raises(ValueError):
        EntityInstanceId.parse("invalid@symbol@placement")


def test_entity_instance_id_hash_and_equality():
    id1 = EntityInstanceId("MyEntity", "instance1")
    id2 = EntityInstanceId("myentity", "instance1")
    id3 = EntityInstanceId("myentity", "instance2")
    id4 = EntityInstanceId("other", "instance1")

    assert hash(id1) == hash(id2)
    assert hash(id1) != hash(id3)
    assert hash(id1) != hash(id4)


def test_entity_instance_id_in_set():
    id1 = EntityInstanceId("MyEntity", "key1")
    id2 = EntityInstanceId("myentity", "key1")
    id3 = EntityInstanceId("myentity", "key2")

    s = {id1, id2, id3}
    assert len(s) == 2
    assert EntityInstanceId("myentity", "key1") in s
    assert EntityInstanceId("myentity", "key2") in s


def test_entity_instance_id_as_dict_key():
    id1 = EntityInstanceId("Counter", "key1")
    id2 = EntityInstanceId("counter", "key1")

    mapping = {id1: "value1"}
    assert mapping[id2] == "value1"
    mapping[id2] = "value2"
    assert len(mapping) == 1
    assert mapping[id1] == "value2"


def test_entity_instance_id_repr():
    eid = EntityInstanceId("MyEntity", "InstanceKey")
    expected = "EntityInstanceId(entity='myentity', key='InstanceKey')"
    assert repr(eid) == expected
