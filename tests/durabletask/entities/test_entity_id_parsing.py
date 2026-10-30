# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import copy
import pickle

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


def test_entity_instance_id_immutable():
    """Verify that entity and key cannot be reassigned or deleted after construction,
    ensuring hash stability when instances are stored in sets or dicts."""
    eid = EntityInstanceId("Counter", "key1")

    with pytest.raises(AttributeError):
        eid.entity = "other"

    with pytest.raises(AttributeError):
        eid.key = "other_key"

    with pytest.raises(AttributeError):
        eid.custom_attr = "value"

    with pytest.raises(AttributeError):
        del eid.entity

    with pytest.raises(AttributeError):
        del eid.key

    # Verify the original values are unchanged
    assert eid.entity == "counter"
    assert eid.key == "key1"


def test_entity_instance_id_immutable_dict_stability():
    """Verify that immutability preserves dict/set lookup integrity."""
    eid = EntityInstanceId("Counter", "key1")
    mapping = {eid: "value"}
    s = {eid}

    # Attempt mutation - should be rejected
    with pytest.raises(AttributeError):
        eid.entity = "other"

    # Lookups with an equal instance must still succeed
    lookup = EntityInstanceId("counter", "key1")
    assert mapping[lookup] == "value"
    assert lookup in s


def test_entity_instance_id_copy_and_pickle():
    """Verify that immutable EntityInstanceId instances can be copied and pickled."""
    eid = EntityInstanceId("Counter", "key1")

    # Copying should preserve identity / immutability
    assert copy.copy(eid) is eid
    assert copy.deepcopy(eid) is eid

    # Pickling should preserve equality and hash
    pickled = pickle.loads(pickle.dumps(eid))
    assert pickled == eid
    assert hash(pickled) == hash(eid)
    assert pickled.entity == "counter"
    assert pickled.key == "key1"


class _SubclassEntityId(EntityInstanceId):
    @property
    def custom_name(self) -> str:
        return self.entity.upper()


def test_entity_instance_id_subclass_copy():
    custom = _SubclassEntityId("Counter", "key1")
    assert copy.copy(custom) is custom
    assert copy.deepcopy(custom) is custom
    constructor, args = custom.__reduce__()
    assert constructor is _SubclassEntityId
    assert args == ("counter", "key1")


def test_entity_instance_id_subclass_pickle():
    """Verify that unpickling a subclass of EntityInstanceId preserves the concrete type."""
    custom = _SubclassEntityId("Counter", "key1")
    assert isinstance(custom, _SubclassEntityId)
    assert custom.custom_name == "COUNTER"

    restored = pickle.loads(pickle.dumps(custom))
    assert type(restored) is _SubclassEntityId
    assert isinstance(restored, _SubclassEntityId)
    assert restored.custom_name == "COUNTER"
    assert restored.entity == "counter"
    assert restored.key == "key1"
    assert restored == custom
    assert hash(restored) == hash(custom)

    # Immutability guarantees remain intact after unpickling
    with pytest.raises(AttributeError):
        restored.entity = "other"
    with pytest.raises(AttributeError):
        restored.key = "other"
    with pytest.raises(AttributeError):
        del restored.entity
