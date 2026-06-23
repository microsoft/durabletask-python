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
