# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Comprehensive round-trip tests for the default ``JsonDataConverter``.

These exercise ``serialize`` -> ``deserialize(..., target_type)`` through the
*public* converter API (not the private codec) across a broad matrix of object
shapes a user might reasonably hand to an orchestrator/activity/entity boundary:
plain dataclasses, deeply nested dataclasses, dataclasses with ``to_json`` /
``from_json`` hooks (including nested), nested non-dataclass custom classes,
enums, containers (``list`` / ``dict`` / ``tuple``), ``Optional`` / ``Union``,
recursive structures, and a set of types the SDK intentionally does **not**
auto-serialize.

The intent is to lock in the "secure, minimal-effort, intuitive object
round-tripping" contract and to document -- via xfail/raises -- exactly where a
user must supply a ``to_json`` hook or a custom ``DataConverter``.
"""

from __future__ import annotations

import enum
import typing
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

import pytest

from durabletask.serialization import JsonDataConverter


@pytest.fixture
def conv() -> JsonDataConverter:
    return JsonDataConverter()


def _round_trip(conv: JsonDataConverter, value, target_type):
    """Serialize then deserialize ``value`` back into ``target_type``."""
    return conv.deserialize(conv.serialize(value), target_type)


# ---------------------------------------------------------------------------
# Plain and nested dataclasses
# ---------------------------------------------------------------------------


@dataclass
class Address:
    street: str
    city: str


@dataclass
class Person:
    name: str
    age: int
    address: Address | None = None


def test_plain_dataclass(conv):
    value = Address("1 Main St", "Redmond")
    assert _round_trip(conv, value, Address) == value


def test_nested_dataclass(conv):
    value = Person("Ada", 30, Address("1 Main St", "Redmond"))
    result = _round_trip(conv, value, Person)
    assert result == value
    assert isinstance(result.address, Address)


def test_optional_dataclass_field_present(conv):
    value = Person("Ada", 30, Address("1 Main St", "Redmond"))
    assert _round_trip(conv, value, Person).address == Address("1 Main St", "Redmond")


def test_optional_dataclass_field_absent(conv):
    value = Person("Ada", 30, None)
    assert _round_trip(conv, value, Person).address is None


@dataclass
class _L3:
    v: int


@dataclass
class _L2:
    l3: _L3


@dataclass
class _L1:
    l2: _L2


def test_three_level_nested_dataclass(conv):
    value = _L1(_L2(_L3(7)))
    result = _round_trip(conv, value, _L1)
    assert result == value
    assert isinstance(result.l2.l3, _L3)


def test_frozen_dataclass(conv):
    @dataclass(frozen=True)
    class Frozen:
        x: int
        y: int

    value = Frozen(1, 2)
    assert _round_trip(conv, value, Frozen) == value


def test_empty_dataclass(conv):
    @dataclass
    class Empty:
        pass

    assert isinstance(_round_trip(conv, Empty(), Empty), Empty)


def test_dataclass_with_builtin_shadowing_field_names(conv):
    @dataclass
    class Shadow:
        type: str
        id: int

    value = Shadow("widget", 1)
    assert _round_trip(conv, value, Shadow) == value


def test_dataclass_default_factory_list_of_dataclasses(conv):
    @dataclass
    class Bag:
        items: list[Address] = field(default_factory=list)

    value = Bag([Address("a", "b")])
    result = _round_trip(conv, value, Bag)
    assert result == value
    assert isinstance(result.items[0], Address)


# ---------------------------------------------------------------------------
# Dataclasses with to_json / from_json hooks (incl. nested)
# ---------------------------------------------------------------------------


@dataclass
class Temperature:
    """Dataclass whose JSON shape differs from its field layout."""

    celsius: float

    def to_json(self) -> dict:
        return {"fahrenheit": self.celsius * 9 / 5 + 32}

    @classmethod
    def from_json(cls, data: dict) -> "Temperature":
        return cls((data["fahrenheit"] - 32) * 5 / 9)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Temperature) and other.celsius == self.celsius


def test_dataclass_with_hooks(conv):
    value = Temperature(100.0)
    encoded = conv.serialize(value)
    assert encoded == '{"fahrenheit": 212.0}'  # hook shape, not raw field
    assert conv.deserialize(encoded, Temperature) == value


def test_nested_dataclass_hook_inside_dataclass(conv):
    @dataclass
    class Reading:
        temp: Temperature
        note: str

    value = Reading(Temperature(37.0), "ok")
    encoded = conv.serialize(value)
    assert '"fahrenheit"' in encoded
    result = conv.deserialize(encoded, Reading)
    assert result.temp == Temperature(37.0)
    assert result.note == "ok"


def test_list_of_hooked_dataclasses(conv):
    value = [Temperature(0.0), Temperature(100.0)]
    result = _round_trip(conv, value, list[Temperature])
    assert result == value


def test_dict_of_hooked_dataclasses(conv):
    value = {"a": Temperature(0.0)}
    result = _round_trip(conv, value, dict[str, Temperature])
    assert result["a"] == Temperature(0.0)


# ---------------------------------------------------------------------------
# Nested non-dataclass custom classes (with hooks)
# ---------------------------------------------------------------------------


class Money:
    """A field type that is not JSON-serializable on its own."""

    def __init__(self, cents: int):
        self.cents = cents

    def to_json(self) -> dict:
        return {"cents": self.cents}

    @classmethod
    def from_json(cls, data: dict) -> "Money":
        return cls(data["cents"])

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Money) and other.cents == self.cents


def test_dataclass_with_non_serializable_field_uses_hook(conv):
    @dataclass
    class Invoice:
        amount: Money

        def __eq__(self, other: object) -> bool:
            return isinstance(other, Invoice) and other.amount == self.amount

    value = Invoice(Money(500))
    result = _round_trip(conv, value, Invoice)
    assert result == value
    assert isinstance(result.amount, Money)


def test_standalone_custom_class_with_hooks(conv):
    value = Money(250)
    assert _round_trip(conv, value, Money) == value


def test_hook_returning_scalar_string(conv):
    class Tag:
        def __init__(self, name: str):
            self.name = name

        def to_json(self) -> str:
            return self.name

        @classmethod
        def from_json(cls, data: str) -> "Tag":
            return cls(data)

        def __eq__(self, other: object) -> bool:
            return isinstance(other, Tag) and other.name == self.name

    value = Tag("vip")
    assert conv.serialize(value) == '"vip"'
    assert _round_trip(conv, value, Tag) == value


def test_dict_of_custom_hooked_class(conv):
    value = {"usd": Money(100), "eur": Money(200)}
    result = _round_trip(conv, value, dict[str, Money])
    assert result == value


# ---------------------------------------------------------------------------
# Converter-aware from_json (recursive reconstruction of nested typed values)
# ---------------------------------------------------------------------------


class Ledger:
    """A type whose from_json uses the converter to rebuild a nested value."""

    def __init__(self, owner: str, balance: Money):
        self.owner = owner
        self.balance = balance

    def to_json(self) -> dict:
        return {"owner": self.owner, "balance": self.balance}

    @classmethod
    def from_json(cls, data: dict, converter) -> "Ledger":
        return cls(data["owner"], converter.coerce(data["balance"], Money))

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, Ledger)
            and other.owner == self.owner
            and other.balance == self.balance
        )


def test_converter_aware_from_json(conv):
    value = Ledger("ada", Money(999))
    result = _round_trip(conv, value, Ledger)
    assert result == value
    assert isinstance(result.balance, Money)


def test_converter_aware_from_json_nested_in_dataclass(conv):
    @dataclass
    class Account:
        ledgers: list[Ledger]

    value = Account([Ledger("ada", Money(1))])
    result = _round_trip(conv, value, Account)
    assert isinstance(result.ledgers[0], Ledger)
    assert isinstance(result.ledgers[0].balance, Money)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class Color(enum.IntEnum):
    RED = 1
    GREEN = 2


class Status(enum.Enum):
    OPEN = "open"
    CLOSED = "closed"


def test_int_enum_round_trip(conv):
    assert conv.serialize(Color.RED) == "1"
    assert conv.deserialize("1", Color) is Color.RED


def test_str_enum_round_trip(conv):
    assert conv.serialize(Status.OPEN) == '"open"'
    assert conv.deserialize('"open"', Status) is Status.OPEN


def test_enum_as_dataclass_field(conv):
    @dataclass
    class Ticket:
        status: Status
        color: Color

    value = Ticket(Status.OPEN, Color.RED)
    result = _round_trip(conv, value, Ticket)
    assert result.status is Status.OPEN
    assert result.color is Color.RED


def test_list_of_enums(conv):
    value = [Status.OPEN, Status.CLOSED]
    result = _round_trip(conv, value, list[Status])
    assert result == value


def test_optional_enum_field(conv):
    @dataclass
    class MaybeStatus:
        status: Status | None = None

    assert _round_trip(conv, MaybeStatus(Status.CLOSED), MaybeStatus).status is Status.CLOSED
    assert _round_trip(conv, MaybeStatus(None), MaybeStatus).status is None


# ---------------------------------------------------------------------------
# Containers
# ---------------------------------------------------------------------------


def test_list_of_dataclasses(conv):
    value = [Address("a", "b"), Address("c", "d")]
    result = _round_trip(conv, value, list[Address])
    assert result == value


def test_dict_of_dataclasses(conv):
    value = {"home": Address("a", "b")}
    result = _round_trip(conv, value, dict[str, Address])
    assert isinstance(result["home"], Address)


def test_dict_of_optional_dataclasses(conv):
    value = {"a": Address("x", "y"), "b": None}
    result = _round_trip(conv, value, dict[str, typing.Optional[Address]])
    assert isinstance(result["a"], Address)
    assert result["b"] is None


def test_fixed_length_tuple(conv):
    value = (Address("a", "b"), 5)
    result = _round_trip(conv, value, tuple[Address, int])
    assert isinstance(result, tuple)
    assert result[0] == Address("a", "b")
    assert result[1] == 5


def test_homogeneous_tuple(conv):
    value = (Address("a", "b"), Address("c", "d"))
    result = _round_trip(conv, value, tuple[Address, ...])
    assert isinstance(result, tuple)
    assert all(isinstance(item, Address) for item in result)


def test_tuple_field_in_dataclass(conv):
    @dataclass
    class Pair:
        pair: tuple[Address, Address]

    value = Pair((Address("a", "b"), Address("c", "d")))
    result = _round_trip(conv, value, Pair)
    assert isinstance(result.pair, tuple)
    assert result.pair[0] == Address("a", "b")


def test_dataclass_with_list_of_nested_dataclasses(conv):
    @dataclass
    class Team:
        members: list[Person]

    value = Team([Person("A", 1, Address("x", "y"))])
    result = _round_trip(conv, value, Team)
    assert isinstance(result.members[0], Person)
    assert isinstance(result.members[0].address, Address)


def test_dataclass_with_dict_of_custom_class(conv):
    @dataclass
    class Wallet:
        funds: dict[str, Money]

    value = Wallet({"usd": Money(100)})
    result = _round_trip(conv, value, Wallet)
    assert isinstance(result.funds["usd"], Money)
    assert result.funds["usd"] == Money(100)


# ---------------------------------------------------------------------------
# Recursive structures
# ---------------------------------------------------------------------------


@dataclass
class TreeNode:
    value: int
    children: list["TreeNode"] = field(default_factory=list)


def test_recursive_tree_dataclass(conv):
    value = TreeNode(1, [TreeNode(2), TreeNode(3, [TreeNode(4)])])
    result = _round_trip(conv, value, TreeNode)
    assert result == value
    assert isinstance(result.children[1].children[0], TreeNode)


# ---------------------------------------------------------------------------
# Documented limitation: nested typed reconstruction needs resolvable hints.
#
# Type-directed reconstruction relies on ``typing.get_type_hints`` to read a
# dataclass's nested field types. A dataclass defined inside a function whose
# fields reference *other function-local* types cannot be resolved (the names
# are not in the module globals), so nested coercion is skipped and the field
# is returned as parsed JSON. Module-level dataclasses (the normal case) work.
# ---------------------------------------------------------------------------


def test_function_local_nested_dataclass_hint_is_not_resolvable(conv):
    @dataclass
    class Inner:
        v: int

    @dataclass
    class Outer:
        inner: Inner

    result = _round_trip(conv, Outer(Inner(7)), Outer)
    # The outer dataclass is rebuilt, but the inner field stays a plain dict
    # because its type hint can't be resolved from a function scope.
    assert isinstance(result, Outer)
    assert result.inner == {"v": 7}


# ---------------------------------------------------------------------------
# Builtins / primitives
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("value,target", [
    (42, int),
    ("hello", str),
    (3.14, float),
    (True, bool),
    (None, type(None)),
])
def test_primitive_round_trip(conv, value, target):
    assert conv.deserialize(conv.serialize(value), target) == value


def test_list_of_primitives(conv):
    assert _round_trip(conv, [1, 2, 3], list[int]) == [1, 2, 3]


def test_dict_of_primitives(conv):
    assert _round_trip(conv, {"a": 1, "b": 2}, dict[str, int]) == {"a": 1, "b": 2}


def test_no_target_type_returns_plain_json(conv):
    # Without a target type, custom objects come back as plain JSON.
    encoded = conv.serialize(Address("a", "b"))
    assert conv.deserialize(encoded) == {"street": "a", "city": "b"}


# ---------------------------------------------------------------------------
# Multi-member Union: documented limitation (not force-coerced)
# ---------------------------------------------------------------------------


def test_multi_member_union_field_is_not_force_coerced(conv):
    @dataclass
    class A:
        a: int

    @dataclass
    class B:
        b: int

    @dataclass
    class HasUnion:
        val: typing.Union[A, B]

    # A multi-member Union cannot be disambiguated from the payload alone, so the
    # value is intentionally left as parsed JSON rather than guessing a member.
    # This documents the current, secure behavior.
    value = HasUnion(A(1))
    result = _round_trip(conv, value, HasUnion)
    assert result.val == {"a": 1}


# ---------------------------------------------------------------------------
# Types the SDK intentionally does NOT auto-serialize.
#
# These require a user-supplied ``to_json`` hook or a custom ``DataConverter``.
# The tests pin the current behavior (a clear TypeError) so a future change that
# adds support is a deliberate, reviewed decision.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("value", [
    datetime(2020, 1, 1, 12, 0, 0),
    Decimal("1.5"),
    {1, 2, 3},
    frozenset({1, 2}),
    b"bytes",
])
def test_unsupported_types_raise_clear_typeerror(conv, value):
    with pytest.raises(TypeError) as exc_info:
        conv.serialize(value)
    assert type(value).__name__ in str(exc_info.value)


def test_plain_object_without_hook_raises(conv):
    class Plain:
        def __init__(self):
            self.a = 1

    with pytest.raises(TypeError):
        conv.serialize(Plain())


def test_custom_datetime_via_to_json_hook(conv):
    # The supported way to round-trip a datetime is a to_json/from_json hook.
    class Timestamp:
        def __init__(self, dt: datetime):
            self.dt = dt

        def to_json(self) -> str:
            return self.dt.isoformat()

        @classmethod
        def from_json(cls, data: str) -> "Timestamp":
            return cls(datetime.fromisoformat(data))

        def __eq__(self, other: object) -> bool:
            return isinstance(other, Timestamp) and other.dt == self.dt

    value = Timestamp(datetime(2020, 1, 1, 12, 0, 0))
    assert _round_trip(conv, value, Timestamp) == value
