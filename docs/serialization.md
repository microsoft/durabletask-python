# Serialization and replay

The core SDK and Azure Managed provider use `JsonDataConverter` by default for
durable payloads. This guide describes that converter's current behavior and the
compatibility policy for changes to default serialization. The support matrix
reflects the current repository code; check the [changelog](../CHANGELOG.md)
for changes not yet present in an installed release.

Azure Functions Durable uses the shared Azure Functions codec instead. Its
serialization behavior is not described by the type matrix below; see the
[provider documentation](../azure-functions-durable/README.md#unicode-json-serialization).
The compatibility policy also guides changes in this repository to that
provider's default serialization.

## Compatibility policy

Orchestrations can replay payloads written by an earlier SDK version. Changing
the serialized representation, reconstructed value or type, raw fallback, or
failure behavior can change orchestration decisions during replay.

The project follows these rules for changes to default serialization:

- New types and previously unsupported serialization or reconstruction cases
  are considered for the next breaking major version. They are not added in a
  non-major release, including behind a new feature-specific opt-in flag.
- Between major releases, existing payload representations, reconstructed
  values and types, fallbacks, and failures are preserved except for security
  fixes and repairs to previously documented supported behavior. Changes that
  can affect replay include a release warning and upgrade guidance.
- Existing options, including the default-off generic set reconstruction
  option, retain their current defaults between major versions, subject to the
  same exceptions.

A type is not supported merely because it is related to a supported type or
because the converter returns a raw value for it. That raw fallback can still
affect replay. A previous failure can also affect replay if application code
catches it; a change from failure to success is not automatically replay-safe.

> [!WARNING]
> Changing annotations, hooks, converters, or their configuration can also
> change replay behavior. Test representative histories before deployment. If
> old behavior cannot be preserved, drain affected instances on the old
> configuration or isolate new instances in a separate task hub.

## JSON representation

The default converter serializes JSON-compatible values, dataclasses,
`SimpleNamespace` objects, and enums with JSON-compatible values. Tuples become
JSON arrays. Sets and frozensets also become arrays, ordered by canonical JSON
representation when their elements can be deterministically encoded. Custom
objects can provide a `to_json()` hook, as described below. A top-level `None`
represents no payload.

The converter does not add or infer a Python destination type from the JSON
payload. Without a target type, decoding normally returns JSON values such as
dictionaries and lists. For compatibility, the decoder still recognizes legacy
object markers.

## Reconstruction support

`deserialize()` and `coerce()` accept an explicit target type, and dataclass
fields are reconstructed using their annotations. For orchestrator, activity,
and entity inputs, and for inferred activity results when a callable is
provided, the SDK first asks `can_reconstruct()` whether to use the annotation.
Explicit target types do not depend on that discovery check.

In the table, `Sequence`, `Mapping`, and `Set` refer to `collections.abc`
types; equivalent `typing` aliases with the same origin are also recognized.

| Target annotation | Reconstruction behavior |
| --- | --- |
| Dataclass or `from_json()` class | Rebuilt when supplied; automatically discoverable. |
| `list[T]`, `Sequence[T]` | Elements are coerced; discovery requires reconstructable `T`. |
| `dict[str, T]`, `Mapping[str, T]` | Values are coerced; discovery requires reconstructable `T`. |
| `tuple[T, ...]`, `tuple[T1, T2]` | Coerces elements; discovery needs a reconstructable element. |
| `Optional[T]` | Non-`None` values use `T`; discovery follows `T`. |
| `set[T]`, `frozenset[T]`, `Set[T]` | Generic reconstruction is opt-in; see below. |
| `MutableMapping[str, T]`, `MutableSequence[T]` | Not discovered or traversed; values stay raw. |

JSON object keys remain strings; mapping value annotations do not coerce keys.
The converter recognizes the listed container origins, not arbitrary related
ABCs or subclasses. Other generic annotations are generally passed through as
parsed JSON rather than recursively reconstructed. `T | None` is equivalent to
`Optional[T]`.

Primitive-only annotations do not normally trigger automatic discovery. For
example, `tuple[int, ...]` is not inferred as an activity input type, so the
input remains a list; explicitly requesting that target type reconstructs a
tuple. Enums can similarly be reconstructed when explicitly requested or used
in a typed dataclass field, but an enum annotation alone is not automatically
discovered. For unions with multiple non-`None` members, the converter does not
guess which member should reconstruct an unmatched value.

Reconstruction is best-effort: if coercion fails, `JsonDataConverter` returns
the raw value and logs the failure at debug level. A custom converter can choose
stricter behavior.

### Set reconstruction

Generic `set[T]`, `frozenset[T]`, and `Set[T]` annotations retain JSON arrays as
lists by default, preserving their recorded order and duplicates. Set
reconstruction for supported, hashable elements is enabled with
`JsonDataConverter(enable_generic_sets=True)`; `enable_set_coercion=True` is an
alias. Existing bare `set` and `frozenset` target types, including dataclass
fields, can still be coerced through their constructors without this option.

> [!WARNING]
> Enabling generic set reconstruction for existing instances can change replay
> behavior. Set iteration order is not replay-safe; order values by a
> deterministic key before using their order to schedule durable work.

## Custom types and converters

For an application-owned type, define `to_json()` and `from_json()` hooks. The
encoder calls `to_json()` for custom values not already handled by Python's
JSON encoder; the hook returns a JSON-compatible value, not a JSON document.
Define `from_json()` as a class method or static method. The decoder calls it
with the parsed value when the type is supplied or discovered:

```python
from durabletask.serialization import JsonDataConverter


class Money:
    def __init__(self, cents: int) -> None:
        self.cents = cents

    def to_json(self) -> dict[str, int]:
        return {"cents": self.cents}

    @classmethod
    def from_json(cls, value: dict[str, int]) -> "Money":
        return cls(value["cents"])


converter = JsonDataConverter()
restored = converter.deserialize(converter.serialize(Money(125)), Money)
assert restored.cents == 125
```

The `from_json()` hook is responsible for its own nested values. It can accept
the active converter as a second parameter named `converter` and use its public
`coerce()` method to reconstruct them.

For other cases, implement a `DataConverter` or subclass `JsonDataConverter` and
delegate unhandled values to its public methods. Implement matching
`deserialize()` and `coerce()` behavior, and override `can_reconstruct()` when
the SDK should discover your additional annotated types. A custom converter
must traverse any additional container types itself: changing discovery alone
does not add reconstruction. The
[custom converter example](../examples/custom_data_converter/README.md) shows
delegation and explains its top-level generic limitation.

For example, where appropriate, `dict[str, Order]` and `list[Order]` annotations
can be used instead of `MutableMapping[str, Order]` and
`MutableSequence[Order]`. Changing existing annotations can affect replay, so
apply the upgrade precautions above.

Keep custom serialization deterministic and configure compatible converters on
both clients and workers that exchange durable payloads.
