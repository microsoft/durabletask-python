# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Unit tests for :class:`durabletask.task.FailureDetails` introspection."""

import pytest

from durabletask import task, worker
from durabletask.internal import helpers


class _BaseError(Exception):
    pass


class _DerivedError(_BaseError):
    pass


def _fd(error_type: str) -> task.FailureDetails:
    return task.FailureDetails(message="boom", error_type=error_type, stack_trace=None)


# ---------------------------------------------------------------------------
# get_qualified_name
# ---------------------------------------------------------------------------

def test_get_qualified_name_builtin_keeps_builtins_prefix():
    assert helpers.get_qualified_name(ValueError) == "builtins.ValueError"


def test_get_qualified_name_module_type():
    assert helpers.get_qualified_name(task.NonDeterminismError) == \
        "durabletask.task.NonDeterminismError"


def test_get_qualified_name_preserves_nested_qualname():
    class Outer:
        class Inner(Exception):
            pass

    assert helpers.get_qualified_name(Outer.Inner).endswith(".Outer.Inner")


# ---------------------------------------------------------------------------
# new_failure_details now emits fully-qualified names
# ---------------------------------------------------------------------------

def test_new_failure_details_uses_fqn_for_builtin():
    assert helpers.new_failure_details(ValueError("x")).errorType == "builtins.ValueError"


def test_new_failure_details_uses_fqn_for_custom_type():
    details = helpers.new_failure_details(_DerivedError("x"))
    assert details.errorType == helpers.get_qualified_name(_DerivedError)


# ---------------------------------------------------------------------------
# is_caused_by with an exception type (base-type aware)
# ---------------------------------------------------------------------------

def test_is_caused_by_exact_builtin_type():
    assert _fd("builtins.ValueError").is_caused_by(ValueError) is True


def test_is_caused_by_matches_base_type():
    fd = _fd("builtins.FileNotFoundError")  # subclass of OSError
    assert fd.is_caused_by(FileNotFoundError) is True
    assert fd.is_caused_by(OSError) is True
    assert fd.is_caused_by(ValueError) is False


def test_is_caused_by_custom_hierarchy():
    fd = _fd(helpers.get_qualified_name(_DerivedError))
    assert fd.is_caused_by(_DerivedError) is True
    assert fd.is_caused_by(_BaseError) is True
    assert fd.is_caused_by(Exception) is True
    assert fd.is_caused_by(KeyError) is False


def test_is_caused_by_does_not_match_more_derived_type():
    # A base-class failure is not "caused by" one of its subclasses.
    fd = _fd(helpers.get_qualified_name(_BaseError))
    assert fd.is_caused_by(_DerivedError) is False


def test_is_caused_by_base_type_across_modules():
    # OrchestratorNotRegisteredError (durabletask.worker) subclasses ValueError.
    fd = _fd("durabletask.worker.OrchestratorNotRegisteredError")
    assert fd.is_caused_by(worker.OrchestratorNotRegisteredError) is True
    assert fd.is_caused_by(ValueError) is True


def test_is_caused_by_fqn_disambiguates_same_simple_name():
    # A fully-qualified stored name from a different module must not match a
    # local type that merely shares the simple name.
    fd = _fd("some.other.module.ValueError")
    assert fd.is_caused_by(ValueError) is False


# ---------------------------------------------------------------------------
# is_caused_by with a name string
# ---------------------------------------------------------------------------

def test_is_caused_by_string_exact_fqn():
    assert _fd("builtins.ValueError").is_caused_by("builtins.ValueError") is True


def test_is_caused_by_string_unqualified_name():
    assert _fd("builtins.ValueError").is_caused_by("ValueError") is True


def test_is_caused_by_string_no_match():
    assert _fd("builtins.ValueError").is_caused_by("KeyError") is False


def test_is_caused_by_string_two_fqns_from_different_modules_do_not_match():
    assert _fd("pkg_a.Foo").is_caused_by("pkg_b.Foo") is False


# ---------------------------------------------------------------------------
# Back-compat: a bare (non-qualified) stored error_type
# ---------------------------------------------------------------------------

def test_is_caused_by_type_with_legacy_bare_stored_name():
    assert _fd("ValueError").is_caused_by(ValueError) is True
    assert _fd("FileNotFoundError").is_caused_by(OSError) is True


def test_is_caused_by_string_qualified_arg_matches_bare_stored_name():
    assert _fd("ValueError").is_caused_by("builtins.ValueError") is True


# ---------------------------------------------------------------------------
# Edge cases / errors
# ---------------------------------------------------------------------------

def test_is_caused_by_empty_error_type_is_false():
    assert _fd("").is_caused_by(ValueError) is False
    assert _fd("").is_caused_by("ValueError") is False


def test_is_caused_by_non_exception_type_raises_type_error():
    with pytest.raises(TypeError):
        _fd("builtins.ValueError").is_caused_by(int)  # type: ignore[arg-type]


def test_is_caused_by_non_type_argument_raises_type_error():
    with pytest.raises(TypeError):
        _fd("builtins.ValueError").is_caused_by(123)  # type: ignore[arg-type]


def test_is_caused_by_exception_instance_raises_type_error():
    with pytest.raises(TypeError):
        _fd("builtins.ValueError").is_caused_by(ValueError("x"))  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# End-to-end through TaskFailedError.details
# ---------------------------------------------------------------------------

def test_task_failed_error_details_is_caused_by():
    err = task.TaskFailedError("failed", ValueError("boom"))
    assert err.details.error_type == "builtins.ValueError"
    assert err.details.is_caused_by(ValueError) is True
    assert err.details.is_caused_by(Exception) is True
    assert err.details.is_caused_by(KeyError) is False
