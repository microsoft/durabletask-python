# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Shared fixtures and helpers for the history-export test package.

The most useful helper here is :func:`wait_until`, which lets tests
poll for an asynchronous state change (e.g. an entity processing a
signal) instead of sleeping for a fixed duration.  Polling is both
faster on the happy path and much less flaky than a single fixed
sleep, and it lets every test fail fast when the expected condition
never materializes.
"""

from __future__ import annotations

import time
from typing import Any, Callable, Optional, TypeVar

T = TypeVar("T")


def wait_until(
    predicate: Callable[[], Optional[T]],
    *,
    timeout: float = 10.0,
    interval: float = 0.05,
    description: str = "condition",
) -> T:
    """Poll *predicate* until it returns a truthy value or *timeout* elapses.

    The predicate is called immediately and then every *interval*
    seconds.  Returns the first truthy value the predicate produced.
    Raises :class:`AssertionError` if *timeout* elapses with no truthy
    return — the message includes *description* and the last value.
    """
    deadline = time.monotonic() + timeout
    last: Any = None
    while True:
        try:
            value = predicate()
        except Exception as ex:  # noqa: BLE001
            value = None
            last = f"<predicate raised {type(ex).__name__}: {ex}>"
        if value:
            return value  # type: ignore[return-value]
        last = value if last is None else last
        if time.monotonic() >= deadline:
            raise AssertionError(
                f"Timed out after {timeout}s waiting for {description}; "
                f"last value was {last!r}"
            )
        time.sleep(interval)
