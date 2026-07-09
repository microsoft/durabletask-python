# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from __future__ import annotations

from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from durabletask.task import OrchestrationContext


class EntityLock:
    # Note: This should
    def __init__(self, context: OrchestrationContext):
        self._context = context

    def __enter__(self) -> EntityLock:
        return self

    def __exit__(self, *args: object) -> None:
        self._context._exit_critical_section()  # pyright: ignore[reportPrivateUsage]
