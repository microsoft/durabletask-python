# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from typing import Callable, TypeVar

TInput = TypeVar('TInput')
TOutput = TypeVar('TOutput')


class ActivityContext:
    def __init__(self, orchestration_id: str, task_id: int):
        self._orchestration_id = orchestration_id
        self._task_id = task_id

    @property
    def orchestration_id(self) -> str:
        """Get the ID of the orchestration instance that scheduled this activity.

        Returns
        -------
        str
            The ID of the current orchestration instance.
        """
        return self._orchestration_id

    @property
    def task_id(self) -> int:
        """Get the task ID associated with this activity invocation.

        The task ID is an auto-incrementing integer that is unique within
        the scope of the orchestration instance. It can be used to distinguish
        between multiple activity invocations that are part of the same
        orchestration instance.

        Returns
        -------
        str
            The ID of the current orchestration instance.
        """
        return self._task_id


Activity = Callable[[ActivityContext, TInput], TOutput]
