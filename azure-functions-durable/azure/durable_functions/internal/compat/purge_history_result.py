# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from typing import Any

from durabletask.client import PurgeInstancesResult


class PurgeHistoryResult:
    """Information provided when a request to purge history has been made.

    Backwards-compatible wrapper around the durabletask
    :class:`~durabletask.client.PurgeInstancesResult`. New code should use
    ``PurgeInstancesResult`` directly (note the attribute is
    ``deleted_instance_count`` there).
    """

    def __init__(self, instances_deleted: int):
        self._instances_deleted = instances_deleted

    @classmethod
    def from_purge_result(cls, result: PurgeInstancesResult) -> "PurgeHistoryResult":
        """Wrap a durabletask ``PurgeInstancesResult``."""
        return cls(result.deleted_instance_count)

    @classmethod
    def from_json(cls, json_obj: "dict[str, Any]") -> "PurgeHistoryResult":
        """Reconstruct a result from its v1 JSON representation."""
        return cls(instances_deleted=json_obj["instancesDeleted"])

    @property
    def instances_deleted(self) -> int:
        """Get the number of deleted instances."""
        return self._instances_deleted
