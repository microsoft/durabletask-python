from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from durabletask.task import OrchestrationContext


class EntityLock:
    # Note: This should
    def __init__(self, context: 'OrchestrationContext'):
        self._context = context

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._context._exit_critical_section()
