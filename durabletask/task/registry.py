from types import GeneratorType
from typing import Callable, Dict

from durabletask.task.orchestrator import Orchestrator


class Registry:

    orchestrators: Dict[str, Orchestrator]

    def __init__(self):
        self.orchestrators = dict[str, Orchestrator]()

    def add_orchestrator(self, fn: Orchestrator) -> str:
        if fn is None:
            raise ValueError('An orchestrator function argument is required.')

        name = get_name(fn)
        self.add_named_orchestrator(name, fn)
        return name

    def add_named_orchestrator(self, name: str, fn: Orchestrator) -> None:
        if not name:
            raise ValueError('A non-empty orchestrator name is required.')
        if name in self.orchestrators:
            raise ValueError(f"A '{name}' orchestrator already exists.")

        self.orchestrators[name] = fn

    def get_orchestrator(self, name: str) -> Orchestrator | None:
        return self.orchestrators.get(name)


def get_name(fn: Callable) -> str:
    """Returns the name of the provided function"""
    name = fn.__name__
    if name == '<lambda>':
        raise ValueError('Cannot infer a name from a lambda function. Please provide a name explicitly.')

    return name
