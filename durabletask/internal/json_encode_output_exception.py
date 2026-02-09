from typing import Any


class JsonEncodeOutputException(Exception):
    """Custom exception type used to indicate that an orchestration result could not be JSON-encoded."""

    def __init__(self, problem_object: Any):
        super().__init__()
        self.problem_object = problem_object

    def __str__(self) -> str:
        return f"The orchestration result could not be encoded. Object details: {self.problem_object}"
