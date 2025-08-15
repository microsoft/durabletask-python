class VersionFailureException(Exception):
    pass


class AbandonOrchestrationError(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)
