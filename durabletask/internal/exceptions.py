import orchestrator_service_pb2 as pb


class VersionFailureException(Exception):
    def __init__(self, error_details: pb.TaskFailureDetails) -> None:
        super().__init__()
        self.error_details = error_details


class AbandonOrchestrationError(Exception):
    pass
