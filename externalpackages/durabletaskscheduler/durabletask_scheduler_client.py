from durabletask import TaskHubGrpcClient

class DurableTaskSchedulerClient(TaskHubGrpcClient):
    def __init__(self, *args, **kwargs):
        # Initialize the base class
        super().__init__(*args, **kwargs)