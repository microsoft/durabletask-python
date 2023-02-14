import grpc
import orchestrator_service_pb2
import orchestrator_service_pb2_grpc

class TaskHubGrpcClient:
    
    async def schedule_new_orchestration(self):
        pass

    async def wait_for_orchestration_start(self):
        pass

    async def wait_for_orchestration_completion(self):
        pass

    async def terminate_orchestration(self):
        pass

    async def suspend_orchestration(self):
        pass

    async def resume_orchestration(self):
        pass

    async def raise_orchestration_event(self):
        pass
