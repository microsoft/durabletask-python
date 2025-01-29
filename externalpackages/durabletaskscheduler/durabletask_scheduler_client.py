from typing import Optional
from durabletask.client import TaskHubGrpcClient
from externalpackages.durabletaskscheduler.access_token_manager import AccessTokenManager

class DurableTaskSchedulerClient(TaskHubGrpcClient):
    def __init__(self, *args, 
                 metadata: Optional[list[tuple[str, str]]] = None,
                 client_id: Optional[str] = None,
                 taskhub: str,
                 **kwargs):
        if metadata is None:
            metadata = []  # Ensure metadata is initialized
        self._metadata = metadata
        self._client_id = client_id
        self._metadata.append(("taskhub", taskhub))
        self._access_token_manager = AccessTokenManager(client_id=self._client_id)
        self.__update_metadata_with_token()
        super().__init__(*args, metadata=self._metadata, **kwargs)

    def __update_metadata_with_token(self):
        """
        Add or update the `authorization` key in the metadata with the current access token.
        """
        token = self._access_token_manager.get_access_token()

        # Ensure that self._metadata is initialized
        if self._metadata is None:
            self._metadata = []  # Initialize it if it's still None
        
        # Check if "authorization" already exists in the metadata
        updated = False
        for i, (key, _) in enumerate(self._metadata):
            if key == "authorization":
                self._metadata[i] = ("authorization", token)
                updated = True
                break
        
        # If not updated, add a new entry
        if not updated:
            self._metadata.append(("authorization", token))

    def schedule_new_orchestration(self, *args, **kwargs) -> str:
        self.__update_metadata_with_token()
        return super().schedule_new_orchestration(*args, **kwargs)

    def get_orchestration_state(self, *args, **kwargs):
        self.__update_metadata_with_token()
        super().get_orchestration_state(*args, **kwargs)

    def wait_for_orchestration_start(self, *args, **kwargs):
        self.__update_metadata_with_token()
        super().wait_for_orchestration_start(*args, **kwargs)

    def wait_for_orchestration_completion(self, *args, **kwargs):
        self.__update_metadata_with_token()
        super().wait_for_orchestration_completion(*args, **kwargs)

    def raise_orchestration_event(self, *args, **kwargs):
        self.__update_metadata_with_token()
        super().raise_orchestration_event(*args, **kwargs)

    def terminate_orchestration(self, *args, **kwargs):
        self.__update_metadata_with_token()
        super().terminate_orchestration(*args, **kwargs)

    def suspend_orchestration(self, *args, **kwargs):
        self.__update_metadata_with_token()
        super().suspend_orchestration(*args, **kwargs)

    def resume_orchestration(self, *args, **kwargs):
        self.__update_metadata_with_token()
        super().resume_orchestration(*args, **kwargs)

    def purge_orchestration(self, *args, **kwargs):
        self.__update_metadata_with_token()
        super().purge_orchestration(*args, **kwargs)