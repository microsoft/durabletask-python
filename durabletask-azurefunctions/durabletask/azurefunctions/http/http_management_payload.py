import json


class HttpManagementPayload:
    """A class representing the HTTP management payload for a Durable Function orchestration instance.

    Contains URLs for managing the instance, such as querying status,
    sending events, terminating, restarting, etc.
    """

    def __init__(self, instance_id: str, instance_status_url: str, required_query_string_parameters: str):
        """Initializes the HttpManagementPayload with the necessary URLs.

        Args:
            instance_id (str): The ID of the Durable Function instance.
            instance_status_url (str): The base URL for the instance status.
            required_query_string_parameters (str): The required URL parameters provided by the Durable extension.
        """
        self.urls = {
            'id': instance_id,
            'purgeHistoryDeleteUri': instance_status_url + "?" + required_query_string_parameters,
            'restartPostUri': instance_status_url + "/restart?" + required_query_string_parameters,
            'sendEventPostUri': instance_status_url + "/raiseEvent/{eventName}?" + required_query_string_parameters,
            'statusQueryGetUri': instance_status_url + "?" + required_query_string_parameters,
            'terminatePostUri': instance_status_url + "/terminate?reason={text}&" + required_query_string_parameters,
            'resumePostUri': instance_status_url + "/resume?reason={text}&" + required_query_string_parameters,
            'suspendPostUri': instance_status_url + "/suspend?reason={text}&" + required_query_string_parameters
        }

    def __str__(self):
        return json.dumps(self.urls)
