import json


class HttpManagementPayload:
    def __init__(self, instance_id: str, instance_status_url: str, required_query_string_parameters: str):
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
