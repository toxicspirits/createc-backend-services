# ruff: noqa


def create_response_json(return_dict: dict, status: bool, message: str, log_type, extras=None, usr_message=None):
    """
    A function to create the final structured json for response

    :param extras: additional data to add in response
    :param log_type: type of log which was created
    :param return_dict: the prepared result dictionary
    :param status: status for operation (True for 200, False for 409). used to check if the overall lambda/operation is successful or failed
    :param message: A message to be passed on wrt the operation
    :param usr_message: An optional user message to include in the response

    :return: final dictionary to return
    """

    status_code = 200 if status is True else 409  # checking status and setting the status code

    return_event = {
        "operation_detail_json": return_dict,
        "operation_status_code": status_code,
        "operation_message": message
    }

    if usr_message is not None:
        return_event["usr_message"] = usr_message

    if log_type == 'CRITICAL':
        # adding the tracback if log is CRITICAL
        return_event['traceback'] = extras
    # elif log_type == 'ERROR':
    #     # adding the local objects/variables if log is an ERROR
    #     return_event['local_variables'] = extras

    return return_event
