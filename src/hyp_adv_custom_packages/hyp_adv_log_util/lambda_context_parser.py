# ruff: noqa

"""
This module contains function to parse the lambdaContext class object received by function when run on AWS lambda

"""


def parse_LambdaContext(context_object):
    """

    :param context_object:
    :return:
    """

    # did not include 'identity' & 'client_context' properties of class
    parsed_context = {
        "function_name": context_object.function_name,
        "function_version": context_object.function_version,
        "aws_request_id": context_object.aws_request_id,
        "invoked_function_arn": context_object.invoked_function_arn,
        'memory_limit_in_mb': context_object.memory_limit_in_mb,
        'log_group_name': context_object.log_group_name,
        'log_stream_name': context_object.log_stream_name
    }

    return parsed_context
