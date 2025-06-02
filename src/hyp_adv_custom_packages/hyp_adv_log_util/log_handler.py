# ruff: noqa

"""
Logging module for lambdas . The response_and_log_handler acts the actual logging function from where other functions
are called.
"""
import inspect
import json
import os
import sys
import traceback
from collections import deque
from concurrent.futures.thread import ThreadPoolExecutor

# importing different class types for json serialize handler
from datetime import datetime

from psycopg.rows import dict_row

from .. import db_connection_manager
from .__create_response import create_response_json


class EventStack:
    """
    Class to initialize and assign a deque() object
    """
    def __init__(self):
        self.stack = deque()


# declaring global variable with type hint of EventStack class.
EVENT_STACK: EventStack


def initialize_stack():
    """
    Function to initialize EventStack object to declared global variable
    :return:
    """
    global EVENT_STACK
    EVENT_STACK = EventStack()


def json_datatypes_handler(val):
    """
    To handle non-serializable data types when performing json.dumps()

    :param val:
    :return:
    """
    if isinstance(val, datetime):
        return val.strftime('%Y-%m-%d %H:%M:%S')
    elif isinstance(val, ThreadPoolExecutor):
        return "<Python ThreadPoolExecutor Object>"
    elif isinstance(val, object):
        try:
            # if val is a DataFrame
            return f"{type(val)} object. Shape[r, c]: {val.shape} Columns: {val.columns.tolist()}"
        except Exception:
            pass

        try:
            # if val is a request's response object
            return f"{type(val)} object. status_code: {val.status_code}, response: {val.json()}"
        except Exception:
            pass

        return f"class object of type {type(val)}"

    raise TypeError(f"Unknown data type. Cannot JSON serialize to string (json.dumps) --> {type(val)}")


def _insert_log_entry(log_entry):
    """
    Insert log entry into the database

    Table-> monitoring_logs

    :param log_entry:
    :return:
    """
    rds_conn = db_connection_manager.get_db_connection()
    cur = rds_conn.cursor(row_factory=dict_row)

    columns = ','.join(log_entry.keys())  # columns to insert into
    values = log_entry.values()  # values to insert
    query = f"""
            INSERT INTO logging.monitoring_logs(
                {columns}
            ) 
            VALUES(
                {','.join(['%s' for _ in values])}
            )
            RETURNING log_id;
            """

    cur.execute(query, tuple(values))
    log_id = cur.fetchone()['log_id']

    # checking if a log id was returned, to validate successful insert
    if log_id and type(log_id) is int:
        rds_conn.commit()

        db_connection_manager.put_db_connection(rds_conn)

    else:
        rds_conn.rollback()

        db_connection_manager.put_db_connection(rds_conn)

        raise RuntimeError(f"Log insert failed for the following entry: {log_entry}")


def _prepare_log_entry(log_entry):
    """
    To prepare log_entry for insert into DB.

    Calls insert_log_entry to also insert the prepared log_entry into the database.

    :return:
    """

    # appending timestamps to log entry
    # timestamp = commons.curr_timestamp()
    # log_entry['created_at'] = timestamp
    # log_entry['updated_at'] = timestamp

    # To insert lambda context with log entry
    # try:
    #     lambda_context = json.loads(os.environ['MY_LAMBDA_CONTEXT'])
    #     if lambda_context:
    #         # if context is present, run is mostly from aws lambda, so appending flag.
    #         lambda_context["is_local_run"] = False
    #     else:
    #         # creating a basic context, with flag that this run was not from aws lambda.
    #         lambda_context = {
    #             "function_name": os.getcwd().split('/')[-1],
    #             "is_local_run": True
    #         }
    #     # add the context to log_entry
    #     log_entry['lambda_context'] = json.dumps(lambda_context, default=json_datatypes_handler)
    # except Exception as ex:
    #     raise RuntimeError("lambda context not found/was not set.") from ex

    # call insert to DB
    _insert_log_entry(log_entry)


def _info_level_actions(log_level, log_detail, log_source, caller_event, current_event):
    """
    To perform certain actions when an INFO level log is made.

    :return:
    """

    # create dict, with key_names SAME as columns in DB log table. Insert log entry into DB
    # insertable_log = {
    #     'log_level': log_level,
    #     'log_detail': log_detail,
    #     'log_source': log_source
    # }
    # _prepare_log_entry(log_entry=insertable_log)


def _debug_level_actions():
    """

    :return:
    """


def _warn_level_actions(log_level, log_detail, log_source, caller_event, current_event, operation_body: dict | list):
    """
    To perform certain actions when a WARNING level log is made.

    :return:
    """

    # create dict, with key_names SAME as columns in DB log table. Insert log entry into DB
    insertable_log = {
        'log_level': log_level,
        'log_detail': log_detail,
        'log_source': log_source,
        "log_current_event": json.dumps(current_event, default=json_datatypes_handler),
        "operation_body_json": json.dumps(operation_body, default=json_datatypes_handler)
    }
    # check if a caller/parent exists, and append
    if caller_event:
        insertable_log['log_caller_event'] = json.dumps(caller_event, default=json_datatypes_handler)

    _prepare_log_entry(log_entry=insertable_log)


def _error_level_actions(log_level, log_detail, log_source, local_vars, caller_event, current_event,
                         operation_body: dict | list):
    """
    To perform certain actions when an ERROR level log is made.

    :return:
    """

    # create dict, with key_names SAME as columns in DB log table. Insert log entry into DB
    insertable_log = {
        'log_level': log_level,
        'log_detail': log_detail,
        'log_source': log_source,
        'function_variables': local_vars,
        "log_current_event": json.dumps(current_event, default=json_datatypes_handler),
        "operation_body_json": json.dumps(operation_body, default=json_datatypes_handler)
    }
    # check if a caller/parent exists, and append
    if caller_event:
        insertable_log['log_caller_event'] = json.dumps(caller_event, default=json_datatypes_handler)

    _prepare_log_entry(log_entry=insertable_log)


def _critical_level_actions(log_level, log_detail, log_source, traceback_string, caller_event, current_event,
                            local_vars, operation_body: dict | list):
    """
    To perform certain actions when a CRITICAL level log is made.

    :return:
    """

    # create dict, with key_names SAME as columns in DB log table. Insert log entry into DB
    insertable_log = {
        'log_level': log_level,
        'log_detail': log_detail,
        'log_source': log_source,
        'function_traceback': traceback_string,
        'function_variables': local_vars,
        "log_current_event": json.dumps(current_event, default=json_datatypes_handler),
        "operation_body_json": json.dumps(operation_body, default=json_datatypes_handler)
    }
    # check if a caller/parent exists, and append
    if caller_event:
        insertable_log['log_caller_event'] = json.dumps(caller_event, default=json_datatypes_handler)

    _prepare_log_entry(log_entry=insertable_log)


def _find_caller():
    """
    Find caller function from stack and return it's name, module_name and the line from where the response_and_log_handler
    is called.

    :return:
    """
    # A new stack entry is created for every function call. So to access the original caller function's stack, we access the
    # third stack entry.

    stack = inspect.stack()[2]
    try:
        module_name = inspect.getmodule(stack[0])
        func_name = stack.function
        module_name = inspect.getmodulename(stack.filename)
        lineno = stack.lineno

    except AttributeError:
        pass
    finally:
        # deleting to prevent any creation of reference cycle. It also prevents the manipulation of stack from outside
        # of this function.
        del stack
        return func_name, module_name, lineno  # noqa


def base_config(event):
    """
    Push event to stack.
    :param event:
    :return:
    """
    EVENT_STACK.stack.append(event)


def response_and_log_handler(log_level, operation_body: dict | list, operation_message: str, usr_message: str = None):
    """
    Main handler module for logging. It prints the log on console and calls _create_response_json function which is
    responsible for creating final returning response json.

    The _find_caller function is called to find the actual function calling the logging module. Then it calls the _create_response_json function
    which is responsible for returning the final response_json.
    :param log_level: the level of log to be set
    :param operation_body: a dictionary containing details/data of the operation performed.
    :param operation_message: a message string to accompany the operation
    :param usr_message: an optional user message to include in the response
    :return:
    """

    LOG_LEVEL_MAP = {
        "INFO": "hypAdv-INFO",
        "DEBUG": "hypAdv-DEBUG",
        "WARNING": "hypAdv-WARNING",
        "ERROR": "hypAdv-ERROR",
        "CRITICAL": "hypAdv-CRITICAL"
    }
    func_name, module_name, lineno = _find_caller()

    # time = datetime.now(tz=timezone(timedelta(hours=5.5)))  # for timestamp

    # local stack implemented using simple list. Helps in keeping track of top-most 2 entries of event_stack.stack.
    # Note: top_of_stack -> current event and second_top_of_stack -> caller event
    _local_stack = deque()

    # if there is only one element in stack. This might be possible when the program reaches its EOL of execution and there
    # are no new functions to be called. In that case we would only have our current element/event in stack.
    if len(EVENT_STACK.stack) == 1:
        _local_stack.append(EVENT_STACK.stack[-1])
        # current_event = {
        #     "profile_id": _local_stack[0]['profile_id'],
        #     "company_id": _local_stack[0]['company_id'],
        #     "event_type": _local_stack[0]['event_type']
        # }
        current_event = _local_stack[0]
        caller_event = {}
    elif len(EVENT_STACK.stack) == 0:
        _local_stack = deque
    else:
        _local_stack.append(EVENT_STACK.stack[-1])
        _local_stack.append(EVENT_STACK.stack[-2])
        # caller_event = {
        #     "profile_id": _local_stack[1]['profile_id'],
        #     "company_id": _local_stack[1]['company_id'],
        #     "event_type": _local_stack[1]['event_type']
        # }
        # current_event = {
        #     "profile_id": _local_stack[0]['profile_id'],
        #     "company_id": _local_stack[0]['company_id'],
        #     "event_type": _local_stack[0]['event_type']
        # }
        caller_event = _local_stack[1]
        current_event = _local_stack[0]

    # when warning, our control will go back to function where it continues to run,
    # to generate another log for the same current_event
    if log_level != "WARNING":
        # Pop the current event from stack, next event present would be the caller event
        EVENT_STACK.stack.pop()

    if log_level == 'INFO' or log_level == 'WARNING':
        log_source = f"'{func_name}()' in module '{module_name}': line {lineno} "
        log_string = f"{LOG_LEVEL_MAP[log_level]} | {log_source} | {operation_message} | caller_event: {caller_event} || current_event: {current_event}"
        print(json.dumps(log_string, default=json_datatypes_handler))
        response_json = create_response_json(return_dict=operation_body, status=True, message=operation_message,
                                             log_type=log_level, usr_message=usr_message)

        # # perform actions, if any
        if log_level == 'WARNING':
            _warn_level_actions(log_level=log_level, log_detail=operation_message, log_source=log_source,
                                caller_event=caller_event, current_event=current_event, operation_body=operation_body)

        return response_json

    elif log_level == 'CRITICAL':
        log_source = f"'{func_name}()' in module '{module_name}': line {lineno}"
        log_string = f"{LOG_LEVEL_MAP[log_level]} | {log_source} | {operation_message} | caller_event: {caller_event} || current_event: {current_event}"

        # returns current handled exception
        exc = sys.exc_info()[0]
        trace_stack = traceback.extract_stack()

        if exc is not None:
            # will contain the caught exception from current caller so delete
            del trace_stack[-1]

        trc = 'Traceback: \n'
        stack_trace_string = trc + ''.join(traceback.format_list(trace_stack))
        if exc is not None:
            stack_trace_string += '  ' + traceback.format_exc(chain=True).lstrip(trc)

        variables_stack = inspect.stack()[1]
        # get local variables in scope of the function from where the logging function was called.
        local_vars = variables_stack.frame.f_locals

        message = f"{log_string}\n{stack_trace_string}\n\nLocal variables:\n{local_vars}"
        print(json.dumps(message, default=json_datatypes_handler))

        response_json = create_response_json(return_dict=operation_body, status=False, message=operation_message,
                                             log_type=log_level, extras=message, usr_message=usr_message)

        # to perform actions, if any
        _critical_level_actions(log_level=log_level,
                                log_detail=operation_message,
                                log_source=log_source,
                                traceback_string=stack_trace_string,
                                local_vars=json.dumps(local_vars, default=json_datatypes_handler),
                                caller_event=caller_event, current_event=current_event,
                                operation_body=operation_body)

        return response_json

    elif log_level == 'ERROR':
        log_source = f"'{func_name}()' in module '{module_name}': line {lineno}"
        log_string = f"{LOG_LEVEL_MAP[log_level]} | {log_source} | {operation_message} | caller_event: {caller_event} || current_event: {current_event}"

        variables_stack = inspect.stack()[1]
        # get local variables in scope of the function from where the logging function was called.
        local_vars = variables_stack.frame.f_locals
        size = sys.getsizeof(local_vars)
        if size > 512:
            local_vars = {"max_size_limit_hit": True}
        message = f"{log_string}\nLocal variables:\n{json.dumps(local_vars, default=json_datatypes_handler)}"
        # stack = inspect.getouterframes(inspect.currentframe(), 0)[1]
        # frame = stack.frame.f_locals
        print(json.dumps(message, default=json_datatypes_handler))
        response_json = create_response_json(return_dict=operation_body, status=False, message=operation_message,
                                             log_type=log_level, usr_message=usr_message)

        # to perform actions, if any
        _error_level_actions(log_level=log_level,
                             log_detail=operation_message,
                             log_source=log_source,
                             local_vars=json.dumps(local_vars, default=json_datatypes_handler),
                             caller_event=caller_event, current_event=current_event,
                             operation_body=operation_body)

        return response_json

    else:
        raise RuntimeError(f"Unknown log level: {log_level}")
