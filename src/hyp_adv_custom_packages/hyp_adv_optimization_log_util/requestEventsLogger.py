# ruff: noqa

"""
This module is used in the requesting-individual-data stage of the pipeline.
It logs the events of individual data which are generated, then later sent to lambdas for data fetch

"""

import json

from psycopg.rows import dict_row

from .. import db_connection_manager


def insert_prepared_events(lambda_events):
    """
    Create an entry in table for received new events and returns same list with event_id appended, where event_id is returned from DB (INSERT INTO ..... RETURNING)

    :param lambda_events:  list of prepared events
    :return: lambda_events(list)
    """
    rds_conn = db_connection_manager.get_db_connection()
    cur = rds_conn.cursor(row_factory=dict_row)
    query = """
    INSERT INTO logging_optimization_pipeline.request_events_details(request_id, event, request_type) 
    VALUES(%s, %s, %s)
    RETURNING event_id;
    """
    for event in lambda_events:
        cur.execute(query, (
            event['request_id'],
            json.dumps(event),
            event['request_type']
        ))
        event_id = cur.fetchone()['event_id']
        event['event_id'] = event_id  # Add request id to the event

    rds_conn.commit()
    db_connection_manager.put_db_connection(rds_conn)
    return lambda_events


def update_fetch_status(request_id, status, **kwargs):
    """
    To Update fetch status and details of event data_fetch execution. UPDATE query using id received in insert_prepared_events()

    :param request_id: request_id of request which generated this event
    :param status: status of data_fetch execution attempt
    :param kwargs: to pass 'log' with any additional information about the execution
    :return: None
    """
    set_params = {
        "fetch_status": status,
        "fetch_log": None,
    }

    if 'log' in kwargs:
        set_params['fetch_log'] = kwargs['log']

    query = f"""
            UPDATE logging_optimization_pipeline.request_details SET fetch_status=(%s), fetch_log=(%s)
            WHERE request_id = {request_id};
            """

    rds_conn = db_connection_manager.get_db_connection()
    cur = rds_conn.cursor(row_factory=dict_row)

    cur.execute(query, tuple(set_params.values()))
    rds_conn.commit()
    db_connection_manager.put_db_connection(rds_conn)


def update_process_status(request_id, status, **kwargs):
    """
    To Update processing status and details of event processing execution after data fetch. UPDATE query using id received in insert_prepared_events()

    :param request_id: request_id of request which generated this event
    :param status: status of processing execution attempt
    :param kwargs: to pass 'log' with any additional information about the execution
    :return: None
    """
    set_params = {
        'process_status': status,
        'process_log': None,
    }

    if status == 'FAILED' or status == 'SUCCESS':
        set_params['process_log'] = kwargs['log']

    query = f"""
            UPDATE logging_optimization_pipeline.request_details SET process_status=(%s), process_log=(%s)
            WHERE request_id = {request_id};
            """

    rds_conn = db_connection_manager.get_db_connection()
    cur = rds_conn.cursor(row_factory=dict_row)

    cur.execute(query, tuple(set_params.values()))
    rds_conn.commit()
    db_connection_manager.put_db_connection(rds_conn)


def update_event_status(request_id, event_id, status, **kwargs):

    set_params = {
        'event_status': status,
        'event_log': None,
    }

    if status == 'FAILED':
        set_params['event_log'] = kwargs['log']

    query = f"""
                UPDATE logging_optimization_pipeline.request_events_details SET event_status=(%s), event_log=(%s)
                WHERE request_id = {request_id} AND event_id = {event_id};
                """

    rds_conn = db_connection_manager.get_db_connection()
    cur = rds_conn.cursor(row_factory=dict_row)

    cur.execute(query, tuple(set_params.values()))
    rds_conn.commit()
    db_connection_manager.put_db_connection(rds_conn)