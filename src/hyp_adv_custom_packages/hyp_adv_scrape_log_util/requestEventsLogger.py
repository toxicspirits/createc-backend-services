# ruff: noqa

"""
This module is used to log the individual events mapped out from a request.

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
    INSERT INTO logging_scraping_pipeline.request_events_details(request_id, event) 
    VALUES(%s, %s)
    RETURNING event_id;
    """
    for event in lambda_events:
        cur.execute(query, (
            event['request_id'],
            json.dumps(event)
        ))
        event_id = cur.fetchone()['event_id']
        event['event_id'] = event_id  # Add request id to the event

    rds_conn.commit()
    db_connection_manager.put_db_connection(rds_conn)
    return lambda_events


def update_fetch_status(request_id, event_id, status, **kwargs):
    """
    To Update fetch status and details of event data_fetch execution. UPDATE query using id received in insert_prepared_events()

    :param request_id: request_id of request which generated this event
    :param event_id: id of event
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
            UPDATE logging_scraping_pipeline.request_events_details SET fetch_status=(%s), fetch_log=(%s)
            WHERE request_id = {request_id} AND event_id = {event_id};
            """

    rds_conn = db_connection_manager.get_db_connection()
    cur = rds_conn.cursor(row_factory=dict_row)

    cur.execute(query, tuple(set_params.values()))
    rds_conn.commit()
    db_connection_manager.put_db_connection(rds_conn)


def update_process_status(request_id, event_id, status, **kwargs):
    """
    To Update processing status and details of event processing execution after data fetch. UPDATE query using id received in insert_prepared_events()

    :param request_id: request_id of request which generated this event
    :param event_id: id of event
    :param status: status of processing execution attempt
    :param kwargs: to pass 'log' with any additional information about the execution
    :return: None
    """
    set_params = {
        'process_status': status,
        'process_log': None,
    }

    if status == 'FAILED':
        set_params['process_log'] = kwargs['log']

    query = f"""
            UPDATE logging_scraping_pipeline.request_events_details SET process_status=(%s), process_log=(%s)
            WHERE request_id = {request_id} AND event_id = {event_id};
            """

    rds_conn = db_connection_manager.get_db_connection()
    cur = rds_conn.cursor(row_factory=dict_row)

    cur.execute(query, tuple(set_params.values()))
    rds_conn.commit()
    db_connection_manager.put_db_connection(rds_conn)


def update_load_status(request_id, event_id, status, **kwargs):
    """
    To Update load status and details of event processing execution after data fetch. UPDATE query using id received in insert_prepared_events()

    :param request_id: request_id of request which generated this event
    :param event_id: id of event
    :param status: status of loading execution attempt
    :param kwargs: to pass 'log' with any additional information about the execution
    :return: None
    """
    set_params = {
        'load_status': status,
        'load_log': None,
    }

    if status == 'FAILED':
        set_params['load_log'] = kwargs['log']

    query = f"""
            UPDATE logging_scraping_pipeline.request_events_details SET load_status=(%s), load_log=(%s)
            WHERE request_id = {request_id} AND event_id = {event_id};
            """

    rds_conn = db_connection_manager.get_db_connection()
    cur = rds_conn.cursor(row_factory=dict_row)

    cur.execute(query, tuple(set_params.values()))
    rds_conn.commit()
    db_connection_manager.put_db_connection(rds_conn)

# if __name__ == "__main__":
#     update_fetch_status(21, 1, 'IN PROGRESS', status_code=404, log='Traceback: - - - - ')
