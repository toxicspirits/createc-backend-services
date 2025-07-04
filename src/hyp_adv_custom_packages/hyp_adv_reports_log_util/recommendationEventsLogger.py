# ruff: noqa

"""
This module is used to log the individual components/sub-events of a received request.
Ex: individual reports for a recommendation report request.

"""

import json

from psycopg.rows import dict_row

from .. import db_connection_manager
from . import commons


def insert_prepared_events(event):
    """
    Create an entry in table for received new events and returns same list with event_id appended, where event_id is returned from DB (INSERT INTO ..... RETURNING)

    :param lambda_events:  list of prepared events
    :return: lambda_events(list)
    """
    rds_conn = db_connection_manager.get_db_connection()
    cur = rds_conn.cursor(row_factory=dict_row)
    query = """
    INSERT INTO logging_reports_pipeline.recommendation_event_details(company_id, request_id, profile_id, status, detail_json) 
    VALUES(%s, %s, %s, %s, %s) ON CONFLICT(request_id) DO UPDATE SET (status) = ROW(excluded.status)
    """
    cur.execute(query, (event['company_id'], event['request_id'], event['profile_id'], "PENDING", json.dumps(event), ))
        # event_id = cur.fetchone()['event_id']
        # event['event_id'] = event_id  # Add request id to the event

    rds_conn.commit()
    db_connection_manager.put_db_connection(rds_conn)
    #return lambda_events


def update_event_status(request_id, status, **kwargs):
    """

    To Update event status and event_log for the execution. UPDATE query using id received in insert_prepared_events()

    :param request_id: request_id of request which generated this event
    :param event_id: id of event
    :param status: status of data_fetch execution attempt
    :param kwargs: to pass 'log' with any additional information about the execution
    :return: None
    """
    set_params = {
        "event_status": status,
        "event_log": None
    }

    if 'log' in kwargs:
        set_params['event_log'] = kwargs['log']

    if 'recommendation_ids' in kwargs:
        set_params["recommendation_ids"] = kwargs['recommendation_ids']
        query = f"""
        UPDATE logging_reports_pipeline.recommendation_event_details SET status=(%s), 
        event_log=(%s), updated_at='{commons.curr_timestamp()}', recommendation_ids = (%s) where request_id = '{request_id}'
        """
    else:
        query = f"""
                UPDATE logging_reports_pipeline.recommendation_event_details SET status=(%s), event_log=(%s), updated_at = '{commons.curr_timestamp()}'
                WHERE request_id = {request_id};
                """

    rds_conn = db_connection_manager.get_db_connection()
    cur = rds_conn.cursor(row_factory=dict_row)

    cur.execute(query, tuple(set_params.values()))
    rds_conn.commit()
    db_connection_manager.put_db_connection(rds_conn)
