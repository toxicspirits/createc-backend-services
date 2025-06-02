# ruff: noqa

"""
This module is used in the start-init stage of the pipeline.

"""

import json

from psycopg.rows import dict_row

from .. import db_connection_manager

# def insert_prepared_requests(lambda_events):
#     """
#     TODO: general flow reporting
#
#     :param lambda_events:  list of prepared events
#     :return: lambda_events(list)
#     """
#     rds_conn = db_connection_manager.get_db_connection()
#     cur = rds_conn.cursor(row_factory=dict_row)
#     query = """
#     INSERT INTO logging_data_pipeline.request_details(created_at, company_id, start_date, end_date, data_to_fetch)
#     VALUES(%s,%s,%s,%s,%s)
#     RETURNING request_id;
#     """
#     for event in lambda_events:
#         cur.execute(query, (
#             commons.curr_timestamp(),
#             event['company_id'],
#             event['start_date'],
#             event['end_date'],
#             json.dumps(event['data_to_fetch'])
#         ))
#         req_id = cur.fetchone()['request_id']
#         event['request_id'] = req_id  # add request id to the event
#
#     rds_conn.commit()
#     db_connection_manager.put_db_connection(rds_conn)
#     return lambda_events


def insert_prepared_amz_requests(lambda_events):
    """
    Note:-  For amazon reporting flow
    Create an entry in table for received new requests and returns same list with request_id appended, where request_id is returned from DB (INSERT INTO ..... RETURNING)

    :param lambda_events:  list of prepared events
    :return: lambda_events(list)
    """
    rds_conn = db_connection_manager.get_db_connection()
    cur = rds_conn.cursor(row_factory=dict_row)
    query = """
    INSERT INTO logging_reports_pipeline.request_details(company_id, amazon_profile_id, operation_type, detail_json, email_status) 
    VALUES(%s,%s,%s,%s,%s)
    RETURNING request_id;
    """
    for event in lambda_events:
        cur.execute(query, (
            event['company_id'],
            event['profile_id'],
            event['operation_type'],
            json.dumps(event['detail_json']),
            event['email_status']
        ))
        req_id = cur.fetchone()['request_id']
        event['request_id'] = req_id  # Add request id to the event

    rds_conn.commit()
    db_connection_manager.put_db_connection(rds_conn)
    return lambda_events


def update_email_status(request_id, status, **kwargs):
    """

    To Update email status of request. UPDATE query using id received in insert_prepared_events()

    :param request_id: request_id of request which generated this event
    :param status: status of data_fetch execution attempt
    :return: None
    """
    set_params = {
        "email_status": status,
        "email_log": None
    }

    if 'log' in kwargs:
        set_params['email_log'] = kwargs['log']

    query = f"""
            UPDATE logging_reports_pipeline.request_details SET email_status=(%s), email_log=(%s)
            WHERE request_id = {request_id};
            """

    rds_conn = db_connection_manager.get_db_connection()
    cur = rds_conn.cursor(row_factory=dict_row)

    cur.execute(query, tuple(set_params.values()))
    rds_conn.commit()
    db_connection_manager.put_db_connection(rds_conn)
