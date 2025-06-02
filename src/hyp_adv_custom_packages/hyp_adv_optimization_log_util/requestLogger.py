# ruff: noqa

"""
This module is used in the start-init stage of the pipeline.
Logged:- The requests received by pipeline step function, and prepared for the next stages, by step event handlers

"""

from psycopg.rows import dict_row

from .. import db_connection_manager


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
    INSERT INTO logging_optimization_pipeline.request_details(company_id, amazon_profile_id, request_type) 
    VALUES(%s,%s,%s)
    RETURNING request_id;
    """
    for event in lambda_events:
        cur.execute(query, (
            event['company_id'],
            event['profile_id'],
            event['request_type']
        ))
        req_id = cur.fetchone()['request_id']
        event['request_id'] = req_id  # Add request id to the event

    rds_conn.commit()
    db_connection_manager.put_db_connection(rds_conn)
    return lambda_events
