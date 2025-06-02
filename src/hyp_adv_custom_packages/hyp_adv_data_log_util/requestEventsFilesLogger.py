# ruff: noqa
"""
This module is used in the processing stage of the pipeline.
It logs the files which are generated, which are later loaded onto DB

"""

from psycopg.rows import dict_row

from .. import db_connection_manager


def insert_events_files(event, s3_uri_list):
    """
    Create an entry in table for new files and creates new list of events with file_id returned from DB (INSERT INTO ..... RETURNING)

    :param event: event with all details of files, and parent event_id
    :param s3_uri_list: list of all file s3_uris
    :return: return_events(list)
    """
    rds_conn = db_connection_manager.get_db_connection()
    cur = rds_conn.cursor(row_factory=dict_row)
    query = """
    INSERT INTO logging_data_pipeline.request_events_files_details(event_id, s3_object_uri) 
    VALUES(%s,%s)
    RETURNING file_id;
    """
    return_events = []
    for s3_uri in s3_uri_list:
        cur.execute(query, (
            event['event_id'],
            s3_uri
        ))
        file_id = cur.fetchone()['file_id']
        return_events.append(
            {
                "event_id": event['event_id'],
                "file_id": file_id,
                "s3_object_uri": s3_uri
            }
        )

    rds_conn.commit()
    db_connection_manager.put_db_connection(rds_conn)
    return return_events


def update_load_status(event_id, file_id, status, **kwargs):
    """
    To Update status and details of file loading. UPDATE query using id received in insert_events_files()

    :param event_id: event_id of event which generated this file
    :param file_id: id of file
    :param status: status of the load attempt
    :param kwargs: if status FAILED, to also pass a 'log' with additional details
    :return: None
    """
    set_params = {
        'load_status': status,
        'load_log': None,
    }

    if status == 'FAILED':
        set_params['load_log'] = kwargs['log']

    query = f"""
            UPDATE logging_data_pipeline.request_events_files_details SET load_status=(%s), load_log=(%s)
            WHERE event_id = {event_id} AND file_id = {file_id};
            """

    rds_conn = db_connection_manager.get_db_connection()
    cur = rds_conn.cursor(row_factory=dict_row)

    cur.execute(query, tuple(set_params.values()))
    rds_conn.commit()
    db_connection_manager.put_db_connection(rds_conn)
