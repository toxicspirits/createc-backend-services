# ruff: noqa

""""
Database connection pool and conn ops functions

Based on Psycopg3
-> pip install "psycopg[binary]" psycopg_pool
"""

import os

import psycopg_pool

# Configurations of Connection Pool
if 'DB_CONN_POOL_MIN_SIZE' in os.environ:
    MIN_SIZE = int(os.environ['DB_CONN_POOL_MIN_SIZE'])
else:
    MIN_SIZE = 1

if 'DB_CONN_POOL_MAX_SIZE' in os.environ:
    MAX_SIZE = int(os.environ['DB_CONN_POOL_MAX_SIZE'])
else:
    MAX_SIZE = 5

if 'DB_GET_CONN_TIMEOUT' in os.environ:
    GET_CONN_TIMEOUT = int(os.environ['DB_GET_CONN_TIMEOUT'])
else:
    GET_CONN_TIMEOUT = 30

CONN_STRING = """
    dbname=hyp_adv_db
    user=hyp_adv_user
    password=5@CCvs@Z35
    host=hyperads.cr5hatqwc5ag.ap-south-1.rds.amazonaws.com
    port=5432
    """


# Creating connection pool. Lambda reuses this when warm-starts
_DB_CONNECTION_POOL = psycopg_pool.ConnectionPool(
    # environ setting in lambda to set min & max connections of pool
    min_size=MIN_SIZE, max_size=MAX_SIZE,

    conninfo=CONN_STRING,
    open=True,
    timeout=GET_CONN_TIMEOUT
)


def get_db_connection():
    """
    Gets a connection from connection pool
    :return:
    """
    try:
        conn = _DB_CONNECTION_POOL.getconn()

        print("Total connections: ", _DB_CONNECTION_POOL.get_stats())
        return conn
    except Exception as e:
        raise Exception(f"getConn() from Pool failed{e}") from e


def put_db_connection(conn):
    """
    Puts connection back into connection pool
    :param conn:
    :return:
    """
    try:
        _DB_CONNECTION_POOL.putconn(conn)
        return True
    except Exception as e:
        raise Exception(f"putConn() into Pool failed{e}") from e

