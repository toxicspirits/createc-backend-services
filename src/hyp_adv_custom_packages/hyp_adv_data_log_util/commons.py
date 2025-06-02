# ruff: noqa
"""
Few common functions for use across the package
"""

from datetime import datetime, timedelta, timezone


def curr_timestamp():
    """
    To return current timestamp IST

    :return: current_timestamp(string)
    """
    current_timestamp = datetime.now(tz=timezone(timedelta(hours=5.5)))
    current_timestamp = current_timestamp.strftime("%Y-%m-%d %H:%M:%S")

    return current_timestamp

