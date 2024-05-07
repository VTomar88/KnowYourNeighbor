# -*- coding: utf-8 -*-

"""
This module provide utility functions for delete operation.
"""

from sqlalchemy import Table
from sqlalchemy.engine import Engine


def delete_all(
    engine: Engine,
    table: Table,
):
    """
    Delete all data in a table.
    """
    with engine.connect() as connection:
        connection.execute(table.delete())
