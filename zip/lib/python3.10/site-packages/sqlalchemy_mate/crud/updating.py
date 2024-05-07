# -*- coding: utf-8 -*-

"""
This module provide utility functions for update operation.
"""

from typing import Union, List, Tuple, Dict, Any
from collections import OrderedDict
from sqlalchemy import and_
from sqlalchemy import Table
from sqlalchemy.engine import Engine

from ..utils import ensure_list


def update_all(
    engine: Engine,
    table: Table,
    data: Union[Dict[str, Any], List[Dict[str, Any]]],
    upsert=False,
) -> Tuple[int, int]:
    """
    Update data by its primary_key column values. By default upsert is False.
    """
    with engine.connect() as connection:
        update_counter = 0
        insert_counter = 0

        data = ensure_list(data)

        ins = table.insert()
        upd = table.update()

        # Find all primary key columns
        pk_cols = OrderedDict()
        for column in table._columns:
            if column.primary_key:
                pk_cols[column.name] = column

        data_to_insert = list()

        # Multiple primary key column
        if len(pk_cols) >= 2:
            for row in data:
                result = engine.execute(
                    upd.where(
                        and_(
                            *[col == row[name] for name, col in pk_cols.items()]
                        )
                    ).values(**row)
                )
                if result.rowcount == 0:
                    data_to_insert.append(row)
                else:
                    update_counter += 1
        # Single primary key column
        elif len(pk_cols) == 1:
            for row in data:
                result = engine.execute(
                    upd.where(
                        [col == row[name] for name, col in pk_cols.items()][0]
                    ).values(**row)
                )
                if result.rowcount == 0:
                    data_to_insert.append(row)
                else:
                    update_counter += 1
        else:  # pragma: no cover
            data_to_insert = data

        # Insert rest of data
        if upsert:
            if len(data_to_insert):
                engine.execute(ins, data_to_insert)
                insert_counter += len(data_to_insert)

        return update_counter, insert_counter


def upsert_all(
    engine: Engine,
    table: Table,
    data: Union[Dict[str, Any], List[Dict[str, Any]]],
) -> Tuple[int, int]:
    """
    Update data by primary key columns. If not able to update, do insert.

    Example::

        # define data model
        t_user = Table(
            "users", metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String),
        )

        # suppose in database we already have {"id": 1, "name": "Alice"}
        data = [
            {"id": 1, "name": "Bob"}, # this will be updated
            {"id": 2, "name": "Cathy"}, # this will be added
        ]
        update_count, insert_count = upsert_all(engine, t_user, data)
        print(update_count) # number of row updated counter
        print(insert_count) # number of row inserted counter

        # will return: [{"id": 1, "name": "Bob"}, {"id": 2, "name": "Cathy"}]
        with engine.connect() as connection:
            print(connection.execute(select([table_user])).all())

    **中文文档**

    批量更新文档. 如果该表格定义了Primary Key, 则用Primary Key约束where语句. 对于
    where语句无法找到的行, 自动进行批量 bulk insert.
    """
    return update_all(engine=engine, table=table, data=data, upsert=True)
