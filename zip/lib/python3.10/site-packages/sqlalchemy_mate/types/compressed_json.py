# -*- coding: utf-8 -*-

"""
Compressed json type.
"""

import zlib
import json
import sqlalchemy as sa


class CompressedJSONType(sa.types.TypeDecorator):
    """
    This column store json serialized object and automatically compress it
    in form of binary before writing to the database.

    This column should be a json serializable python type such as combination of
    list, dict, string, int, float, bool. Also you can use other standard
    json api compatible library for better serialization / deserialization
    support.

    **NOTE**, this type doesn't support JSON path query, it treats the object
    as a whole and compress it to save storage only.

    :param json_lib: optional, the json library you want to use. It should have
        ``json.dumps`` method takes object as first arg, and returns a json
        string. Should also have ``json.loads`` method takes string as
        first arg, returns the original object.

    .. code-block:: python

        # standard json api compatible json library
        import jsonpickle

        class Order(Base):
            ...

            id = Column(Integer, primary_key=True)
            items = CompressedJSONType(json_lib=jsonpickle)

        items = [
            {"item_name": "apple", "quantity": 12},
            {"item_name": "banana", "quantity": 6},
            {"item_name": "cherry", "quantity": 3},
        ]

        order = Order(id=1, items=items)
        with Session(engine) as ses:
            ses.add(order)
            ses.save()

            order = ses.get(Order, 1)
            assert order.items == items

            # WHERE ... = ... also works
            stmt = select(Order).where(Order.items==items)
            order = ses.scalars(stmt).one()
    """
    impl = sa.LargeBinary
    cache_ok = True

    _JSON_LIB = "json_lib"

    def __init__(self, *args, **kwargs):
        if self._JSON_LIB in kwargs:
            self.json_lib = kwargs.pop(self._JSON_LIB)
        else:
            self.json_lib = json
        super(CompressedJSONType, self).__init__(*args, **kwargs)

    def load_dialect_impl(self, dialect):
        return dialect.type_descriptor(self.impl)

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        return zlib.compress(
            self.json_lib.dumps(value).encode("utf-8")
        )

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return self.json_lib.loads(
            zlib.decompress(value).decode("utf-8")
        )
