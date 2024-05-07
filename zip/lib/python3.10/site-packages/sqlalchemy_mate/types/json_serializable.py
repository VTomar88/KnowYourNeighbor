# -*- coding: utf-8 -*-

"""
Json serializable type.
"""

import typing
try:
    from superjson import json
except ImportError:  # pragma: no cover
    import json
except:  # pragma: no cover
    import json
import sqlalchemy as sa


class JSONSerializableType(sa.types.TypeDecorator):
    """
    This column store json serialized python object in form of

    This column should be a json serializable python type such as combination of
    list, dict, string, int, float, bool.

    Usage:

        import jsonpickle

        # a custom python class
        class ComputerDetails:
            def __init__(self, ...):
                ...

            def to_json(self) -> str:
                return jsonpickle.encode(self)

            @classmethod
            def from_json(cls, json_str: str) -> 'Computer':
                return cls(**jsonpickle.decode(json_str))

        Base = declarative_base()

        class Computer(Base):
            id = Column(Integer, primary_key)
            details = Column(JSONSerializableType(factory_class=Computer)

            ...

        computer = Computer(
            id=1,
            details=ComputerDetails(...),
        )

        with Session(engine) as session:
            session.add(computer)
            session.commit()

            computer = session.get(Computer, 1)
            print(computer.details)
    """
    impl = sa.UnicodeText
    cache_ok = True

    _FACTORY_CLASS = "factory_class"

    def __init__(self, *args, **kwargs):
        if self._FACTORY_CLASS not in kwargs:
            raise ValueError(
                (
                    "'JSONSerializableType' take only ONE argument {}, "
                    "it is the generic type that has ``to_json(self): -> str``, "
                    "and ``from_json(cls, value: str):`` class method."
                ).format(self._FACTORY_CLASS)
            )
        self.factory_class = kwargs.pop(self._FACTORY_CLASS)
        super(JSONSerializableType, self).__init__(*args, **kwargs)

    def load_dialect_impl(self, dialect):
        return self.impl

    def process_bind_param(self, value, dialect) -> typing.Union[str, None]:
        if value is None:
            return value
        else:
            return value.to_json()

    def process_result_value(self, value: typing.Union[str, None], dialect):
        if value is None:
            return value
        else:
            return self.factory_class.from_json(value)
