# -*- coding: utf-8 -*-

"""
This module provide some custom data types that automatically compress before
writing to the database.
"""

import zlib
import typing
import sqlalchemy as sa


class BaseCompressedType(sa.types.TypeDecorator):
    def __init__(self, *args, **kwargs):
        super(BaseCompressedType, self).__init__(*args, **kwargs)

    def load_dialect_impl(self, dialect):
        return dialect.type_descriptor(self.impl)

    def _compress(self, value) -> typing.Union[bytes, None]:
        raise NotImplementedError

    def _decompress(self, value: typing.Union[bytes, None]):
        raise NotImplementedError

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        return self._compress(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return self._decompress(value)


class CompressedStringType(BaseCompressedType):
    """
    Compressed unicode string.
    """
    impl = sa.LargeBinary
    cache_ok = True

    def _compress(self, value: typing.Union[str, None]) -> typing.Union[bytes, None]:
        if value is None:
            return None
        return zlib.compress(value.encode("utf-8"))

    def _decompress(self, value: typing.Union[bytes, None]) -> typing.Union[str, None]:
        if value is None:
            return None
        return zlib.decompress(value).decode("utf-8")


class CompressedBinaryType(BaseCompressedType):
    """
    Compressed binary data.
    """
    impl = sa.LargeBinary
    cache_ok = True

    def _compress(self, value: typing.Union[bytes, None]) -> typing.Union[bytes, None]:
        if value is None:
            return None
        return zlib.compress(value)

    def _decompress(self, value: typing.Union[bytes, None]) -> typing.Union[bytes, None]:
        if value is None:
            return None
        return zlib.decompress(value)
