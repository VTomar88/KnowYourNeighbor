# -*- coding: utf-8 -*-

"""
Extend the power of declarative base.
"""

import math
from typing import Union, List, Tuple, Dict, Any
from collections import OrderedDict
from copy import deepcopy

from sqlalchemy import inspect, func, text, select, update, Column
from sqlalchemy.sql.expression import TextClause
from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base, Session, InstrumentedAttribute
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import FlushError

from ..utils import (
    ensure_exact_one_arg_is_not_none, ensure_list, grouper_list,
    ensure_session, clean_session,
)

Base = declarative_base()


class ExtendedBase(Base):
    """
    Provide additional method.

    Example::

        from sqlalchemy.ext.declarative import declarative_base

        Base = declarative_base()

        class User(Base, ExtendedBase):
            ... do what you do with sqlalchemy ORM

    **中文文档**

    提供了三个快捷函数, 分别用于获得列表形式的 primary key names, fields, values

    - :meth:`ExtendedBase.pk_names`
    - :meth:`ExtendedBase.pk_fields`
    - :meth:`ExtendedBase.pk_values`

    另外提供了三个快捷函数, 专门针对只有一个 primary key 的情况, 分别用于获得单个形式的
    primary key name, field, value.

    - :meth:`ExtendedBase.id_field_name`
    - :meth:`ExtendedBase.id_field`
    - :meth:`ExtendedBase.id_field_value`

    所有参数包括 ``engine_or_session`` 的函数需返回

    - 所有的 insert / update
    - 所有的 select 相关的 method 返回的不是 ResultProxy, 因为有 engine_or_session
        这个参数如果输入是 engine, 用于执行的 session 都是临时对象, 离开了这个 method,
        session 将被摧毁. 而返回的 Result 是跟当前的 session 绑定相关的, session 一旦
        被关闭, Result 理应不进行任何后续操作. 所以建议全部返回所有结果的列表而不是迭代器.
    """
    __abstract__ = True

    _settings_major_attrs: list = None

    _cache_pk_names: tuple = None

    # --- No DB interaction APIs ---
    @classmethod
    def pk_names(cls) -> Tuple[str]:
        """
        Primary key column name list.
        """
        if cls._cache_pk_names is None:
            cls._cache_pk_names = tuple([
                col.name for col in inspect(cls).primary_key
            ])
        return cls._cache_pk_names

    _cache_pk_fields: tuple = None

    @classmethod
    def pk_fields(cls) -> Tuple[InstrumentedAttribute]:
        """
        Primary key columns instance. For example::

            class User(Base):
                id = Column(..., primary_key=True)
                name = Column(...)

            User.pk_fields() # (User.id,)

        :rtype: tuple
        """
        if cls._cache_pk_fields is None:
            cls._cache_pk_fields = tuple([
                getattr(cls, name) for name in cls.pk_names()
            ])
        return cls._cache_pk_fields

    def pk_values(self) -> tuple:
        """
        Primary key values

        :rtype: tuple
        """
        return tuple([getattr(self, name) for name in self.pk_names()])

    # id_field_xxx() method are only valid if there's only one primary key

    _id_field_name: str = None

    @classmethod
    def id_field_name(cls) -> str:
        """
        If only one primary_key, then return the name of it.
        Otherwise, raise ValueError.
        """
        if cls._id_field_name is None:
            if len(cls.pk_names()) == 1:
                cls._id_field_name = cls.pk_names()[0]
            else:
                raise ValueError(
                    "{classname} has more than 1 primary key!"
                        .format(classname=cls.__name__)
                )
        return cls._id_field_name

    _id_field = None

    @classmethod
    def id_field(cls) -> InstrumentedAttribute:
        """
        If only one primary_key, then return the Class.field name object.
        Otherwise, raise ValueError.
        """
        if cls._id_field is None:
            cls._id_field = getattr(cls, cls.id_field_name())
        return cls._id_field

    def id_field_value(self):
        """
        If only one primary_key, then return the value of primary key.
        Otherwise, raise ValueError
        """
        return getattr(self, self.id_field_name())

    _cache_keys: List[str] = None

    @classmethod
    def keys(cls) -> List[str]:
        """
        return list of all declared columns.

        :rtype: List[str]
        """
        if cls._cache_keys is None:
            cls._cache_keys = [c.name for c in cls.__table__.columns]
        return cls._cache_keys

    def values(self) -> list:
        """
        return list of value of all declared columns.
        """
        return [getattr(self, c.name, None) for c in self.__table__.columns]

    def items(self) -> List[Tuple[str, Any]]:
        """
        return list of pair of name and value of all declared columns.
        """
        return [
            (c.name, getattr(self, c.name, None))
            for c in self.__table__.columns
        ]

    def __repr__(self):
        kwargs = list()
        for attr, value in self.items():
            kwargs.append("%s=%r" % (attr, value))
        return "%s(%s)" % (self.__class__.__name__, ", ".join(kwargs))

    def __str__(self):
        return self.__repr__()

    def to_dict(self, include_null=True) -> Dict[str, Any]:
        """
        Convert to dict.

        :rtype: dict
        """
        if include_null:
            return dict(self.items())
        else:
            return {
                attr: value
                for attr, value in self.__dict__.items()
                if not attr.startswith("_sa_")
            }

    def to_OrderedDict(
        self,
        include_null: bool = True,
    ) -> OrderedDict:
        """
        Convert to OrderedDict.
        """
        if include_null:
            return OrderedDict(self.items())
        else:
            items = list()
            for c in self.__table__._columns:
                try:
                    items.append((c.name, self.__dict__[c.name]))
                except KeyError:
                    pass
            return OrderedDict(items)

    _cache_major_attrs: tuple = None

    @classmethod
    def _major_attrs(cls):
        if cls._cache_major_attrs is None:
            l = list()
            for item in cls._settings_major_attrs:
                if isinstance(item, Column):
                    l.append(item.name)
                elif isinstance(item, str):
                    l.append(item)
                else:  # pragma: no cover
                    raise TypeError
            if len(set(l)) != len(l):  # pragma: no cover
                raise ValueError
            cls._cache_major_attrs = tuple(l)
        return cls._cache_major_attrs

    def glance(self, _verbose: bool = True):  # pragma: no cover
        """
        Print itself, only display attributes defined in
        :attr:`ExtendedBase._settings_major_attrs`

        :param _verbose: internal param for unit testing
        """
        if self._settings_major_attrs is None:
            msg = ("Please specify attributes you want to include "
                   "in `class._settings_major_attrs`!")
            raise NotImplementedError(msg)

        kwargs = [
            (attr, getattr(self, attr))
            for attr in self._major_attrs()
        ]

        text = "{classname}({kwargs})".format(
            classname=self.__class__.__name__,
            kwargs=", ".join([
                "%s=%r" % (attr, value)
                for attr, value in kwargs
            ])
        )

        if _verbose:  # pragma: no cover
            print(text)

    def absorb(
        self,
        other: 'ExtendedBase',
        ignore_none: bool = True,
    ) -> 'ExtendedBase':
        """
        For attributes of others that value is not None, assign it to self.

        **中文文档**

        将另一个文档中的数据更新到本条文档。当且仅当数据值不为None时。
        """
        if not isinstance(other, self.__class__):
            raise TypeError("`other` has to be a instance of %s!" %
                            self.__class__)

        if ignore_none:
            for attr, value in other.items():
                if value is not None:
                    setattr(self, attr, deepcopy(value))
        else:
            for attr, value in other.items():
                setattr(self, attr, deepcopy(value))

        return self

    def revise(
        self,
        data: dict,
        ignore_none: bool = True,
    ) -> 'ExtendedBase':
        """
        Revise attributes value with dictionary data.

        **中文文档**

        将一个字典中的数据更新到本条文档. 当且仅当数据值不为 None 时.
        """
        if not isinstance(data, dict):
            raise TypeError("`data` has to be a dict!")

        if ignore_none:
            for key, value in data.items():
                if value is not None:
                    setattr(self, key, deepcopy(value))
        else:
            for key, value in data.items():
                setattr(self, key, deepcopy(value))

        return self

    # --- DB interaction APIs ---
    @classmethod
    def by_pk(
        cls,
        engine_or_session: Union[Engine, Session],
        id_: Union[Any, List[Any], Tuple],
    ):
        """
        Get one object by primary_key values.

        Examples::

            class User(Base):
                id = Column(Integer, primary_key)
                name = Column(String)

            with Session(engine) as session:
                session.add(User(id=1, name="Alice")
                session.commit()

            # User(id=1, name="Alice")
            print(User.by_pk(1, engine))
            print(User.by_pk((1,), engine))
            print(User.by_pk([1,), engine))

            with Session(engine) as session:
                print(User.by_pk(1, session))
                print(User.by_pk((1,), session))
                print(User.by_pk([1,), session))

        **中文文档**

        一个简单的语法糖, 允许用户直接用 primary key column 的值访问单个对象.
        """
        ses, auto_close = ensure_session(engine_or_session)
        obj = ses.get(cls, id_)
        clean_session(ses, auto_close)
        return obj

    @classmethod
    def by_sql(
        cls,
        engine_or_session: Union[Engine, Session],
        sql: Union[str, TextClause],
    ) -> List['ExtendedBase']:
        """
        Query with sql statement or texture sql.

        Examples::

            class User(Base):
                id = Column(Integer, primary_key)
                name = Column(String)

            with Session(engine) as session:
                user_list = [
                    User(id=1, name="Alice"),
                    User(id=2, name="Bob"),
                    User(id=3, name="Cathy"),
                ]
                session.add_all(user_list)
                session.commit()

            results = User.by_sql(
                "SELECT * FROM extended_declarative_base_user",
                engine,
            )

            # [User(id=1, name="Alice"), User(id=2, name="Bob"), User(id=3, name="Cathy")]
            print(results)

        **中文文档**

        一个简单的语法糖, 允许用户直接用 SQL 的字符串进行查询.
        """
        if isinstance(sql, str):
            sql_stmt = text(sql)
        elif isinstance(sql, TextClause):
            sql_stmt = sql
        else:  # pragma: no cover
            raise TypeError
        ses, auto_close = ensure_session(engine_or_session)
        results = ses.scalars(select(cls).from_statement(sql_stmt)).all()
        clean_session(ses, auto_close)
        return results

    @classmethod
    def smart_insert(
        cls,
        engine_or_session: Union[Engine, Session],
        obj_or_objs: Union['ExtendedBase', List['ExtendedBase']],
        minimal_size: int = 5,
        _op_counter: int = 0,
        _insert_counter: int = 0,
    ) -> Tuple[int, int]:
        """
        An optimized Insert strategy.
\
        :param minimal_size: internal bulk size for each attempts
        :param _op_counter: number of successful bulk INSERT sql invoked
        :param _insert_counter: number of successfully inserted objects.

        :return: number of bulk INSERT sql invoked. Usually it is
            greatly smaller than ``len(data)``. and also return the number of
            successfully inserted objects.

        .. warning::

            This operation is not atomic, if you force stop the program,
            then it could be only partially completed

        **中文文档**

        在Insert中, 如果已经预知不会出现IntegrityError, 那么使用Bulk Insert的速度要
        远远快于逐条Insert。而如果无法预知, 那么我们采用如下策略:

        1. 尝试Bulk Insert, Bulk Insert由于在结束前不Commit, 所以速度很快。
        2. 如果失败了, 那么对数据的条数开平方根, 进行分包, 然后对每个包重复该逻辑。
        3. 若还是尝试失败, 则继续分包, 当分包的大小小于一定数量时, 则使用逐条插入。
          直到成功为止。

        该 Insert 策略在内存上需要额外的 sqrt(n) 的开销, 跟原数据相比体积很小。
        但时间上是各种情况下平均最优的。

        1.4 以后的重要变化: session 变得更聪明了.
        """
        ses, auto_close = ensure_session(engine_or_session)

        if isinstance(obj_or_objs, list):
            # 首先进行尝试bulk insert
            try:
                ses.add_all(obj_or_objs)
                ses.commit()
                _op_counter += 1
                _insert_counter += len(obj_or_objs)
            # 失败了
            except (IntegrityError, FlushError):
                ses.rollback()
                # 分析数据量
                n = len(obj_or_objs)
                # 如果数据条数多于一定数量
                if n >= minimal_size ** 2:
                    # 则进行分包
                    n_chunk = math.floor(math.sqrt(n))
                    for chunk in grouper_list(obj_or_objs, n_chunk):
                        (
                            _op_counter,
                            _insert_counter,
                        ) = cls.smart_insert(
                            engine_or_session=ses,
                            obj_or_objs=chunk,
                            minimal_size=minimal_size,
                            _op_counter=_op_counter,
                            _insert_counter=_insert_counter,
                        )
                # 否则则一条条地逐条插入
                else:
                    for obj in obj_or_objs:
                        try:
                            ses.add(obj)
                            ses.commit()
                            _op_counter += 1
                            _insert_counter += 1
                        except (IntegrityError, FlushError):
                            ses.rollback()
        else:
            try:
                ses.add(obj_or_objs)
                ses.commit()
                _op_counter += 1
                _insert_counter += 1
            except (IntegrityError, FlushError):
                ses.rollback()

        clean_session(ses, auto_close)

        return _op_counter, _insert_counter

    @classmethod
    def update_all(
        cls,
        engine_or_session: Union[Engine, Session],
        obj_or_objs: Union['ExtendedBase', List['ExtendedBase']],
        include_null: bool = True,
        upsert: bool = False,
    ) -> Tuple[int, int]:
        """
        The :meth:`sqlalchemy.crud.updating.update_all` function in ORM syntax.

        This operation **IS NOT ATOMIC**. It is a greedy operation, trying to
        update as much as it can.

        :param engine_or_session: an engine created by``sqlalchemy.create_engine``.
        :param obj_or_objs: single object or list of object
        :param include_null: update those None value field or not
        :param upsert: if True, then do insert also.

        :return: number of row been changed
        """
        update_counter = 0
        insert_counter = 0

        ses, auto_close = ensure_session(engine_or_session)

        obj_or_objs = ensure_list(obj_or_objs)  # type: List[ExtendedBase]

        objs_to_insert = list()
        for obj in obj_or_objs:
            res = ses.execute(
                update(cls).
                    where(*[
                    field == value
                    for field, value in zip(obj.pk_fields(), obj.pk_values())
                ]).
                    values(**obj.to_dict(include_null=include_null))
            )
            if res.rowcount:
                update_counter += 1
            else:
                objs_to_insert.append(obj)

        if upsert:
            try:
                ses.add_all(objs_to_insert)
                ses.commit()
                insert_counter += len(objs_to_insert)
            except (IntegrityError, FlushError):  # pragma: no cover
                ses.rollback()
        else:
            ses.commit()

        clean_session(ses, auto_close)

        return update_counter, insert_counter

    @classmethod
    def upsert_all(
        cls,
        engine_or_session: Union[Engine, Session],
        obj_or_objs: Union['ExtendedBase', List['ExtendedBase']],
        include_null: bool = True,
    ) -> Tuple[int, int]:
        """
        The :meth:`sqlalchemy.crud.updating.upsert_all` function in ORM syntax.

        :param engine_or_session: an engine created by``sqlalchemy.create_engine``.
        :param obj_or_objs: single object or list of object
        :param include_null: update those None value field or not

        :return: number of row been changed
        """
        return cls.update_all(
            engine_or_session=engine_or_session,
            obj_or_objs=obj_or_objs,
            include_null=include_null,
            upsert=True,
        )

    @classmethod
    def delete_all(
        cls,
        engine_or_session: Union[Engine, Session],
    ):  # pragma: no cover
        """
        Delete all data in this table.

        TODO: add a boolean flag for cascade remove
        """
        ses, auto_close = ensure_session(engine_or_session)
        ses.execute(cls.__table__.delete())
        ses.commit()
        clean_session(ses, auto_close)

    @classmethod
    def count_all(
        cls,
        engine_or_session: Union[Engine, Session],
    ) -> int:
        """
        Return number of rows in this table.
        """
        ses, auto_close = ensure_session(engine_or_session)
        count = ses.execute(select(func.count()).select_from(cls)).one()[0]
        clean_session(ses, auto_close)
        return count

    @classmethod
    def select_all(
        cls,
        engine_or_session: Union[Engine, Session],
    ) -> List['ExtendedBase']:
        """

        """
        ses, auto_close = ensure_session(engine_or_session)
        results = ses.scalars(select(cls)).all()
        clean_session(ses, auto_close)
        return results

    @classmethod
    def random_sample(
        cls,
        engine_or_session: Union[Engine, Session],
        limit: int = None,
        perc: int = None,
    ) -> List['ExtendedBase']:
        """
        Return random ORM instance.

        :rtype: List[ExtendedBase]
        """
        ses, auto_close = ensure_session(engine_or_session)
        ensure_exact_one_arg_is_not_none(limit, perc)
        if limit is not None:
            results = ses.scalars(
                select(cls).order_by(func.random()).limit(limit)
            ).all()
        elif perc is not None:
            selectable = cls.__table__.tablesample(
                func.bernoulli(perc),
                name="alias",
                seed=func.random()
            )
            args = [
                getattr(selectable.c, column.name)
                for column in cls.__table__.columns
            ]
            stmt = select(*args)
            results = [cls(**dict(row)) for row in ses.execute(stmt)]
        else:
            raise ValueError
        clean_session(ses, auto_close)
        return results
