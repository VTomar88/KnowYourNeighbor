# -*- coding: utf-8 -*-

import sys
import typing

from sqlalchemy import String, Integer
from sqlalchemy import create_engine, MetaData, Table, Column
from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base, Session

from ..engine_creator import EngineCreator
from ..orm.extended_declarative_base import ExtendedBase

IS_WINDOWS = sys.platform.lower().startswith("win")

# use make run-psql to run postgres container on local
engine_sqlite = create_engine("sqlite:///:memory:")

engine_psql = EngineCreator(
    username="postgres",
    password="password",
    database="postgres",
    host="localhost",
    port=40311,
).create_postgresql_pg8000()

metadata = MetaData()

t_user = Table(
    "t_user", metadata,
    Column("user_id", Integer, primary_key=True),
    Column("name", String),
)

t_inv = Table(
    "t_inventory", metadata,
    Column("store_id", Integer, primary_key=True),
    Column("item_id", Integer, primary_key=True),
)

t_smart_insert = Table(
    "t_smart_insert", metadata,
    Column("id", Integer, primary_key=True),
)

t_cache = Table(
    "t_cache", metadata,
    Column("key", String(), primary_key=True),
    Column("value", Integer()),
)

t_graph = Table(
    "t_edge", metadata,
    Column("x_node_id", Integer, primary_key=True),
    Column("y_node_id", Integer, primary_key=True),
    Column("value", Integer),
)

# --- Orm
Base = declarative_base()


class User(Base, ExtendedBase):
    __tablename__ = "extended_declarative_base_user"

    _settings_major_attrs = ["id", "name"]

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)


class Association(Base, ExtendedBase):
    __tablename__ = "extended_declarative_base_association"

    x_id = Column(Integer, primary_key=True)
    y_id = Column(Integer, primary_key=True)
    flag = Column(Integer)


class Order(Base, ExtendedBase):
    __tablename__ = "extended_declarative_base_order"

    id = Column(Integer, primary_key=True)


class BankAccount(Base, ExtendedBase):
    __tablename__ = "extended_declarative_base_edge_case_bank_account"

    _settings_major_attrs = ["id", "name"]

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    pin = Column(String)


class PostTagAssociation(Base, ExtendedBase):
    __tablename__ = "extended_declarative_base_edge_case_post_tag_association"

    post_id = Column(Integer, primary_key=True)
    tag_id = Column(Integer, primary_key=True)
    description = Column(String)


class BaseTest:
    engine: Engine = None

    @property
    def eng(self) -> Engine:
        """
        shortcut for ``self.engine``
        """
        return self.engine

    @classmethod
    def setup_class(cls):
        """
        It is called one once before all test method start.

        Don't overwrite this method in Child Class!
        Use :meth:`BaseTest.class_level_data_setup` please
        """
        if cls.engine is not None:
            metadata.create_all(cls.engine)
            Base.metadata.create_all(cls.engine)
        cls.class_level_data_setup()

    @classmethod
    def teardown_class(cls):
        """
        It is called one once when all test method finished.

        Don't overwrite this method in Child Class!
        Use :meth:`BaseTest.class_level_data_teardown` please
        """
        cls.class_level_data_teardown()

    def setup_method(self, method):
        """
        It is called before all test method invocation

        Don't overwrite this method in Child Class!
        Use :meth:`BaseTest.method_level_data_setup` please
        """
        self.method_level_data_setup()

    def teardown_method(self, method):
        """
        It is called after all test method invocation.

        Don't overwrite this method in Child Class!
        Use :meth:`BaseTest.method_level_data_teardown` please
        """
        self.method_level_data_teardown()

    @classmethod
    def class_level_data_setup(cls):
        """
        Put data preparation task here.
        """
        pass

    @classmethod
    def class_level_data_teardown(cls):
        """
        Put data cleaning task here.
        """
        pass

    def method_level_data_setup(self):
        """
        Put data preparation task here.
        """
        pass

    def method_level_data_teardown(self):
        """
        Put data cleaning task here.
        """
        pass

    @classmethod
    def delete_all_data_in_core_table(cls):
        with cls.engine.connect() as connection:
            connection.execute(t_user.delete())
            connection.execute(t_inv.delete())
            connection.execute(t_cache.delete())
            connection.execute(t_graph.delete())
            connection.execute(t_smart_insert.delete())

    @classmethod
    def delete_all_data_in_orm_table(cls):
        with cls.engine.connect() as connection:
            connection.execute(User.__table__.delete())
            connection.execute(Association.__table__.delete())
            connection.execute(Order.__table__.delete())
            connection.execute(BankAccount.__table__.delete())
            connection.execute(PostTagAssociation.__table__.delete())
