# -*- coding: utf-8 -*-

"""
Safe database credential loader.
"""

import os
import json
import string
import sqlalchemy as sa
from sqlalchemy.engine import Engine


class EngineCreator:  # pragma: no cover
    """
    Tired of looking up docs on https://docs.sqlalchemy.org/en/latest/core/engines.html?

    ``EngineCreator`` creates sqlalchemy engine in one line:

    Example::

        from sqlalchemy_mate import EngineCreator

        # sqlite in memory
        engine = EngineCreator.create_sqlite()

        # connect to postgresql, credential stored at ``~/.db.json``
        # content of ``.db.json``
        {
            "mydb": {
                "host": "example.com",
                "port": 1234,
                "database": "test",
                "username": "admin",
                "password": "admin"
            },
            ...
        }
        engine = EngineCreator.from_home_db_json("mydb").create_postgresql()
    """

    def __init__(
        self,
        host=None,
        port=None,
        database=None,
        username=None,
        password=None,
    ):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password

    uri_template = "{username}{has_password}{password}@{host}{has_port}{port}/{database}"
    path_db_json = os.path.join(os.path.expanduser("~"), ".db.json")
    local_home = os.path.basename(os.path.expanduser("~"))

    def __repr__(self):
        return "{classname}(host='{host}', port={port}, database='{database}', username={username}, password='xxxxxxxxxxxx')".format(
            classname=self.__class__.__name__,
            host=self.host, port=self.port,
            database=self.database, username=self.username,
        )

    @property
    def uri(self) -> str:
        """
        Return sqlalchemy connect string URI.
        """
        return self.uri_template.format(
            host=self.host,
            port="" if self.port is None else self.port,
            database=self.database,
            username=self.username,
            password="" if self.password is None else self.password,
            has_password="" if self.password is None else ":",
            has_port="" if self.port is None else ":",
        )

    @classmethod
    def _validate_key_mapping(cls, key_mapping):
        if key_mapping is not None:
            keys = list(key_mapping)
            keys.sort()
            if keys != ["database", "host", "password", "port", "username"]:
                msg = ("`key_mapping` is the credential field mapping from `Credential` to custom json! "
                       "it has to be a dictionary with 5 keys: "
                       "host, port, password, port, username!")
                raise ValueError(msg)

    @classmethod
    def _transform(cls, data, key_mapping):
        if key_mapping is None:
            return data
        else:
            return {actual: data[custom] for actual, custom in key_mapping.items()}

    @classmethod
    def _from_json_data(cls, data, json_path=None, key_mapping=None):
        if json_path is not None:
            for p in json_path.split("."):
                data = data[p]
        return cls(**cls._transform(data, key_mapping))

    @classmethod
    def from_json(
        cls,
        json_file: str,
        json_path: str = None,
        key_mapping: dict = None,
    ) -> 'EngineCreator':
        """
        Load connection credential from json file.

        :param json_file: str, path to json file
        :param json_path: str, dot notation of the path to the credential dict.
        :param key_mapping: dict, map 'host', 'port', 'database', 'username', 'password'
            to custom alias, for example ``{'host': 'h', 'port': 'p', 'database': 'db', 'username': 'user', 'password': 'pwd'}``. This params are used to adapt any json data.

        :rtype:
        :return:

        Example:

        Your json file::

            {
                "credentials": {
                    "db1": {
                        "h": "example.com",
                        "p": 1234,
                        "db": "test",
                        "user": "admin",
                        "pwd": "admin",
                    },
                    "db2": {
                        ...
                    }
                }
            }

        Usage::

            cred = Credential.from_json(
                "path-to-json-file", "credentials.db1",
                dict(host="h", port="p", database="db", username="user", password="pwd")
            )
        """
        cls._validate_key_mapping(key_mapping)
        with open(json_file, "rb") as f:
            data = json.loads(f.read().decode("utf-8"))
            return cls._from_json_data(data, json_path, key_mapping)

    @classmethod
    def from_home_db_json(
        cls,
        identifier: str,
        key_mapping: dict = None,
    ) -> 'EngineCreator':  # pragma: no cover
        """
        Read credential from $HOME/.db.json file.

        :type identifier: str
        :param identifier: str, database identifier.

        :type key_mapping: Dict[str, str]
        :param key_mapping: dict

        ``.db.json````::

            {
                "identifier1": {
                    "host": "example.com",
                    "port": 1234,
                    "database": "test",
                    "username": "admin",
                    "password": "admin",
                },
                "identifier2": {
                    ...
                }
            }
        """
        return cls.from_json(
            json_file=cls.path_db_json, json_path=identifier, key_mapping=key_mapping)

    @classmethod
    def from_s3_json(
        cls,
        bucket_name: str,
        key: str,
        json_path: str = None,
        key_mapping: dict = None,
        aws_profile: str = None,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
        region_name: str = None,
    ) -> 'EngineCreator':  # pragma: no cover
        """
        Load database credential from json on s3.

        :param bucket_name: str
        :param key: str
        :param aws_profile: if None, assume that you are using this from
            AWS cloud. (service on the same cloud doesn't need profile name)
        :param aws_access_key_id: str, not recommend to use
        :param aws_secret_access_key: str, not recommend to use
        :param region_name: str
        """
        import boto3

        ses = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
            profile_name=aws_profile,
        )
        s3 = ses.resource("s3")
        bucket = s3.Bucket(bucket_name)
        object = bucket.Object(key)
        data = json.loads(object.get()["Body"].read().decode("utf-8"))
        return cls._from_json_data(data, json_path, key_mapping)

    @classmethod
    def from_env(
        cls,
        prefix: str,
        kms_decrypt: bool = False,
        aws_profile: str = None,
    ) -> 'EngineCreator':
        """
        Load database credential from env variable.

        - host: ENV.{PREFIX}_HOST
        - port: ENV.{PREFIX}_PORT
        - database: ENV.{PREFIX}_DATABASE
        - username: ENV.{PREFIX}_USERNAME
        - password: ENV.{PREFIX}_PASSWORD

        :param prefix: str
        :param kms_decrypt: bool
        :param aws_profile: str
        """
        if len(prefix) < 1:
            raise ValueError("prefix can't be empty")

        if len(set(prefix).difference(set(string.ascii_uppercase + "_"))):
            raise ValueError("prefix can only use [A-Z] and '_'!")

        if not prefix.endswith("_"):
            prefix = prefix + "_"

        data = dict(
            host=os.getenv(prefix + "HOST"),
            port=os.getenv(prefix + "PORT"),
            database=os.getenv(prefix + "DATABASE"),
            username=os.getenv(prefix + "USERNAME"),
            password=os.getenv(prefix + "PASSWORD"),
        )
        if kms_decrypt is True:  # pragma: no cover
            import boto3
            from base64 import b64decode

            if aws_profile is not None:
                kms = boto3.client("kms")
            else:
                ses = boto3.Session(profile_name=aws_profile)
                kms = ses.client("kms")

            def decrypt(kms, text):
                return kms.decrypt(
                    CiphertextBlob=b64decode(text.encode("utf-8"))
                )["Plaintext"].decode("utf-8")

            data = {
                key: value if value is None else decrypt(kms, str(value))
                for key, value in data.items()
            }

        return cls(**data)

    def to_dict(self):
        """
        Convert credentials into a dict.
        """
        return dict(
            host=self.host,
            port=self.port,
            database=self.database,
            username=self.username,
            password=self.password,
        )

    # --- engine creator logic
    def create_connect_str(self, dialect_and_driver) -> str:
        return "{}://{}".format(dialect_and_driver, self.uri)

    _ccs = create_connect_str

    def create_engine(self, conn_str, **kwargs) -> Engine:
        """
        :rtype: Engine
        """
        return sa.create_engine(conn_str, **kwargs)

    _ce = create_engine

    @classmethod
    def create_sqlite(cls, path=":memory:", **kwargs):
        """
        Create sqlite engine.
        """
        return sa.create_engine("sqlite:///{path}".format(path=path), **kwargs)

    class DialectAndDriver(object):
        """
        DB dialect and DB driver mapping.
        """
        psql = "postgresql"
        psql_psycopg2 = "postgresql+psycopg2"
        psql_pg8000 = "postgresql+pg8000"
        psql_pygresql = "postgresql+pygresql"
        psql_psycopg2cffi = "postgresql+psycopg2cffi"
        psql_pypostgresql = "postgresql+pypostgresql"
        mysql = "mysql"
        mysql_mysqldb = "mysql+mysqldb"
        mysql_mysqlconnector = "mysql+mysqlconnector"
        mysql_oursql = "mysql+oursql"
        mysql_pymysql = "mysql+pymysql"
        mysql_cymysql = "mysql+cymysql"
        oracle = "oracle"
        oracle_cx_oracle = "oracle+cx_oracle"
        mssql_pyodbc = "mssql+pyodbc"
        mssql_pymssql = "mssql+pymssql"
        redshift_psycopg2 = "redshift+psycopg2"

    # postgresql
    def create_postgresql(self, **kwargs):
        """
        :rtype: Engine
        """
        return self._ce(
            self._ccs(self.DialectAndDriver.psql), **kwargs
        )

    def create_postgresql_psycopg2(self, **kwargs):
        """
        :rtype: Engine
        """
        return self._ce(
            self._ccs(self.DialectAndDriver.psql_psycopg2), **kwargs
        )

    def create_postgresql_pg8000(self, **kwargs):
        """
        :rtype: Engine
        """
        return self._ce(
            self._ccs(self.DialectAndDriver.psql_pg8000), **kwargs
        )

    def _create_postgresql_pygresql(self, **kwargs):
        """
        :rtype: Engine
        """
        return self._ce(
            self._ccs(self.DialectAndDriver.psql_pygresql), **kwargs
        )

    def create_postgresql_psycopg2cffi(self, **kwargs):
        """
        :rtype: Engine
        """
        return self._ce(
            self._ccs(self.DialectAndDriver.psql_psycopg2cffi), **kwargs
        )

    def create_postgresql_pypostgresql(self, **kwargs):
        """
        :rtype: Engine
        """
        return self._ce(
            self._ccs(self.DialectAndDriver.psql_pypostgresql), **kwargs
        )

    # mysql
    def create_mysql(self, **kwargs):
        """
        :rtype: Engine
        """
        return self._ce(
            self._ccs(self.DialectAndDriver.mysql), **kwargs
        )

    def create_mysql_mysqldb(self, **kwargs):
        """
        :rtype: Engine
        """
        return self._ce(
            self._ccs(self.DialectAndDriver.mysql_mysqldb), **kwargs
        )

    def create_mysql_mysqlconnector(self, **kwargs):
        """
        :rtype: Engine
        """
        return self._ce(
            self._ccs(self.DialectAndDriver.mysql_mysqlconnector), **kwargs
        )

    def create_mysql_oursql(self, **kwargs):
        """
        :rtype: Engine
        """
        return self._ce(
            self._ccs(self.DialectAndDriver.mysql_oursql), **kwargs
        )

    def create_mysql_pymysql(self, **kwargs):
        """
        :rtype: Engine
        """
        return self._ce(
            self._ccs(self.DialectAndDriver.mysql_pymysql), **kwargs
        )

    def create_mysql_cymysql(self, **kwargs):
        """
        :rtype: Engine
        """
        return self._ce(
            self._ccs(self.DialectAndDriver.mysql_cymysql), **kwargs
        )

    # oracle
    def create_oracle(self, **kwargs):
        """
        :rtype: Engine
        """
        return self._ce(
            self._ccs(self.DialectAndDriver.oracle), **kwargs
        )

    def create_oracle_cx_oracle(self, **kwargs):
        """
        :rtype: Engine
        """
        return self._ce(
            self._ccs(self.DialectAndDriver.oracle_cx_oracle), **kwargs
        )

    # mssql
    def create_mssql_pyodbc(self, **kwargs):
        """
        :rtype: Engine
        """
        return self._ce(
            self._ccs(self.DialectAndDriver.mssql_pyodbc), **kwargs
        )

    def create_mssql_pymssql(self, **kwargs):
        """
        :rtype: Engine
        """
        return self._ce(
            self._ccs(self.DialectAndDriver.mssql_pymssql), **kwargs
        )

    # redshift
    def create_redshift(self, **kwargs):
        """
        :rtype: Engine
        """
        return self._ce(
            self._ccs(self.DialectAndDriver.redshift_psycopg2), **kwargs
        )


if __name__ == "__main__":
    import boto3
    from base64 import b64decode

    cred = Credential.from_s3_json(
        "sanhe-credential", "db/elephant-dupe-remove.json",
        aws_profile="sanhe",
    )
    print(cred)
