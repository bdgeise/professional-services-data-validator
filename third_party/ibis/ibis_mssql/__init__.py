# Copyright 2020 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import contextlib
import datetime
import getpass
import logging

import sqlalchemy as sa

import sqlalchemy.dialects.mssql as mssql

import ibis.expr.datatypes as dt
from ibis.backends.base.sql.alchemy import BaseAlchemyBackend
from ibis.backends.base import BaseBackend
import ibis.expr.schema as sch
from third_party.ibis.ibis_mssql.compiler import MSSQLCompiler

import pyodbc  # NOQA fail early if the driver is missing

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s-%(levelname)s: %(message)s",
    datefmt="%m/%d/%Y %I:%M:%S %p",
)


class Backend(BaseAlchemyBackend):
    name = "mssql"
    compiler = MSSQLCompiler
    database_name = None

    def do_connect(
        self,
        host='localhost',
        user=None,
        password=None,
        port=1433,
        database='master',
        url=None,
        driver='pyodbc',
        odbc_driver='ODBC Driver 17 for SQL Server',
    ) -> None:
        """Create a :class:`Backend` for use with Ibis.
        """
        if url is None:
            if driver != 'pyodbc':
                raise NotImplementedError(
                    'pyodbc is currently the only supported driver'
                )
            user = user or getpass.getuser()
            url = sa.engine.url.URL.create(
                'mssql+pyodbc',
                host=host,
                port=port,
                username=user,
                password=password,
                database=database,
                query={'driver': odbc_driver},
            )
        else:
            url = sa.engine.url.make_url(url)
        self.database_name = url.database
        super().do_connect(sa.create_engine(url))

    @contextlib.contextmanager
    def begin(self):
        """Start transaction with client to database."""
        with super().begin() as bind:
            # set timezone utc
            yield bind
            # set timezone previous timezone

    def list_databases(self, like=None):
        """List all databases for client to connect to."""
        return [
            row.name
            for row in self.con.execute(
                'SELECT name FROM master.dbo.sysdatabases'
            )
        ]

    def list_schemas(self, like=None):
        """List all the schemas in the current database."""
        return self.inspector.get_schema_names()

    def set_database(self, name):
        """Set current database that client is connected to."""
        raise NotImplementedError(
            'Cannot set database with MSSQL client. To use a different'
            ' database, use client.database({!r})'.format(name)
        )

    def _get_schema_using_query(self, limited_query):
        type_map = {
            int: 'int64',
            bool: 'boolean',
            float: 'float64',
            str: 'string',
            datetime.datetime: 'timestamp',
        }

        logging.debug(f"Limited query {limited_query}")

        with self.execute(limited_query, results=True) as cur:
            type_info = cur.fetchall()

        tuples = [(col, type_map[typestr]) for col, typestr in type_info]
        return sch.Schema.from_tuples(tuples)


def compile(expr, params=None, **kwargs):
    """Compile an expression for MSSQL.
    Returns
    -------
    compiled : str
    See Also
    --------
    ibis.expr.types.Expr.compile
    """
    backend = Backend()
    return backend.compile(expr, params=params, **kwargs)


def connect(
    host='localhost',
    user=None,
    password=None,
    port=1433,
    database='master',
    driver='pyodbc',
    odbc_driver='ODBC Driver 17 for SQL Server',
    url=None,
) -> BaseBackend:
    """Create a :class:`Backend` for use with Ibis.
    Parameters
    ----------
    host : string, default 'localhost'
    user : string, optional
    password : string, optional
    port : string or integer, default 1433
    database : string, default 'master'
    url : string, optional
        Complete SQLAlchemy connection string. If passed, the other connection
        arguments are ignored.
    driver : string, default 'pyodbc'
    odbc_driver : string, default 'ODBC Driver 17 for SQL Server'

    Returns
    -------
    Backend
    """
    backend = Backend()
    backend.do_connect(
        host=host,
        user=user,
        password=password,
        port=port,
        database=database,
        url=url,
        driver=driver,
        odbc_driver=odbc_driver,
    )
    return backend


@dt.dtype.register(mssql.UNIQUEIDENTIFIER)
def sa_string(satype, nullable=True):
    return dt.String(nullable=nullable)


@dt.dtype.register(mssql.BIT)
def sa_boolean(satype, nullable=True):
    return dt.Boolean(nullable=nullable)
