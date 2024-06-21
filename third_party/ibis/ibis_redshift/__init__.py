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
from ibis import util
import logging

import sqlalchemy as sa

from typing import Literal
from ibis.backends.base.sql.alchemy import BaseAlchemyBackend
from ibis.backends.base import BaseBackend
import ibis.expr.schema as sch
from ibis.backends.postgres.compiler import PostgreSQLCompiler
from ibis.backends.postgres.datatypes import _get_type
import sqlalchemy_redshift

class Backend(BaseAlchemyBackend):
    name = "redshift"
    compiler = PostgreSQLCompiler
    database_name = None

    def do_connect(
        self,
        host='localhost',
        user=None,
        password=None,
        port=5432,
        database=None,
        url=None,
        driver: Literal["psycopg2"] = "psycopg2",
    ) -> None:
        """Create a :class:`Backend` for use with Ibis.
        """
        if driver != 'psycopg2':
            raise NotImplementedError(
                'psycopg2 is currently the only supported driver'
            )
        alchemy_url = self._build_alchemy_url(
            url=url,
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            driver=f'postgresql+{driver}',
        )
        self.database_name = alchemy_url.database
        super().do_connect(sa.create_engine(alchemy_url))

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

    def _get_schema_using_query(self, query: str) -> sch.Schema:
        raw_name = util.guid()
        name = self.con.dialect.identifier_preparer.quote_identifier(raw_name)
        type_info_sql = f"""\
SELECT
  attname,
  format_type(atttypid, atttypmod) AS type
FROM pg_attribute
WHERE attrelid = {raw_name!r}::regclass
  AND attnum > 0
  AND NOT attisdropped
ORDER BY attnum
"""
        with self.con.connect() as con:
            con.execute(f"CREATE VIEW {name} AS {query}")
            try:
                type_info = con.execute(type_info_sql).fetchall()
            finally:
                con.execute(f"DROP VIEW {name}")
        tuples = [(col, _get_type(typestr)) for col, typestr in type_info]
        return sch.Schema.from_tuples(tuples)

    def _get_temp_view_definition(
            self,
            name: str,
            definition: sa.sql.compiler.Compiled,
    ) -> str:
        return f"CREATE OR REPLACE TEMPORARY VIEW {name} AS {definition}"


def compile(expr, params=None, **kwargs):
    """Compile an expression for Redshift.
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
        port=5432,
        database=None,
        url=None,
        driver: Literal["psycopg2"] = "psycopg2",
) -> BaseBackend:
    """Create a :class:`Backend` for use with Ibis.
    Parameters
    ----------
    host : string, default 'localhost'
    user : string, optional
    password : string, optional
    port : string or integer, default 5432
    database : string
    url : string, optional
        Complete SQLAlchemy connection string. If passed, the other connection
        arguments are ignored.
    driver : string, default 'psycopg2'

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
    )
    return backend

