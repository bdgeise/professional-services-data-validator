import os
from pathlib import Path
import sqlalchemy as sa
from typing import Any, Generator
import csv

import pytest

import ibis
import third_party
from ibis.backends.conftest import TEST_TABLES, init_database
from ibis.backends.tests.base import BackendTest, RoundHalfToEven

MSSQL_USER = os.environ.get(
    'IBIS_TEST_MSSQL_USER', os.environ.get('MSSQLUSER', 'SA')
)
MSSQL_PASS = os.environ.get(
    'IBIS_TEST_MSSQL_PASSWORD', os.environ.get('MSSQLPASSWORD', 'Password1')
)
MSSQL_HOST = os.environ.get(
    'IBIS_TEST_MSSQL_HOST', os.environ.get('MSSQLHOST', 'localhost')
)
MSSQL_PORT = os.environ.get(
    'IBIS_TEST_MSSQL_PORT', os.environ.get('MSSQLPORT', 1433)
)
MSSQL_DB = os.environ.get(
    'IBIS_TEST_MSSQL_DATABASE', os.environ.get('MSSQLDATABASE', 'ibis_testing')
)


class TestConf(BackendTest, RoundHalfToEven):

    @staticmethod
    def _load_data(
        data_directory: Path,
        script_directory: Path,
        user=MSSQL_USER,
        password=MSSQL_PASS,
        database=MSSQL_DB,
        port=MSSQL_PORT,
        host=MSSQL_HOST,
        **_: Any,
    ) -> None:
        """Load test data into a PostgreSQL backend instance.
                Parameters
                ----------
                data_dir
                    Location of test data
                script_dir
                    Location of scripts defining schemas
                """
        with open(script_directory / 'schema' / 'mssql.sql') as schema:
            engine = init_database(
                url=sa.engine.url.URL.create(
                    'mssql+pyodbc',
                    host=host,
                    port=port,
                    username=user,
                    password=password,
                    database="master",
                    query={'driver': 'ODBC Driver 17 for SQL Server'},
                ),
                database=database,
                schema=schema,
                isolation_level='AUTOCOMMIT',
            )

            engine.dispose()

        engine = sa.create_engine(url=sa.engine.url.URL.create(
                    'mssql+pyodbc',
                    host=host,
                    port=port,
                    username=user,
                    password=password,
                    database=database,
                    query={'driver': 'ODBC Driver 17 for SQL Server'},
                ))

        tables = list(TEST_TABLES)
        with engine.begin() as con, con.connection.cursor() as cur:
            for table in tables:
                with data_directory.joinpath(f'{table}.csv').open('r') as file:
                    csvreader = csv.reader(file)
                    headers = next(csvreader, None)
                    params = ','.join('?' for h in headers)
                    insert_to_tmp_tbl_stmt = f"INSERT INTO dbo.{table} VALUES ({params})"
                    count = 0
                    for line in csvreader:
                        cur.execute(insert_to_tmp_tbl_stmt, line)
                        count+=1
                        if count == 1000:
                            break
                    cur.commit()

    @staticmethod
    def connect(data_directory: Path):
        return third_party.ibis.ibis_mssql.connect(
            host=MSSQL_HOST,
            port=MSSQL_PORT,
            user=MSSQL_USER,
            password=MSSQL_PASS,
            database=MSSQL_DB,
        )


def _random_identifier(suffix):
    return '__ibis_test_{}_{}'.format(suffix, ibis.util.guid())


@pytest.fixture(scope='session')
def con(data_directory, script_directory):
    TestConf._load_data(
        data_directory,
        script_directory
    )
    return TestConf.connect(data_directory)


@pytest.fixture(scope='module')
def db(con):
    print(con.meta)
    return con.database()


@pytest.fixture(scope='module')
def alltypes(db):
    return db.functional_alltypes


@pytest.fixture(scope='module')
def df(alltypes):
    return alltypes.execute()


@pytest.fixture(scope='module')
def at(alltypes):
    return alltypes.op().sqla_table


@pytest.fixture(scope='module')
def batting(db):
    return db.batting


@pytest.fixture(scope='module')
def batting_df(batting):
    return batting.execute()


@pytest.fixture(scope='module')
def awards_players(db):
    return db.awards_players


@pytest.fixture(scope='module')
def awards_players_df(awards_players):
    return awards_players.execute()


@pytest.fixture
def translate():
    from third_party.ibis.ibis_mssql import Backend

    context = Backend.compiler.make_context()
    return lambda expr: (
        Backend.compiler.translator_class(expr, context).get_result()
    )


@pytest.fixture
def temp_table(con) -> str:
    name = _random_identifier('table')
    try:
        yield name
    finally:
        con.drop_table(name, force=True)
