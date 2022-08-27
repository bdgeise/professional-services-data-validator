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

import sqlalchemy.dialects.mssql as mssql
import ibis.expr.datatypes as dt
import ibis.backends.base.sql.alchemy as alch
from .registry import operation_registry
from .rewrites import REWRITES


class MSSQLExprTranslator(alch.AlchemyExprTranslator):
    _registry = operation_registry
    _rewrites = REWRITES
    _type_map = alch.AlchemyExprTranslator._type_map.copy()
    _type_map.update(
        {
            dt.Boolean: mssql.BIT,
            dt.Int8: mssql.TINYINT,
            dt.Int32: mssql.INTEGER,
            dt.Int64: mssql.BIGINT,
            dt.Float16: mssql.REAL,
            dt.Float32: mssql.REAL,
            dt.Float64: mssql.DECIMAL,
            dt.String: mssql.VARCHAR,
        }
    )

class MSSQLCompiler(alch.AlchemyCompiler):

    translator_class = MSSQLExprTranslator
