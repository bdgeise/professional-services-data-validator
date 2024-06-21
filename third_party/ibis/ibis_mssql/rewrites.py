import ibis.expr.operations as ops
import ibis.expr.types as ir
from ibis.backends.base.sql import compiler as sql_compiler


def mssql_string_contains(expr):
    arg = expr.op().args[0]
    return arg


REWRITES = {
    **sql_compiler.ExprTranslator._rewrites,
    # ops.StringContains: mssql_string_contains,
}