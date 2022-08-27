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

import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.rules as rlz
from ibis.expr.operations import All, Any
from ibis.expr.signature import Argument as Arg


class StringContains(ops.ValueOp):
    arg = rlz.string
    substr = rlz.string
    start = Arg(rlz.integer, default=None)
    end = Arg(rlz.integer, default=None)
    output_type = rlz.shape_like(rlz.integer)


class StringSQLLike(ops.Value):
    arg = Arg(rlz.string)
    pattern = Arg(rlz.string)
    escape = Arg(str, default=None)
    output_type = rlz.shape_like('arg')


class IsNull(ops.UnaryOp):
    output_dtype = dt.boolean
    output_type = rlz.shape_like('arg')


class NotNull(ops.UnaryOp):

    output_type = rlz.shape_like('arg')


class NotAny(Any):
    def negate(self):
        return Any(self.arg)


class NotAll(All):
    def negate(self):
        return All(self.arg)
