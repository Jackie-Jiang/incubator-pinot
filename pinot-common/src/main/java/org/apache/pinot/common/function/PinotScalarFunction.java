/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.common.function;

import javax.annotation.Nullable;
import org.apache.pinot.common.function.sql.PinotSqlFunction;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.annotations.ScalarFunction;


/**
 * Provides finer control to the scalar functions annotated with {@link ScalarFunction}.
 */
public interface PinotScalarFunction {

  /**
   * Returns the name of the function.
   */
  String getName();

  /**
   * Returns the corresponding {@link PinotSqlFunction} to be registered into the OperatorTable, or {@link null} if
   * it doesn't need to be registered (e.g. standard SqlFunction).
   */
  @Nullable
  PinotSqlFunction toPinotSqlFunction();

  /**
   * Returns the {@link FunctionInfo} for the given argument types, or {@link null} if there is no matching.
   */
  @Nullable
  default FunctionInfo getFunctionInfo(ColumnDataType[] argumentTypes) {
    return getFunctionInfo(argumentTypes.length);
  }

  /**
   * Returns the {@link FunctionInfo} for the given number of arguments, or {@link null} if there is no matching.
   */
  @Nullable
  FunctionInfo getFunctionInfo(int numArguments);
}
