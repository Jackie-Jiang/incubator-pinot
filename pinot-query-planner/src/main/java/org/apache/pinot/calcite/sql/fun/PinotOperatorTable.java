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
package org.apache.pinot.calcite.sql.fun;

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.function.PinotScalarFunction;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.function.sql.PinotSqlAggFunction;
import org.apache.pinot.common.function.sql.PinotSqlFunction;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.checkerframework.checker.nullness.qual.Nullable;


/**
 * {@link PinotOperatorTable} defines the {@link SqlOperator} overrides on top of the {@link SqlStdOperatorTable}.
 *
 * <p>The main purpose of this Pinot specific SQL operator table is to
 * <ul>
 *   <li>Ensure that any specific SQL validation rules can apply with Pinot override entirely over Calcite's.</li>
 *   <li>Ability to create customer operators that are not function and cannot use
 *     {@link org.apache.calcite.prepare.Prepare.CatalogReader} to override</li>
 *   <li>Still maintain minimum customization and benefit from Calcite's original operator table setting.</li>
 * </ul>
 */
@SuppressWarnings("unused") // unused fields are accessed by reflection
public class PinotOperatorTable implements SqlOperatorTable {
  private static final Supplier<PinotOperatorTable> INSTANCE = Suppliers.memoize(PinotOperatorTable::new);

  public static PinotOperatorTable instance() {
    return INSTANCE.get();
  }

  /**
   * This list includes the supported standard {@link SqlOperator}s defined in {@link SqlStdOperatorTable}.
   * The operator order follows the same order as defined in {@link SqlStdOperatorTable} for easier search.
   * Add more operators as needed.
   */
  //@formatter:off
  private static final List<SqlOperator> STANDARD_OPERATORS = List.of(
      // SET OPERATORS
      SqlStdOperatorTable.UNION,
      SqlStdOperatorTable.UNION_ALL,
      SqlStdOperatorTable.EXCEPT,
      SqlStdOperatorTable.EXCEPT_ALL,
      SqlStdOperatorTable.INTERSECT,
      SqlStdOperatorTable.INTERSECT_ALL,

      // BINARY OPERATORS
      SqlStdOperatorTable.AND,
      SqlStdOperatorTable.AS,
      SqlStdOperatorTable.FILTER,
      SqlStdOperatorTable.WITHIN_GROUP,
      SqlStdOperatorTable.WITHIN_DISTINCT,
      SqlStdOperatorTable.CONCAT,
      SqlStdOperatorTable.DIVIDE,
      SqlStdOperatorTable.PERCENT_REMAINDER,
      SqlStdOperatorTable.DOT,
      SqlStdOperatorTable.EQUALS,
      SqlStdOperatorTable.GREATER_THAN,
      SqlStdOperatorTable.IS_DISTINCT_FROM,
      SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
      SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
      SqlStdOperatorTable.IN,
      SqlStdOperatorTable.NOT_IN,
      SqlStdOperatorTable.SEARCH,
      SqlStdOperatorTable.LESS_THAN,
      SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
      SqlStdOperatorTable.MINUS,
      SqlStdOperatorTable.MULTIPLY,
      SqlStdOperatorTable.NOT_EQUALS,
      SqlStdOperatorTable.OR,
      SqlStdOperatorTable.PLUS,
      SqlStdOperatorTable.INTERVAL,

      // POSTFIX OPERATORS
      SqlStdOperatorTable.DESC,
      SqlStdOperatorTable.NULLS_FIRST,
      SqlStdOperatorTable.NULLS_LAST,
      SqlStdOperatorTable.IS_NOT_NULL,
      SqlStdOperatorTable.IS_NULL,
      SqlStdOperatorTable.IS_NOT_TRUE,
      SqlStdOperatorTable.IS_TRUE,
      SqlStdOperatorTable.IS_NOT_FALSE,
      SqlStdOperatorTable.IS_FALSE,
      SqlStdOperatorTable.IS_NOT_UNKNOWN,
      SqlStdOperatorTable.IS_UNKNOWN,

      // PREFIX OPERATORS
      SqlStdOperatorTable.EXISTS,
      SqlStdOperatorTable.NOT,

      // AGGREGATE OPERATORS
      SqlStdOperatorTable.SUM,
      SqlStdOperatorTable.COUNT,
      SqlStdOperatorTable.MODE,
      SqlStdOperatorTable.MIN,
      SqlStdOperatorTable.MAX,
      SqlStdOperatorTable.LAST_VALUE,
      SqlStdOperatorTable.FIRST_VALUE,
      SqlStdOperatorTable.LEAD,
      SqlStdOperatorTable.LAG,
      SqlStdOperatorTable.AVG,
      SqlStdOperatorTable.STDDEV_POP,
      SqlStdOperatorTable.COVAR_POP,
      SqlStdOperatorTable.COVAR_SAMP,
      SqlStdOperatorTable.STDDEV_SAMP,
      SqlStdOperatorTable.VAR_POP,
      SqlStdOperatorTable.VAR_SAMP,
      SqlStdOperatorTable.SUM0,

      // WINDOW Rank Functions
      SqlStdOperatorTable.DENSE_RANK,
      SqlStdOperatorTable.RANK,
      SqlStdOperatorTable.ROW_NUMBER,

      // SPECIAL OPERATORS
      SqlStdOperatorTable.BETWEEN,
      SqlStdOperatorTable.SYMMETRIC_BETWEEN,
      SqlStdOperatorTable.NOT_BETWEEN,
      SqlStdOperatorTable.SYMMETRIC_NOT_BETWEEN,
      SqlStdOperatorTable.NOT_LIKE,
      SqlStdOperatorTable.LIKE,
      SqlStdOperatorTable.CASE,
      SqlStdOperatorTable.OVER,
      SqlStdOperatorTable.CAST,
      SqlStdOperatorTable.EXTRACT,
      SqlStdOperatorTable.ITEM,
      SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
      SqlStdOperatorTable.LISTAGG
  );

  private static final List<Pair<SqlOperator, List<String>>> STANDARD_OPERATORS_WITH_ALIASES = List.of(
  );
  //@formatter:on

  /**
   * This list includes the customized {@link SqlOperator}s.
   */
  //@formatter:off
  private static final List<SqlOperator> PINOT_OPERATORS = List.of(
      // Placeholder for special predicates
      new PinotSqlFunction("TEXT_MATCH", ReturnTypes.BOOLEAN, OperandTypes.CHARACTER_CHARACTER),
      new PinotSqlFunction("TEXT_CONTAINS", ReturnTypes.BOOLEAN, OperandTypes.CHARACTER_CHARACTER),
      new PinotSqlFunction("JSON_MATCH", ReturnTypes.BOOLEAN, OperandTypes.CHARACTER_CHARACTER),
      new PinotSqlFunction("VECTOR_SIMILARITY", ReturnTypes.BOOLEAN_NOT_NULL,
          OperandTypes.family(List.of(SqlTypeFamily.ARRAY, SqlTypeFamily.ARRAY, SqlTypeFamily.INTEGER),
              ordinal -> ordinal == 2)),

      // SqlStdOperatorTable.COALESCE without rewrite
      new SqlFunction("COALESCE", SqlKind.COALESCE,
          ReturnTypes.LEAST_RESTRICTIVE.andThen(SqlTypeTransforms.LEAST_NULLABLE), null, OperandTypes.SAME_VARIADIC,
          SqlFunctionCategory.SYSTEM)
  );

  private static final List<Pair<SqlOperator, List<String>>> PINOT_OPERATORS_WITH_ALIASES = List.of(

  );
  //@formatter:on

  // Key is canonical name
  private final Map<String, SqlOperator> _operatorMap;
  private final List<SqlOperator> _operatorList;

  private PinotOperatorTable() {
    Map<String, SqlOperator> operatorMap = new HashMap<>();

    // Register standard operators
    for (SqlOperator operator : STANDARD_OPERATORS) {
      register(operator.getName(), operator, operatorMap);
    }
    for (Pair<SqlOperator, List<String>> pair : STANDARD_OPERATORS_WITH_ALIASES) {
      SqlOperator operator = pair.getLeft();
      for (String name : pair.getRight()) {
        register(name, operator, operatorMap);
      }
    }

    // Register Pinot operators
    for (SqlOperator operator : PINOT_OPERATORS) {
      register(operator.getName(), operator, operatorMap);
    }
    for (Pair<SqlOperator, List<String>> pair : PINOT_OPERATORS_WITH_ALIASES) {
      SqlOperator operator = pair.getLeft();
      for (String name : pair.getRight()) {
        register(name, operator, operatorMap);
      }
    }

    registerAggregateFunctions(operatorMap);
    registerTransformFunctions(operatorMap);
    registerScalarFunctions(operatorMap);

    _operatorMap = Map.copyOf(operatorMap);
    _operatorList = List.copyOf(operatorMap.values());
  }

  private void register(String name, SqlOperator sqlOperator, Map<String, SqlOperator> operatorMap) {
    Preconditions.checkState(operatorMap.put(FunctionRegistry.canonicalize(name), sqlOperator) == null,
        "SqlOperator: %s is already registered", name);
  }

  private void registerAggregateFunctions(Map<String, SqlOperator> operatorMap) {
    for (AggregationFunctionType functionType : AggregationFunctionType.values()) {
      if (functionType.getReturnTypeInference() != null) {
        String functionName = functionType.getName();
        PinotSqlAggFunction function = new PinotSqlAggFunction(functionName, functionType.getReturnTypeInference(),
            functionType.getOperandTypeChecker());
        Preconditions.checkState(operatorMap.put(FunctionRegistry.canonicalize(functionName), function) == null,
            "Aggregate function: %s is already registered", functionName);
      }
    }
  }

  private void registerTransformFunctions(Map<String, SqlOperator> operatorMap) {
    for (TransformFunctionType functionType : TransformFunctionType.values()) {
      if (functionType.getReturnTypeInference() != null) {
        PinotSqlFunction function = new PinotSqlFunction(functionType.getName(), functionType.getReturnTypeInference(),
            functionType.getOperandTypeChecker());
        for (String name : functionType.getAlternativeNames()) {
          Preconditions.checkState(operatorMap.put(FunctionRegistry.canonicalize(name), function) == null,
              "Transform function: %s is already registered", name);
        }
      }
    }
  }

  private void registerScalarFunctions(Map<String, SqlOperator> operatorMap) {
    for (Map.Entry<String, PinotScalarFunction> entry : FunctionRegistry.FUNCTION_MAP.entrySet()) {
      String canonicalName = entry.getKey();
      PinotScalarFunction scalarFunction = entry.getValue();
      PinotSqlFunction sqlFunction = scalarFunction.toPinotSqlFunction();
      if (sqlFunction == null) {
        continue;
      }
      if (operatorMap.containsKey(canonicalName)) {
        // Skip registering ArgumentCountBasedScalarFunction if it is already registered
        Preconditions.checkState(scalarFunction instanceof FunctionRegistry.ArgumentCountBasedScalarFunction,
            "Scalar function: %s is already registered", canonicalName);
        continue;
      }
      operatorMap.put(canonicalName, sqlFunction);
    }
  }

  @Override
  public void lookupOperatorOverloads(SqlIdentifier opName, @Nullable SqlFunctionCategory category, SqlSyntax syntax,
      List<SqlOperator> operatorList, SqlNameMatcher nameMatcher) {
    if (!opName.isSimple()) {
      return;
    }
    String canonicalName = FunctionRegistry.canonicalize(opName.getSimple());
    SqlOperator operator = _operatorMap.get(canonicalName);
    if (operator != null) {
      operatorList.add(operator);
    }
  }

  @Override
  public List<SqlOperator> getOperatorList() {
    return _operatorList;
  }
}
