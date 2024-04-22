/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Field;
import org.apache.iotdb.db.relational.sql.tree.Expression;
import org.apache.iotdb.db.relational.sql.tree.FunctionCall;
import org.apache.iotdb.db.relational.sql.tree.SymbolReference;

import org.apache.tsfile.read.common.type.Type;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class SymbolAllocator {
  private final Map<Symbol, Type> symbols;
  private int nextId;

  public SymbolAllocator() {
    symbols = new HashMap<>();
  }

  public SymbolAllocator(Map<Symbol, Type> initial) {
    symbols = new HashMap<>(initial);
  }

  public Symbol newSymbol(Symbol symbolHint) {
    return newSymbol(symbolHint, null);
  }

  public Symbol newSymbol(Symbol symbolHint, String suffix) {
    checkArgument(symbols.containsKey(symbolHint), "symbolHint not in symbols map");
    return newSymbol(symbolHint.getName(), symbols.get(symbolHint), suffix);
  }

  public Symbol newSymbol(String nameHint, Type type) {
    return newSymbol(nameHint, type, null);
  }

  public Symbol newSymbol(String nameHint, Type type, String suffix) {
    requireNonNull(nameHint, "nameHint is null");
    requireNonNull(type, "type is null");

    // TODO: workaround for the fact that QualifiedName lowercases parts
    nameHint = nameHint.toLowerCase(ENGLISH);

    // don't strip the tail if the only _ is the first character
    //    int index = nameHint.lastIndexOf("_");
    //    if (index > 0) {
    //      String tail = nameHint.substring(index + 1);
    //
    //      // only strip if tail is numeric or _ is the last character
    //      if (Ints.tryParse(tail) != null || index == nameHint.length() - 1) {
    //        nameHint = nameHint.substring(0, index);
    //      }
    //    }

    String unique = nameHint;

    if (suffix != null) {
      unique = unique + "$" + suffix;
    }

    Symbol symbol = new Symbol(unique);
    while (symbols.putIfAbsent(symbol, type) != null) {
      symbol = new Symbol(unique + "_" + nextId());
    }

    return symbol;
  }

  public Symbol newSymbol(Expression expression, Type type) {
    return newSymbol(expression, type, null);
  }

  public Symbol newSymbol(Expression expression, Type type, String suffix) {
    String nameHint = "expr";
    if (expression instanceof FunctionCall) {
      FunctionCall functionCall = (FunctionCall) expression;
      // symbol allocation can happen during planning, before function calls are rewritten
      nameHint = functionCall.getName().toString();
    } else if (expression instanceof SymbolReference) {
      SymbolReference symbolReference = (SymbolReference) expression;
      nameHint = symbolReference.getName();
    }

    return newSymbol(nameHint, type, suffix);
  }

  public Symbol newSymbol(Field field) {
    String nameHint = field.getName().orElse("field");
    return newSymbol(nameHint, field.getType());
  }

  public TypeProvider getTypes() {
    return TypeProvider.viewOf(symbols);
  }

  private int nextId() {
    return nextId++;
  }
}