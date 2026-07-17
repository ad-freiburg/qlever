// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Artem <artem@rem.sh>

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Example custom SPARQL function extension (`increment`/`plus`). A template for
// your own: copy this folder and change the IRIs and factories.

#include <memory>

#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "global/Id.h"
#include "parser/SparqlFunctionRegistry.h"

namespace {
using Expr = parsedQuery::SparqlFunctionRegistry::ExpressionPtr;
using Args = parsedQuery::SparqlFunctionRegistry::ArgList;

// increment(x) == x + 1
Expr makeIncrement(Args args) {
  if (args.size() != 1) {
    throw parsedQuery::InvalidSparqlFunctionCall{
        "increment() takes exactly one argument"};
  }
  return sparqlExpression::makeAddExpression(
      std::move(args.at(0)),
      std::make_unique<sparqlExpression::IdExpression>(Id::makeFromInt(1)));
}

// plus(x, y) == x + y
Expr makePlus(Args args) {
  if (args.size() != 2) {
    throw parsedQuery::InvalidSparqlFunctionCall{
        "plus() takes exactly two arguments"};
  }
  return sparqlExpression::makeAddExpression(std::move(args.at(0)),
                                             std::move(args.at(1)));
}

// Self-register at load.
[[maybe_unused]] const bool registered = [] {
  auto& registry = parsedQuery::SparqlFunctionRegistry::get();
  registry.addExact("<http://qlever.cs.uni-freiburg.de/example/increment>",
                    &makeIncrement);
  registry.addExact("<http://qlever.cs.uni-freiburg.de/example/plus>",
                    &makePlus);
  return true;
}();
}  // namespace
