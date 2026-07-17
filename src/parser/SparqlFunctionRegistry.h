// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Artem <artem@rem.sh>

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_PARSER_SPARQLFUNCTIONREGISTRY_H
#define QLEVER_SRC_PARSER_SPARQLFUNCTIONREGISTRY_H

#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "util/HashMap.h"

namespace sparqlExpression {
class SparqlExpression;
}

namespace parsedQuery {

// A factory throws this on a bad call (wrong arity or argument kinds); the
// parser reports it as a query error. Any other exception propagates as an
// internal error.
class InvalidSparqlFunctionCall : public std::runtime_error {
  using std::runtime_error::runtime_error;
};

// Registry of custom SPARQL functions: a function-call IRI to a factory that
// builds its `SparqlExpression`. Functions come from drop-in extensions under
// `src/extensions` that self-register at load.
class SparqlFunctionRegistry {
 public:
  using ExpressionPtr = std::unique_ptr<sparqlExpression::SparqlExpression>;
  using ArgList = std::vector<ExpressionPtr>;
  using Factory = std::function<ExpressionPtr(ArgList)>;

  static SparqlFunctionRegistry& get();

  // `iri` is the function's well-formed bracketed IRI, e.g.
  // `<http://example.org/f>`.
  void addExact(std::string iri, Factory factory);

  std::optional<Factory> lookup(std::string_view iri) const;

 private:
  ad_utility::HashMap<std::string, Factory> entries_;
};

}  // namespace parsedQuery

#endif  // QLEVER_SRC_PARSER_SPARQLFUNCTIONREGISTRY_H
