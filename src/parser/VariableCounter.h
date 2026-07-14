// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_PARSER_VARIABLECOUNTER_H
#define QLEVER_SRC_PARSER_VARIABLECOUNTER_H

#include <cstddef>
#include <variant>

#include "parser/GraphPattern.h"
#include "parser/GraphPatternOperation.h"
#include "parser/ParsedQuery.h"
#include "parser/SparqlTriple.h"

namespace qlever::parsedQuery {

// Visits the various types of graph patterns to extract how often a variable
// appears.
struct VariableCounter {
 public:
  using Map = ad_utility::HashMap<Variable, size_t>;

 private:
  Map counts_;

 public:
  const Map& counts() const { return counts_; }

  // Count variables from a view or range, e.g. `std::vector<GraphPattern>`.
  CPP_template(typename T)(
      requires ql::ranges::input_range<ql::remove_cvref_t<T>>) void
  operator()(T&& range) {
    for (const auto& elem : range) {
      (*this)(elem);
    }
  }

  // Unwrap a `std::optional<...>`.
  template <typename T>
  void operator()(const std::optional<T>& opt) {
    if (opt.has_value()) {
      (*this)(opt.value());
    }
  }

  // `GraphPatternOperation` is a `std::variant`, which we visit. If an overload
  // for one of the variant types were missing, the compiler would implicitly
  // convert it to `GraphPatternOperation` and we would have a stack overflow.
  // Therefore we prevent implicit conversion here.
  CPP_template(typename T)(
      requires std::is_same_v<T, GraphPatternOperation>) void
  operator()(const T& gpo) {
    gpo.visit(*this);
  }

  // Overloads for helper types.
  void operator()(const Variable& var);
  void operator()(const Variable* var);
  void operator()(const TripleComponent& tc);
  void operator()(const SparqlTriple& triple);

  // Overloads for the individual operations.
  void operator()(const GraphPattern& op);
  void operator()(const SparqlFilter& op);
  void operator()(const Subquery& op);
  void operator()(const Optional& op);
  void operator()(const Union& op);
  void operator()(const TransPath& op);
  void operator()(const Bind& op);
  void operator()(const BasicGraphPattern& op);
  void operator()(const Values& op);
  void operator()(const Service& op);
  void operator()(const PathQuery& op);
  void operator()(const SpatialQuery& op);
  void operator()(const TextSearchQuery& op);
  void operator()(const TextSearchConfig& op);
  void operator()(const Minus& op);
  void operator()(const GroupGraphPattern& op);
  void operator()(const Describe& op);
  void operator()(const Load& op);
  void operator()(const NamedCachedResult& op);
  void operator()(const MaterializedViewQuery& op);
  void operator()(const ExternalValuesQuery& op);
};

}  // namespace qlever::parsedQuery

#endif  // QLEVER_SRC_PARSER_VARIABLECOUNTER_H
