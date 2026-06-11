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

#include "parser/GraphPattern.h"
#include "parser/SparqlTriple.h"

namespace parsedQuery {

// Visits the various types of graph patterns to extract how often a variable
// appears.
struct VariableCounter {
  ad_utility::HashMap<Variable, size_t> count_;

  CPP_template(typename T)(
      requires ql::ranges::input_range<std::remove_cvref_t<T>>) void
  operator()(T&& rng);

  template <typename T>
  void operator()(const std::optional<T>& opt);

  void operator()(const Variable& var);
  void operator()(const TripleComponent& var);
  void operator()(const SparqlTriple& var);
  void operator()(const Variable* var);
  void operator()(const GraphPattern& gp);
  void operator()(const SparqlFilter& filter);
  void operator()(const GraphPatternOperation& gpo);
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

}  // namespace parsedQuery

#endif  // QLEVER_SRC_PARSER_VARIABLECOUNTER_H
