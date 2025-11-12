// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_PARSER_MATERIALIZEDVIEWQUERY_H
#define QLEVER_SRC_PARSER_MATERIALIZEDVIEWQUERY_H

#include "parser/MagicServiceQuery.h"

namespace parsedQuery {

// A `MagicServiceQuery` for performing a custom `IndexScan` on a
// `MaterializedView`. Using the `SERVICE` version of this query, the user may
// select arbitrary payload columns to be read. With the magic predicate
// version, also supported by this class, only one payload column may be
// read.
struct MaterializedViewQuery : MagicServiceQuery {
 public:
  std::optional<std::string> viewName_;
  std::optional<TripleComponent> scanCol_;
  ad_utility::HashMap<Variable, Variable> requestedVariables_;

  // Default constructor. If this is used, add configuration triples one-by-one
  // using `addParameter`.
  MaterializedViewQuery() = default;

  // Alternative: Initialize using magic predicate. No calls to `addParameter`
  // are necessary in this case.
  explicit MaterializedViewQuery(const SparqlTriple& triple);

  void addParameter(const SparqlTriple& triple) override;

  // Return the variables that should be visible from this read on the
  // materialized view. Used for column stripping.
  ad_utility::HashSet<Variable> getVarsToKeep() const;

 private:
  // Internal helpers for shared code between `addParameter` and magic predicate
  // constructor
  void setViewName(const TripleComponent& object);
  void setScanCol(const TripleComponent& object);
  void addPayloadVariable(const Variable& column,
                          const TripleComponent& object);
};

}  // namespace parsedQuery

#endif  // QLEVER_SRC_PARSER_MATERIALIZEDVIEWQUERY_H
