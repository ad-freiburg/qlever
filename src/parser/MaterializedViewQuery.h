// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_PARSER_MATERIALIZEDVIEWQUERY_H
#define QLEVER_SRC_PARSER_MATERIALIZEDVIEWQUERY_H

#include "parser/MagicServiceQuery.h"

// A custom exception that will be thrown for all configuration errors while
// reading or writing materialized views.
class MaterializedViewConfigException : public std::runtime_error {
  // Constructors have to be explicitly inherited
  using std::runtime_error::runtime_error;
};

namespace parsedQuery {

// A `MagicServiceQuery` for performing a custom `IndexScan` on a
// `MaterializedView`. Using the `SERVICE` version of this query, the user may
// select arbitrary payload columns to be read. With the magic predicate
// version, also supported by this class, only one payload column may be
// read.
struct MaterializedViewQuery : MagicServiceQuery {
 public:
  // The name of the view to be queried.
  std::optional<std::string> viewName_;

  // The scan column can either be a variable (for a full scan on the view -
  // this column will then get the values from the first column) or a
  // literal/IRI (for reading only those entries from the view where the first
  // column matches the given literal/IRI)
  std::optional<TripleComponent> scanCol_;

  // The requested variables are a mapping of column names in the view to target
  // column names in the query result. This can be used for reading any number
  // of payload columns from the materialized view.
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
