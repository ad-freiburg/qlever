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
//
// Example using magic predicate:
// SELECT * { osmway:110404213 view:geometries-asWKT ?wkt }
//
// Example using magic `SERVICE`:
// SELECT * {
//    SERVICE view:geometries {
//      [ view:column-osmid osmway:110404213 ; view:column-asWKT ?wkt ]
//    }
// }
struct MaterializedViewQuery : MagicServiceQuery {
 public:
  // The name of the view to be queried.
  std::optional<std::string> viewName_;

  // The variable or literal or IRI for the scan column. This is only used when
  // a `MaterializedViewQuery` is built from a special triple (because we do not
  // know the name of the scan column).
  std::optional<TripleComponent> scanCol_;

  // The requested variables are a mapping of column names in the view to target
  // column names in the query result or literals/IRIs to restrict the column
  // on. This can be used for filtering the results and reading any number of
  // payload columns from the materialized view.
  using RequestedColumns = ad_utility::HashMap<Variable, TripleComponent>;
  RequestedColumns requestedColumns_;

  // This constructor takes an IRI consisting of the magic service IRI for
  // materialized views with the view name as a suffix. If this is used, add the
  // requested columns one-by-one using `addParameter`.
  explicit MaterializedViewQuery(const ad_utility::triple_component::Iri& iri);

  // Alternative: Initialize using magic predicate. No calls to `addParameter`
  // are necessary in this case.
  explicit MaterializedViewQuery(const SparqlTriple& triple);

  // For query rewriting: Initialize directly using name and requested columns.
  MaterializedViewQuery(std::string name, RequestedColumns requestedColumns);

  void addParameter(const SparqlTriple& triple) override;

  // Return the variables that should be visible from this read on the
  // materialized view. Used for column stripping.
  ad_utility::HashSet<Variable> getVarsToKeep() const;

  constexpr std::string_view name() const override {
    return "materialized view query";
  };

 private:
  // Internal helpers for shared code between `addParameter` and magic predicate
  // constructor
  void setScanCol(const TripleComponent& object);
  void addRequestedColumn(const Variable& column,
                          const TripleComponent& object);
};

}  // namespace parsedQuery

#endif  // QLEVER_SRC_PARSER_MATERIALIZEDVIEWQUERY_H
