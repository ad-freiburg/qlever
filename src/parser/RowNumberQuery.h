// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_PARSER_ROWNUMBERQUERY_H
#define QLEVER_SRC_PARSER_ROWNUMBERQUERY_H

#include "parser/MagicServiceQuery.h"

namespace parsedQuery {

// The configuration of the row number magic SERVICE, which adds a column with
// the row numbers to the result of an explicitly sorted subquery:
// SELECT * {
//   SERVICE <https://qlever.cs.uni-freiburg.de/rowNumber/> {
//     _:config <bind> ?rowNumber ;
//              <offset> 1 .
//     { SELECT ?x ?y { ?x <p> ?y } ORDER BY ?x ?y }
//   }
// }
// The `<offset>` is optional and defaults to zero.
//
// NOTE 1: Joining the result with itself on `?rowNumber` and another variable,
// using different offsets, is the equivalent of the SQL window functions
// `LEAD` and `LAG`.
//
// NOTE 2: A simple SPARQL function to be used with `BIND` is unsuitable for
// this, because the value of a `BIND` expression may only depend on the
// bindings of its own row and not on the position of that row in the result.
struct RowNumberQuery : MagicServiceQuery {
  std::optional<Variable> rowNumberVariable_;
  int64_t offset_ = 0;

  // See `MagicServiceQuery`, processes the configuration triples.
  void addParameter(const SparqlTriple& triple) override;

  // Throw if the `<bind>` parameter is missing, if the child graph pattern is
  // not a single subquery with an `ORDER BY` or `INTERNAL SORT BY` clause, or
  // if the subquery selects the `<bind>` variable.
  void validate() const override;

  std::string_view name() const override { return "row number"; }
};

}  // namespace parsedQuery

#endif  // QLEVER_SRC_PARSER_ROWNUMBERQUERY_H
