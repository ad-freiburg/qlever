//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_PARSER_UPDATECLAUSE_H
#define QLEVER_SRC_PARSER_UPDATECLAUSE_H

#include "parser/Iri.h"
#include "parser/SelectClause.h"
#include "parser/SparqlTriple.h"
#include "parser/data/GraphRef.h"
#include "parser/data/Types.h"

namespace updateClause {
// A Graph Update is an Update operation that inserts or deletes some triples.
// These triples can contain variables that are bound the result of the
// ParsedQueries GraphPattern. All Updates are realised with it.
struct GraphUpdate {
  std::vector<SparqlTripleSimpleWithGraph> toInsert_;
  std::vector<SparqlTripleSimpleWithGraph> toDelete_;

  GraphUpdate() = default;
  GraphUpdate(std::vector<SparqlTripleSimpleWithGraph> toInsert,
              std::vector<SparqlTripleSimpleWithGraph> toDelete)
      : toInsert_{std::move(toInsert)}, toDelete_{std::move(toDelete)} {}
};
}  // namespace updateClause

namespace parsedQuery {
struct UpdateClause : ClauseBase {
  updateClause::GraphUpdate op_;

  UpdateClause() = default;
  explicit UpdateClause(updateClause::GraphUpdate op) : op_{std::move(op)} {}
};
}  // namespace parsedQuery

#endif  // QLEVER_SRC_PARSER_UPDATECLAUSE_H
