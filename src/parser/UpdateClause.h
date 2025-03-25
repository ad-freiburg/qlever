//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#pragma once

#include "parser/Iri.h"
#include "parser/SelectClause.h"
#include "parser/SparqlTriple.h"
#include "parser/data/GraphRef.h"
#include "parser/data/Types.h"

namespace updateClause {
struct Load {
  bool silent_;
  ad_utility::triple_component::Iri source_;
  std::optional<GraphRef> target_;
};

struct Clear {
  bool silent_;
  GraphRefAll target_;
};

struct Drop {
  bool silent_;
  GraphRefAll target_;
};

struct Create {
  bool silent_;
  GraphRef target_;
};

struct Add {
  bool silent_;
  GraphOrDefault source_;
  GraphOrDefault target_;
};

struct Move {
  bool silent_;
  GraphOrDefault source_;
  GraphOrDefault target_;
};

struct Copy {
  bool silent_;
  GraphOrDefault source_;
  GraphOrDefault target_;
};

// A Graph Update is an Update operation that inserts or deletes some triples.
// These triples can contain variables that are bound the result of the
// ParsedQueries GraphPattern. This used for `INSERT DATA`, `DELETE DATA`,
// `DELETE WHERE {...}` and `DELETE/INSERT {..} WHERE {...}`.
struct GraphUpdate {
  std::vector<SparqlTripleSimpleWithGraph> toInsert_;
  std::vector<SparqlTripleSimpleWithGraph> toDelete_;
  std::optional<ad_utility::triple_component::Iri> with_;

  GraphUpdate() = default;
  GraphUpdate(std::vector<SparqlTripleSimpleWithGraph> toInsert,
              std::vector<SparqlTripleSimpleWithGraph> toDelete)
      : toInsert_{std::move(toInsert)}, toDelete_{std::move(toDelete)} {}
};

// All the available update operations.
using Operation =
    std::variant<GraphUpdate, Load, Clear, Drop, Create, Add, Move, Copy>;
}  // namespace updateClause

namespace parsedQuery {
struct UpdateClause : ClauseBase {
  updateClause::Operation op_;

  UpdateClause() = default;
  explicit UpdateClause(updateClause::Operation op) : op_{std::move(op)} {}
};
}  // namespace parsedQuery
