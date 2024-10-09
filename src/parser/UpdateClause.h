//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#pragma once

#include "parser/Iri.h"
#include "parser/SelectClause.h"
#include "parser/SparqlTriple.h"
#include "parser/data/GraphRef.h"
#include "parser/data/Types.h"

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

struct GraphUpdate {
  std::vector<SparqlTripleSimple> toInsert_;
  std::vector<SparqlTripleSimple> toDelete_;
  std::optional<ad_utility::triple_component::Iri> with_;

  GraphUpdate() = default;
  GraphUpdate(std::vector<SparqlTripleSimple> toInsert,
              std::vector<SparqlTripleSimple> toDelete)
      : toInsert_{std::move(toInsert)}, toDelete_{std::move(toDelete)} {}
};

namespace parsedQuery {
struct UpdateClause : ClauseBase {
  std::variant<GraphUpdate, Load, Clear, Drop, Create, Add, Move, Copy> op_;

  UpdateClause() = default;
  explicit UpdateClause(
      std::variant<GraphUpdate, Load, Clear, Drop, Create, Add, Move, Copy> op)
      : op_{std::move(op)} {}
};
}  // namespace parsedQuery
