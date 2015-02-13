// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#ifndef SERVER_RELATION_H_
#define SERVER_RELATION_H_

#include <vector>
#include <sstream>
#include <string>
#include "./Globals.h"
#include "./List.h"

using std::vector;
using std::string;

namespace ad_semsearch {
//! Class representing postings from a full-text index.
class RelationEntry {
  public:

    RelationEntry() : _lhs(0), _rhs(0), _score(0) {
    }

    RelationEntry(Id lhs, Id rhs, Score score)
    : _lhs(lhs), _rhs(rhs), _score(score) {
    }

    string asString() {
      std::ostringstream os;
      os << "(" << getPureValue(_lhs) << " - " << getPureValue(_rhs) << " ["
          << static_cast<unsigned int>(_score) << "])";
      return os.str();
    }

    Id _lhs;
    Id _rhs;
    Score _score;
};

// List representing a relation as read from the ontology index
typedef List<RelationEntry>  Relation;
}
#endif  // SERVER_RELATION_H_
