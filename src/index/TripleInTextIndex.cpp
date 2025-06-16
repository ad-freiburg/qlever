// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#include "TripleInTextIndex.h"

#include <re2/re2.h>

// _____________________________________________________________________________
bool TripleInTextIndex::operator()(const TurtleTriple& triple) const {
  if (!(triple.object_.isIri() || triple.object_.isLiteral())) {
    return false;
  }
  return (
      isWhitelist_ ==
      RE2::PartialMatch(triple.predicate_.toStringRepresentation(), regex_));
}
