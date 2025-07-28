// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#include "TripleInTextIndexFilter.h"

#include <re2/re2.h>

// _____________________________________________________________________________
bool TripleInTextIndexFilter::operator()(const TripleComponent& p,
                                         const TripleComponent& o) const {
  if (!o.isLiteral() || !p.isIri()) {
    return false;
  }
  auto matcher = RE2{regex_, RE2::Quiet};
  return (isWhitelist_ ==
          RE2::PartialMatch(p.getIri().toStringRepresentation(), matcher));
}
