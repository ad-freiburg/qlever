// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#include "index/TextIndexLiteralFilter.h"

#include <re2/re2.h>

#include "ctre-unicode.hpp"

// _____________________________________________________________________________
std::tuple<bool, bool, bool> TextIndexLiteralFilter::computeInTextIndexMap(
    const TripleComponent& s, const TripleComponent& p,
    const TripleComponent& o) {
  // If add all Literals check if they are a literal
  if (addAllLiterals_) {
    return {s.isLiteral(), p.isLiteral(), o.isLiteral()};
  }
  if (!o.isLiteral() || !p.isIri()) {
    return {false, false, false};
  }
  return {false, false,
          (filterType_ == FilterType::AcceptMatching) ==
              RE2::PartialMatch(p.getIri().toStringRepresentation(), *regex_)};
}
