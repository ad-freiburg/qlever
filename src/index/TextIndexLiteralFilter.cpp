// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#include "index/TextIndexLiteralFilter.h"

#include <re2/re2.h>

#include "ctre-unicode.hpp"

// _____________________________________________________________________________
void TextIndexLiteralFilter::computeInTextIndexMap(const TripleComponent& s,
                                                   const TripleComponent& p,
                                                   const TripleComponent& o) {
  // Reset map
  inTextIndexMap_ = {false, false, false};
  // If add all Literals check if they are a literal
  if (addAllLiterals_) {
    std::get<0>(inTextIndexMap_) = s.isLiteral();
    std::get<1>(inTextIndexMap_) = p.isLiteral();
    std::get<2>(inTextIndexMap_) = o.isLiteral();
    return;
  }
  if (!o.isLiteral() || !p.isIri()) {
    // Nothing has to be done since map is set to false anyways
    return;
  }
  std::get<2>(inTextIndexMap_) =
      isWhitelist_ ==
      RE2::PartialMatch(p.getIri().toStringRepresentation(), *regex_);
}

// _____________________________________________________________________________
bool TextIndexLiteralFilter::operator()(size_t index) const {
  switch (index) {
    case 0:
      return std::get<0>(inTextIndexMap_);
    case 1:
      return std::get<1>(inTextIndexMap_);
    case 2:
      return std::get<2>(inTextIndexMap_);
    default:
      return false;
  }
}
