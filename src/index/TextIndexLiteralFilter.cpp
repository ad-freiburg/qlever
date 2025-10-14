// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#include "index/TextIndexLiteralFilter.h"

#include <re2/re2.h>

// _____________________________________________________________________________
TextIndexLiteralFilter::TextIndexLiteralFilter()
    : regex_{std::make_unique<RE2>("(?s).*")},
      filterType_{LiteralFilterType::AcceptMatching},
      addAllLiterals_(true){};

// _____________________________________________________________________________
TextIndexLiteralFilter::TextIndexLiteralFilter(
    const TextIndexLiteralConfiguration& config)
    : regex_{std::make_unique<RE2>(config.predicateRegex_, RE2::Quiet)},
      filterType_{config.isWhitelistOrBlacklist_},
      addAllLiterals_(config.addAllLiterals_) {
  if (!regex_->ok()) {
    throw std::runtime_error{
        absl::StrCat("The regex supposed to filter predicates for which the "
                     "objects are stored in the text index was \"",
                     regex_->pattern(),
                     "\". This is not supported by QLever (which uses "
                     "Google's RE2 library); "
                     "the error from RE2 is: ",
                     regex_->error())};
  }
}

// _____________________________________________________________________________
std::tuple<bool, bool, bool> TextIndexLiteralFilter::computeInTextIndexMap(
    const TripleComponent& s, const TripleComponent& p,
    const TripleComponent& o) const {
  // If add all Literals check if they are a literal
  if (addAllLiterals_) {
    return {s.isLiteral(), p.isLiteral(), o.isLiteral()};
  }
  if (!o.isLiteral() || !p.isIri()) {
    return {false, false, false};
  }
  return {false, false, shouldObjectBeInTextIndex(p, o)};
}

// _____________________________________________________________________________
bool TextIndexLiteralFilter::shouldObjectBeInTextIndex(
    const TripleComponent& p, const TripleComponent& o) const {
  // The condition is true iff:
  // object is a literal AND ([predicate matches regex AND filter is set to
  // `AcceptMatching`] OR [predicate doesn't match regex AND filter is set to
  // `DeclineMatching`])
  return o.isLiteral() &&
         ((filterType_ == LiteralFilterType::AcceptMatching) ==
          RE2::PartialMatch(p.getIri().toStringRepresentation(), *regex_));
}
