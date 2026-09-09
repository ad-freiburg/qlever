//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "index/LocalVocabEntry.h"

#include <utility>
#include <variant>

#include "global/VocabIndex.h"
#include "index/LocalVocabContext.h"

// ___________________________________________________________________________
ql::strong_ordering LocalVocabEntry::compareThreeWay(
    const LocalVocabEntry& rhs) const {
  checkSameContext(rhs);
  // If the index has a secondary vocabulary, then first compare the positions
  // in the vocabularies, see the documentation of this function in the header
  // for why this is required. Without such a vocabulary the comparison of the
  // strings below already yields the same result, so skip the position, which
  // would require a lookup in the vocabulary of the index.
  if (context_->hasSecondaryVocabulary()) {
    auto position = positionInVocab();
    auto rhsPosition = rhs.positionInVocab();
    if (position.lowerBound_ != rhsPosition.lowerBound_) {
      return position.lowerBound_ < rhsPosition.lowerBound_
                 ? ql::strong_ordering::less
                 : ql::strong_ordering::greater;
    }
    // A word that is contained in one of the vocabularies (in which case the
    // bounds differ) is greater than a word that only would be sorted at the
    // same position (in which case the bounds are equal), because the latter is
    // strictly smaller than the word at that position.
    bool isContained = position.lowerBound_ != position.upperBound_;
    bool rhsIsContained = rhsPosition.lowerBound_ != rhsPosition.upperBound_;
    if (isContained != rhsIsContained) {
      return isContained ? ql::strong_ordering::greater
                         : ql::strong_ordering::less;
    }
  }
  // The positions are equal (or the index has no secondary vocabulary), so the
  // string values decide, which is exactly the semantic comparison.
  return compareThreeWaySemantically(rhs);
}

// _____________________________________________________________________________
ql::strong_ordering LocalVocabEntry::compareThreeWaySemantically(
    const LocalVocabEntry& rhs) const {
  checkSameContext(rhs);
  int i = context_->compareWords(toStringRepresentation(),
                                 rhs.toStringRepresentation());
  if (i < 0) {
    return ql::strong_ordering::less;
  } else if (i > 0) {
    return ql::strong_ordering::greater;
  } else {
    return ql::strong_ordering::equal;
  }
}

// ___________________________________________________________________________
auto LocalVocabEntry::positionInVocabExpensiveCase() const -> PositionInVocab {
  // Lookup the lower and upper bound from the vocabulary of the index,
  // cache and return them. This represents the place in the vocabulary where
  // this word would be stored if it were present.
  PositionInVocab positionInVocab;

  // NOTE: For encoded IRIs, the only purpose of the returned `std::pair` is to
  // give us a consistent ordering, which is important for determining equality
  // and for operations like `Join`, `Distinct`, `GroupBy`, etc.
  auto [lower, upper] = [&]() -> std::pair<Id, Id> {
    if (auto opt = context_->encodeAsId(toStringRepresentation());
        opt.has_value()) {
      return {opt.value(), Id::fromBits(opt.value().getBits() + 1)};
    }
    // Look up the word in the vocabularies of the index. A word that is
    // contained in one of them is positioned exactly at its `Id`, so its range
    // is that single `Id`. A word that is contained in none of them is
    // positioned at the empty range at which it would be sorted into the
    // vocabulary of the main index.
    auto idOrBounds =
        context_->lookupWordInVocabularies(toStringRepresentation());
    if (const auto* id = std::get_if<Id>(&idOrBounds)) {
      return {*id, Id::fromBits(id->getBits() + 1)};
    }
    auto [l, u] = std::get<LocalVocabContext::VocabBounds>(idOrBounds);
    return {Id::makeFromVocabIndex(l), Id::makeFromVocabIndex(u)};
  }();
  positionInVocab.lowerBound_ = IdProxy::make(lower.getBits());
  positionInVocab.upperBound_ = IdProxy::make(upper.getBits());

  lowerBoundInVocab_.store(positionInVocab.lowerBound_,
                           std::memory_order_relaxed);
  upperBoundInVocab_.store(positionInVocab.upperBound_,
                           std::memory_order_relaxed);
  positionInVocabKnown_.store(true, std::memory_order_release);
  return positionInVocab;
}

// _____________________________________________________________________________
LocalVocabEntry LocalVocabEntry::fromStringRepresentation(
    std::string s, const LocalVocabContext& ctx) {
  return LocalVocabEntry{Base::fromStringRepresentation(std::move(s)), ctx};
}

// _____________________________________________________________________________
LocalVocabEntry LocalVocabEntry::fromIriref(std::string_view view,
                                            const LocalVocabContext& ctx) {
  return LocalVocabEntry{IriT::fromIriref(view), ctx};
}

// _____________________________________________________________________________
LocalVocabEntry LocalVocabEntry::literalWithoutQuotes(
    std::string_view view, const LocalVocabContext& ctx) {
  return LocalVocabEntry{LiteralT::literalWithoutQuotes(view), ctx};
}

// _____________________________________________________________________________
LocalVocabEntry LocalVocabEntry::literalWithNormalizedContent(
    NormalizedStringView view, const LocalVocabContext& ctx) {
  return LocalVocabEntry{LiteralT::literalWithNormalizedContent(view), ctx};
}
