//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "index/LocalVocabEntry.h"

#include "global/VocabIndex.h"
#include "index/LocalVocabContext.h"

// ___________________________________________________________________________
ql::strong_ordering LocalVocabEntry::compareThreeWay(
    const LocalVocabEntry& rhs) const {
  AD_EXPENSIVE_CHECK(
      context_ == rhs.context_,
      "Contexts of LocalVocabEntries have to be identical. If this is not the "
      "case this means that stale entries associated with an old index are "
      "falsely carried over somewhere.");
  // If the index has an auxiliary vocabulary, then first compare the positions
  // in the vocabularies, see the documentation of this function in the header
  // for why this is required. Without such a vocabulary the comparison of the
  // strings below already yields the same result, so skip the position, which
  // would require a lookup in the vocabulary of the index.
  if (context_->hasAuxVocabulary()) {
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
  auto [lower, upper] = [&]() {
    if (auto opt = context_->encodeAsId(toStringRepresentation());
        opt.has_value()) {
      return std::pair{opt.value(), Id::fromBits(opt.value().getBits() + 1)};
    }
    auto [l, u] = context_->getPositionOfWord(toStringRepresentation());
    AD_CORRECTNESS_CHECK(u.get() - l.get() <= 1);
    if (l == u) {
      // The word is not in the vocabulary of the main index, so it may be in
      // the auxiliary vocabulary. Note that the two vocabularies are disjoint,
      // so we only have to look there if the lookup above has failed.
      if (auto auxIndex = context_->getAuxVocabIndex(toStringRepresentation());
          auxIndex.has_value()) {
        auto id = Id::makeFromAuxVocabIndex(auxIndex.value());
        return std::pair{id, Id::fromBits(id.getBits() + 1)};
      }
    }
    return std::pair{Id::makeFromVocabIndex(l), Id::makeFromVocabIndex(u)};
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
