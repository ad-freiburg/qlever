//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "LocalVocabEntry.h"

#include "global/VocabIndex.h"
#include "index/IndexImpl.h"

// ___________________________________________________________________________
auto LocalVocabEntry::positionInVocabExpensiveCase() const -> PositionInVocab {
  // Lookup the lower and upper bound from the vocabulary of the index,
  // cache and return them. This represents the place in the vocabulary where
  // this word would be stored if it were present.
  const IndexImpl& index = IndexImpl::staticGlobalSingletonIndex();
  PositionInVocab positionInVocab;

  const auto& vocab = index.getVocab();

  auto [lower, upper] = [&]() {
    if (auto opt = index.encodedValueManager().encode(toStringRepresentation());
        opt.has_value()) {
      return std::pair{opt.value(), Id::fromBits(opt.value().getBits() + 1)};
    }
    auto [l, u] = vocab.getPositionOfWord(toStringRepresentation());
    AD_CORRECTNESS_CHECK(u.get() - l.get() <= 1);
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
