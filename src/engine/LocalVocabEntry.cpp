//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "LocalVocabEntry.h"

#include "index/IndexImpl.h"

// TODO<joka921> Consider moving the cheap case (if precomputed) into the
// header.
auto LocalVocabEntry::positionInVocab() const -> PositionInVocab {
  // Immediately return if we have previously computed and cached the position.
  if (positionInVocabKnown_.load(std::memory_order_acquire)) {
    return {lowerBoundInVocab_.load(std::memory_order_relaxed),
            upperBoundInVocab_.load(std::memory_order_relaxed)};
  }

  // Lookup the lower and upper bound from the vocabulary of the index,
  // cache and return them.
  const IndexImpl& index = IndexImpl::staticGlobalSingletonIndex();
  PositionInVocab positionInVocab;
  const auto& vocab = index.getVocab();
  positionInVocab.lowerBound_ =
      vocab.lower_bound_external(toStringRepresentation());
  positionInVocab.upperBound_ =
      vocab.upper_bound_external(toStringRepresentation());
  lowerBoundInVocab_.store(positionInVocab.lowerBound_,
                           std::memory_order_relaxed);
  upperBoundInVocab_.store(positionInVocab.upperBound_,
                           std::memory_order_relaxed);
  positionInVocabKnown_.store(true, std::memory_order_release);
  return positionInVocab;
}
