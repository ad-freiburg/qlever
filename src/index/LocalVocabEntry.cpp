//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "LocalVocabEntry.h"

#include "index/IndexImpl.h"

// ___________________________________________________________________________
auto LocalVocabEntry::positionInVocabExpensiveCase() const -> PositionInVocab {
  // Lookup the lower and upper bound from the vocabulary of the index,
  // cache and return them.
  const IndexImpl& index = IndexImpl::staticGlobalSingletonIndex();
  PositionInVocab positionInVocab;
  const auto& vocab = index.getVocab();
  using SortLevel = Index::Vocab::SortLevel;
  positionInVocab.lowerBound_ =
      vocab.lower_bound(toStringRepresentation(), SortLevel::TOTAL);
  positionInVocab.upperBound_ =
      vocab.upper_bound(toStringRepresentation(), SortLevel::TOTAL);
  AD_CORRECTNESS_CHECK(positionInVocab.upperBound_.get() -
                           positionInVocab.lowerBound_.get() <=
                       1);
  lowerBoundInVocab_.store(positionInVocab.lowerBound_,
                           std::memory_order_relaxed);
  upperBoundInVocab_.store(positionInVocab.upperBound_,
                           std::memory_order_relaxed);
  positionInVocabKnown_.store(true, std::memory_order_release);
  return positionInVocab;
}
