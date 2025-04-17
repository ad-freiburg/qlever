//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "LocalVocabEntry.h"

#include "index/IndexImpl.h"
#include "util/Exception.h"

// ___________________________________________________________________________
auto LocalVocabEntry::positionInVocabExpensiveCase() const -> PositionInVocab {
  // Lookup the lower and upper bound from the vocabulary of the index,
  // cache and return them.
  const IndexImpl& index = IndexImpl::staticGlobalSingletonIndex();
  PositionInVocab positionInVocab;
  const auto& vocab = index.getVocab();
  using SortLevel = Index::Vocab::SortLevel;

  auto lowerVec = vocab.lower_bound(toStringRepresentation(), SortLevel::TOTAL);
  auto upperVec = vocab.upper_bound(toStringRepresentation(), SortLevel::TOTAL);
  AD_CORRECTNESS_CHECK(lowerVec.size() == upperVec.size());
  for (size_t i = 0; i < lowerVec.size(); i++) {
    std::pair<ad_utility::CopyableAtomic<VocabIndex>,
              ad_utility::CopyableAtomic<VocabIndex>>
        p;
    AD_CORRECTNESS_CHECK(upperVec[i].get() - lowerVec[i].get() <= 1);
    p.first.store(lowerVec[i], std::memory_order_relaxed);
    p.second.store(upperVec[i], std::memory_order_relaxed);
  }
  positionInVocabKnown_.store(true, std::memory_order_release);
  return positionInVocab;
}
