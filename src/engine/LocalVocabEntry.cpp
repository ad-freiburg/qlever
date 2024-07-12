//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "LocalVocabEntry.h"

#include "index/IndexImpl.h"

// TODO<joka921> Consider moving the cheap case (if precomputed) into the
// header.
auto LocalVocabEntry::positionInVocab() const -> PositionInVocab {
  if (positionInVocabKnown) {
    return {lowerBoundInVocab_, upperBoundInVocab_};
  }
  const IndexImpl& index = IndexImpl::staticGlobalSingletonIndex();
  PositionInVocab positionInVocab;
  const auto& vocab = index.getVocab();
  positionInVocab.lowerBound_ =
      vocab.lower_bound_external(toStringRepresentation());
  positionInVocab.upperBound_ =
      vocab.upper_bound_external(toStringRepresentation());
  lowerBoundInVocab_ = positionInVocab.lowerBound_;
  upperBoundInVocab_ = positionInVocab.upperBound_;
  return positionInVocab;
}
