// Copyright 2025 University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "index/vocabulary/GeoVocabulary.h"
#include "index/vocabulary/SplitVocabularyImpl.h"

// Explicit template instantiations
using namespace detail::splitVocabulary;
template class SplitVocabulary<
    GeoSplitFunc, GeoFilenameFunc,
    CompressedVocabulary<VocabularyInternalExternal>,
    GeoVocabulary<CompressedVocabulary<VocabularyInternalExternal>>>;
template class SplitVocabulary<GeoSplitFunc, GeoFilenameFunc,
                               VocabularyInMemory,
                               GeoVocabulary<VocabularyInMemory>>;

// _____________________________________________________________________________
// Partition marked indices into underlying vocabulary-local index lists.
template <typename SplitFunc, typename FilenameFunc, typename... VocabTypes>
typename SplitVocabulary<SplitFunc, FilenameFunc,
                         VocabTypes...>::IndicesByMarker
SplitVocabulary<SplitFunc, FilenameFunc, VocabTypes...>::
    partitionUnderlyingIndicesByMarker(ql::span<const size_t> indices) {
  IndicesByMarker underlyingVocabIndicesByMarker;
  for (auto markedIndex : indices) {
    underlyingVocabIndicesByMarker[getMarker(markedIndex)].push_back(
        getVocabIndex(markedIndex));
  }
  return underlyingVocabIndicesByMarker;
}
