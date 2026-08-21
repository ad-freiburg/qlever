// Copyright 2025 - 2026, The QLever Authors, in particular:
//
// 2025        Christoph Ullinger <ullingec@cs.uni-freiburg.de>, UFR
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

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
QL_CONCEPT_OR_NOTHING(
    requires SplitFunctionT<SplitFunc>&&
        SplitFilenameFunctionT<FilenameFunc, sizeof...(VocabTypes)>)
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
