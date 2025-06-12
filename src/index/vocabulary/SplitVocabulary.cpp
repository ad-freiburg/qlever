// Copyright 2025 University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "index/vocabulary/SplitVocabularyImpl.h"

// Explicit template instantiations
using namespace detail::splitVocabulary;
template class SplitVocabulary<
    decltype(geoSplitFunc), decltype(geoFilenameFunc),
    CompressedVocabulary<VocabularyInternalExternal>,
    CompressedVocabulary<VocabularyInternalExternal>>;
template class SplitVocabulary<decltype(geoSplitFunc),
                               decltype(geoFilenameFunc), VocabularyInMemory,
                               VocabularyInMemory>;
