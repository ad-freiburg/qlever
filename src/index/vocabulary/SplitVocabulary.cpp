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
