// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "index/vocabulary/GeoVocabulary.h"

#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/VocabularyInternalExternal.h"

// ____________________________________________________________________________
template <typename V>
GeoVocabulary<V>::WordWriter::WordWriter(const V& vocabulary,
                                         const std::string& filename)
    : underlyingWordWriter_{vocabulary.getUnderlyingVocabulary()
                                .getUnderlyingVocabulary()
                                .makeDiskWriter(filename)} {};

// ____________________________________________________________________________
template <typename V>
uint64_t GeoVocabulary<V>::WordWriter::operator()(std::string_view word,
                                                  bool isExternal) {
  underlyingWordWriter_(word, isExternal);
};

// ____________________________________________________________________________
template <typename V>
void GeoVocabulary<V>::WordWriter::finish() {
  underlyingWordWriter_.finish();
}

// ____________________________________________________________________________
template <typename V>
std::string& GeoVocabulary<V>::WordWriter::readableName() {
  return underlyingWordWriter_.readableName();
}

// ____________________________________________________________________________
template <typename V>
void GeoVocabulary<V>::build(const std::vector<std::string>& v,
                             const std::string& filename) {
  literals_.build(v, filename);
}
