// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "index/vocabulary/GeoVocabulary.h"

#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "util/Exception.h"

// ____________________________________________________________________________
template <typename V>
void GeoVocabulary<V>::open(const std::string& filename) {
  literals_.open(filename);

  geoInfoFile_.open(getGeoInfoFilename(filename).c_str(), "r");
};

// ____________________________________________________________________________
template <typename V>
void GeoVocabulary<V>::close() {
  literals_.close();
  geoInfoFile_.close();
}

// ____________________________________________________________________________
template <typename V>
GeoVocabulary<V>::WordWriter::WordWriter(const V& vocabulary,
                                         const std::string& filename)
    : underlyingWordWriter_{vocabulary.makeDiskWriter(filename)},
      geoInfoFile_{filename + geoInfoSuffix, "w"} {};

// ____________________________________________________________________________
template <typename V>
uint64_t GeoVocabulary<V>::WordWriter::operator()(std::string_view word,
                                                  bool isExternal) {
  uint64_t index;

  // Store the WKT literal
  // TODO<ullingerc> Remove this "if" after merge of #1740
  if constexpr (std::is_same_v<V, VocabularyInternalExternal>) {
    index = underlyingWordWriter_(word, isExternal);
  } else {
    index = underlyingWordWriter_(word);
  }

  // Precompute geometry info
  auto info = GeometryInfo::fromWktLiteral(word);
  geoInfoFile_.write(&info, geoInfoOffset);

  return index;
};

// ____________________________________________________________________________
template <typename V>
void GeoVocabulary<V>::WordWriter::finish() {
  // Don't close file twice.
  if (std::exchange(isFinished_, true)) {
    return;
  }

  underlyingWordWriter_.finish();
  geoInfoFile_.close();
}

// _____________________________________________________________________________
template <typename V>
GeoVocabulary<V>::WordWriter::~WordWriter() {
  throwInDestructorIfSafe_([this]() { finish(); },
                           "`~GeoVocabulary::WordWriter`");
}

// ____________________________________________________________________________
template <typename V>
std::string& GeoVocabulary<V>::WordWriter::readableName() {
  if constexpr (std::is_same_v<V, VocabularyInternalExternal>) {
    return underlyingWordWriter_.readableName();
  }
  // TODO<ullingerc> Remove this dummy as soon as possible.
  static std::string dummy;
  return dummy;
}

// ____________________________________________________________________________
template <typename V>
void GeoVocabulary<V>::build(const std::vector<std::string>& v,
                             const std::string& filename) {
  // Build text literal vocabulary
  literals_.close();
  literals_.build(v, filename);

  // Build and precompute geometry info
  geoInfoFile_.close();
  geoInfoFile_.open(getGeoInfoFilename(filename).c_str(), "w");
  for (const auto& word : v) {
    auto info = GeometryInfo::fromWktLiteral(word);
    geoInfoFile_.write(&info, geoInfoOffset);
  }
  geoInfoFile_.close();
  geoInfoFile_.open(getGeoInfoFilename(filename).c_str(), "r");
}

// ____________________________________________________________________________
template <typename V>
GeometryInfo GeoVocabulary<V>::getGeoInfo(uint64_t index) const {
  AD_CONTRACT_CHECK(index < size());
  GeometryInfo info;
  geoInfoFile_.read(&info, geoInfoOffset, index * geoInfoOffset);
  return info;
}

// Avoid linker trouble.
template class GeoVocabulary<CompressedVocabulary<VocabularyInternalExternal>>;
template class GeoVocabulary<VocabularyInMemory>;
