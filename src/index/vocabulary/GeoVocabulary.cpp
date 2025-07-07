// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "index/vocabulary/GeoVocabulary.h"

#include <stdexcept>

#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "util/Exception.h"
#include "util/GeometryInfo.h"

// ____________________________________________________________________________
template <typename V>
void GeoVocabulary<V>::open(const std::string& filename) {
  literals_.open(filename);

  geoInfoFile_.open(getGeoInfoFilename(filename).c_str(), "r");

  // Read header of `geoInfoFile_` to determine version
  std::decay_t<decltype(ad_utility::GEOMETRY_INFO_VERSION)> versionOfFile = 0;
  geoInfoFile_.read(&versionOfFile, geoInfoHeader, 0);

  // Check version
  if (versionOfFile != ad_utility::GEOMETRY_INFO_VERSION) {
    throw std::runtime_error(absl::StrCat(
        "The geometry info version of ", getGeoInfoFilename(filename), " is ",
        versionOfFile, ", which is incompatible with ",
        ad_utility::GEOMETRY_INFO_VERSION,
        " as required by this version of QLever."));
  }
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
    : underlyingWordWriter_{vocabulary.makeDiskWriterPtr(filename)},
      geoInfoFile_{filename + geoInfoSuffix, "w"} {
  geoInfoFile_.write(&ad_utility::GEOMETRY_INFO_VERSION, geoInfoHeader);
};

// ____________________________________________________________________________
template <typename V>
uint64_t GeoVocabulary<V>::WordWriter::operator()(std::string_view word,
                                                  bool isExternal) {
  uint64_t index;

  // Store the WKT literal
  index = (*underlyingWordWriter_)(word, isExternal);

  // Precompute `GeometryInfo` and write the `GeometryInfo` to disk, or write a
  // zero buffer of the same size (indicating an invalid geometry). This is
  // required to ensure direct access by index is still possible on the file.
  const void* ptr = &invalidGeoInfoBuffer;
  auto info = GeometryInfo::fromWktLiteral(word);
  if (info.has_value()) {
    ptr = &info.value();
  }
  geoInfoFile_.write(ptr, geoInfoOffset);

  return index;
};

// ____________________________________________________________________________
template <typename V>
void GeoVocabulary<V>::WordWriter::finishImpl() {
  // WordWriterBase ensures that this is not called twice and we thus don't try
  // to close the file handle twice
  underlyingWordWriter_->finish();
  geoInfoFile_.close();
}

// ____________________________________________________________________________
template <typename V>
std::optional<GeometryInfo> GeoVocabulary<V>::getGeoInfo(uint64_t index) const {
  AD_CONTRACT_CHECK(index < size());
  // Allocate the required number of bytes
  uint8_t buffer[geoInfoOffset];
  void* ptr = &buffer;
  // Read into the buffer
  geoInfoFile_.read(ptr, geoInfoOffset, geoInfoHeader + index * geoInfoOffset);
  // If all bytes are zero, this record on disk represents an invalid geometry.
  if (std::all_of(static_cast<uint8_t*>(ptr),
                  static_cast<uint8_t*>(ptr) + geoInfoOffset,
                  [](uint8_t byte) { return byte == 0; })) {
    return std::nullopt;
  }
  // Interpret the buffer as a GeometryInfo object
  return *absl::bit_cast<GeometryInfo*>(ptr);
}

// Avoid linker trouble.
template class GeoVocabulary<CompressedVocabulary<VocabularyInternalExternal>>;
template class GeoVocabulary<VocabularyInMemory>;
