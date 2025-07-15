// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "index/vocabulary/GeoVocabulary.h"

#include <stdexcept>

#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "rdfTypes/GeometryInfo.h"
#include "util/Exception.h"

using ad_utility::GeometryInfo;

// ____________________________________________________________________________
template <typename V>
void GeoVocabulary<V>::open(const std::string& filename) {
  literals_.open(filename);

  geoInfoFile_.open(getGeoInfoFilename(filename).c_str(), "r");

  // Read header of `geoInfoFile_` to determine version
  std::decay_t<decltype(ad_utility::GEOMETRY_INFO_VERSION)> versionOfFile = 0;
  geoInfoFile_.read(&versionOfFile, geoInfoHeader, 0);

  // Check version of geo info file
  if (versionOfFile != ad_utility::GEOMETRY_INFO_VERSION) {
    throw std::runtime_error(absl::StrCat(
        "The geometry info version of ", getGeoInfoFilename(filename), " is ",
        versionOfFile, ", which is incompatible with version ",
        ad_utility::GEOMETRY_INFO_VERSION,
        " as required by this version of QLever. Please rebuild your index."));
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
      geoInfoFile_{getGeoInfoFilename(filename), "w"} {
  // Initialize geo info file with header
  geoInfoFile_.write(&ad_utility::GEOMETRY_INFO_VERSION, geoInfoHeader);
};

// ____________________________________________________________________________
template <typename V>
uint64_t GeoVocabulary<V>::WordWriter::operator()(std::string_view word,
                                                  bool isExternal) {
  uint64_t index;

  // Store the WKT literal as a string in the underlying vocabulary
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
  // `WordWriterBase` ensures that this is not called twice and we thus do not
  // try to close the file handle twice
  underlyingWordWriter_->finish();
  geoInfoFile_.close();
}

// ____________________________________________________________________________
template <typename V>
std::optional<GeometryInfo> GeoVocabulary<V>::getGeoInfo(uint64_t index) const {
  AD_CONTRACT_CHECK(index < size());
  // Allocate the required number of bytes
  std::array<uint8_t, geoInfoOffset> buffer;
  void* ptr = &buffer;

  // Read into the buffer
  geoInfoFile_.read(ptr, geoInfoOffset, geoInfoHeader + index * geoInfoOffset);

  // If all bytes are zero, this record on disk represents an invalid geometry.
  // The `GeometryInfo` class makes the guarantee that it can not have an
  // all-zero binary representation.
  if (buffer == invalidGeoInfoBuffer) {
    return std::nullopt;
  }

  // Interpret the buffer as a `GeometryInfo` object
  return absl::bit_cast<GeometryInfo>(buffer);
}

// Explicit template instantiations
template class GeoVocabulary<CompressedVocabulary<VocabularyInternalExternal>>;
template class GeoVocabulary<VocabularyInMemory>;
