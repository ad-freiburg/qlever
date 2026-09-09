// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@cs.uni-freiburg.de>, UFR
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/vocabulary/GeoVocabulary.h"

#include <stdexcept>

#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "rdfTypes/GeoPoint.h"
#include "rdfTypes/GeometryInfo.h"
#include "util/Exception.h"
#include "util/File.h"

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

  endIndex_ = computeEndIndex();
}

// ____________________________________________________________________________
template <typename V>
void GeoVocabulary<V>::close() {
  literals_.close();
  geoInfoFile_.close();
  endIndex_ = 0;
}

// ____________________________________________________________________________
template <typename V>
uint64_t GeoVocabulary<V>::indexFromPosition(uint64_t position,
                                             std::string_view word) const {
  if (!grid_.has_value()) {
    return position;
  }
  return grid_->indexFromCellAndPosition(
      cellIndexOfWord(grid_.value(), geoInfoAtPosition(position), word),
      position);
}

// ____________________________________________________________________________
template <typename V>
uint64_t GeoVocabulary<V>::computeEndIndex() const {
  auto numWords = size();
  if (!grid_.has_value() || numWords == 0) {
    return numWords;
  }
  // One past the largest index: the cell of the last word combined with the
  // past-the-end position.
  auto lastPosition = numWords - 1;
  auto lastCell = cellIndexOfWord(
      grid_.value(), geoInfoAtPosition(lastPosition), literals_[lastPosition]);
  return grid_->indexFromCellAndPosition(lastCell, numWords);
}

// ____________________________________________________________________________
template <typename V>
VocabBatchLookupResult GeoVocabulary<V>::lookupBatch(
    ql::span<const size_t> indices) const {
  if (!grid_.has_value()) {
    return literals_.lookupBatch(indices);
  }
  std::vector<size_t> positions;
  positions.reserve(indices.size());
  for (size_t index : indices) {
    positions.push_back(positionFromIndex(index));
  }
  return literals_.lookupBatch(positions);
}

// ____________________________________________________________________________
template <typename V>
GeoVocabulary<V>::WordWriter::WordWriter(
    const V& vocabulary, const std::string& filename,
    std::optional<ad_utility::GeoCellGrid> grid)
    : underlyingWordWriter_{vocabulary.makeDiskWriterPtr(filename)},
      geoInfoFile_{getGeoInfoFilename(filename), "w"},
      grid_{grid} {
  // Initialize geo info file with header
  geoInfoFile_.write(&ad_utility::GEOMETRY_INFO_VERSION, geoInfoHeader);
}

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
    if (!info.value().getMetricArea().isValid()) {
      ++numInvalidPolygonArea_;
    }
    ptr = &info.value();
  } else {
    ++numInvalidGeometries_;
  }
  geoInfoFile_.write(ptr, geoInfoOffset);

  if (grid_.has_value()) {
    AD_CORRECTNESS_CHECK(index == numWords_);
    // Keep one position free, so that `endIndex` (the past-the-end position
    // combined with the cell of the last word) is always a valid index.
    AD_CORRECTNESS_CHECK(numWords_ + 1 < grid_->maxNumWords(),
                         "Too many WKT literals for the configured geo cell "
                         "grid, please rebuild with a smaller grid level");
    auto cellIndex = cellIndexOfWord(grid_.value(), info, word);
    AD_CORRECTNESS_CHECK(
        !lastCellIndex_.has_value() || lastCellIndex_.value() <= cellIndex,
        "WKT literals were not passed to the GeoVocabulary in the order of "
        "their geo grid cells");
    lastCellIndex_ = cellIndex;
    index = grid_->indexFromCellAndPosition(cellIndex, numWords_);
  }
  ++numWords_;
  return index;
}

// ____________________________________________________________________________
template <typename V>
void GeoVocabulary<V>::WordWriter::finishImpl() {
  // `WordWriterBase` ensures that this is not called twice and we thus do not
  // try to close the file handle twice
  underlyingWordWriter_->finish();
  geoInfoFile_.close();

  if (numInvalidGeometries_ > 0) {
    AD_LOG_WARN << "Geometry preprocessing skipped " << numInvalidGeometries_
                << " invalid WKT literal"
                << (numInvalidGeometries_ == 1 ? "" : "s") << std::endl;
  }
  if (numInvalidPolygonArea_ > 0) {
    AD_LOG_WARN << "Geometry preprocessing could not compute the area for "
                << numInvalidPolygonArea_ << " malformed polygon geometr"
                << (numInvalidPolygonArea_ == 1 ? "y" : "ies") << std::endl;
  }
}

// ____________________________________________________________________________
template <typename V>
GeoVocabulary<V>::WordWriter::~WordWriter() {
  if (!finishWasCalled()) {
    ad_utility::terminateIfThrows([this]() { this->finish(); },
                                  "Calling `finish` from the destructor of "
                                  "`GeoVocabulary`");
  }
}

// ____________________________________________________________________________
template <typename V>
std::optional<GeometryInfo> GeoVocabulary<V>::geoInfoAtPosition(
    uint64_t position) const {
  AD_CONTRACT_CHECK(position < size());
  // Allocate the required number of bytes
  std::array<uint8_t, geoInfoOffset> buffer;
  void* ptr = &buffer;

  // Read into the buffer
  geoInfoFile_.read(ptr, geoInfoOffset,
                    geoInfoHeader + position * geoInfoOffset);

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
