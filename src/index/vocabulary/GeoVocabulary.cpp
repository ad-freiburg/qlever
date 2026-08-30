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

#include "backports/filesystem.h"
#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "rdfTypes/GeoPoint.h"
#include "rdfTypes/GeometryInfo.h"
#include "util/Exception.h"
#include "util/File.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/SerializePair.h"
#include "util/Serializer/SerializeVector.h"

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

  // Read the geo cell grid and the cell runs if present. The `.geocells` file
  // is authoritative: without it the vocabulary uses plain indices.
  grid_ = std::nullopt;
  cellRuns_.clear();
  auto cellsFilename = getGeoCellsFilename(filename);
  if (ql::filesystem::exists(cellsFilename)) {
    ad_utility::serialization::FileReadSerializer in{cellsFilename};
    uint64_t version = 0;
    in >> version;
    if (version != 1 && version != geoCellsVersion) {
      throw std::runtime_error(absl::StrCat(
          "The geo cells file ", cellsFilename, " has version ", version,
          ", which is incompatible with version ", geoCellsVersion,
          " as required by this version of QLever. Please rebuild your "
          "index."));
    }
    // Version 1 files have no scheme field and are implicitly `Flat`.
    uint64_t scheme = 0;
    if (version >= 2) {
      in >> scheme;
      AD_CORRECTNESS_CHECK(
          scheme <= static_cast<uint64_t>(
                        ad_utility::GeoCellGridScheme::Hierarchical3Shifts));
    }
    uint64_t level = 0;
    in >> level;
    grid_ = ad_utility::GeoCellGrid{
        static_cast<uint8_t>(level),
        static_cast<ad_utility::GeoCellGridScheme>(scheme)};
    in >> cellRuns_;
    AD_CORRECTNESS_CHECK(!cellRuns_.empty() || literals_.size() == 0);
  }
}

// ____________________________________________________________________________
template <typename V>
void GeoVocabulary<V>::close() {
  literals_.close();
  geoInfoFile_.close();
}

// ____________________________________________________________________________
template <typename V>
uint64_t GeoVocabulary<V>::cellOfPosition(uint64_t position) const {
  AD_CORRECTNESS_CHECK(!cellRuns_.empty());
  // Find the last run that starts at or before `position`.
  auto it = ql::ranges::upper_bound(cellRuns_, position, {},
                                    &std::pair<uint64_t, uint64_t>::first);
  AD_CORRECTNESS_CHECK(it != cellRuns_.begin());
  return (it - 1)->second;
}

// ____________________________________________________________________________
template <typename V>
uint64_t GeoVocabulary<V>::toAnnotatedIndex(uint64_t position) const {
  if (!grid_.has_value()) {
    return position;
  }
  return grid_->annotateIndex(cellOfPosition(position), position);
}

// ____________________________________________________________________________
template <typename V>
uint64_t GeoVocabulary<V>::endIndex() const {
  auto numWords = size();
  if (!grid_.has_value() || numWords == 0) {
    return numWords;
  }
  // One past the largest annotated index: the cell of the last word combined
  // with the past-the-end position.
  return grid_->annotateIndex(cellOfPosition(numWords - 1), numWords);
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
    positions.push_back(toPosition(index));
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
      geoCellsFilename_{getGeoCellsFilename(filename)},
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
    AD_CORRECTNESS_CHECK(numWords_ < grid_->maxNumWords(),
                         "Too many WKT literals for the configured geo cell "
                         "grid, please rebuild with a smaller grid level");
    // The cell assignment must be exactly that of
    // `GeoCellGrid::cellFromWktLiteral` (which the vocabulary order is based
    // on). When the `GeometryInfo` is valid, its bounding box is the one that
    // `cellFromWktLiteral` would compute, so we can reuse it; otherwise we
    // delegate to `cellFromWktLiteral`, which can still assign a regular cell
    // in corner cases where only parts of the `GeometryInfo` computation
    // failed.
    uint64_t cell = info.has_value()
                        ? grid_->cellFromBoundingBox(info->getBoundingBox())
                        : grid_->cellFromWktLiteral(word);
    if (cellRuns_.empty() || cellRuns_.back().second != cell) {
      AD_CORRECTNESS_CHECK(
          cellRuns_.empty() || cellRuns_.back().second < cell,
          "WKT literals were not passed to the GeoVocabulary in the order of "
          "their geo grid cells");
      cellRuns_.emplace_back(numWords_, cell);
    }
    index = grid_->annotateIndex(cell, numWords_);
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

  if (grid_.has_value()) {
    ad_utility::serialization::FileWriteSerializer out{geoCellsFilename_};
    out << geoCellsVersion;
    out << static_cast<uint64_t>(grid_->scheme());
    out << uint64_t{grid_->level()};
    out << cellRuns_;
  }

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
std::optional<GeometryInfo> GeoVocabulary<V>::getGeoInfo(uint64_t index) const {
  uint64_t position = toPosition(index);
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
