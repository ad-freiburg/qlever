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

#include <spatialjoin/Sweeper.h>

#include <stdexcept>
#include <vector>

#include "backports/filesystem.h"
#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/VocabularyConstraints.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "rdfTypes/GeoPoint.h"
#include "rdfTypes/GeometryInfo.h"
#include "rdfTypes/ParsedGeometry.h"
#include "util/Exception.h"
#include "util/File.h"

using ad_utility::GeometryInfo;
using ad_utility::ParsedGeometry;

// ____________________________________________________________________________
template <typename V>
GeoVocabulary<V>::GeoVocabulary() {
  // The index of a word is computed from its position in the underlying
  // vocabulary (see `indexFromPosition`), which requires contiguous positions
  // and a plain `getPositionOfWord`. The vocabularies with "holes" and the
  // composite vocabularies do not qualify.
  static_assert(!HasSpecialGetPositionOfWord<V>);
}

// ____________________________________________________________________________
template <typename V>
void GeoVocabulary<V>::open(const std::string& filename) {
  literals_.open(filename);

  // Open the geo info file and the parsed geometries file, and check that
  // their headers hold the versions this code expects.
  auto openWithVersionCheck =
      [](ad_utility::File& file, const std::string& filename,
         uint64_t expectedVersion, std::string_view what) {
        file.open(filename.c_str(), "r");
        uint64_t versionOfFile = 0;
        file.read(&versionOfFile, sizeof(versionOfFile), 0);
        if (versionOfFile != expectedVersion) {
          throw std::runtime_error(absl::StrCat(
              "The ", what, " version of ", filename, " is ", versionOfFile,
              ", which is incompatible with version ", expectedVersion,
              " as required by this version of QLever. Please rebuild your "
              "index."));
        }
      };
  openWithVersionCheck(geoInfoFile_, getGeoInfoFilename(filename),
                       ad_utility::GEOMETRY_INFO_VERSION, "geometry info");
  openWithVersionCheck(parsedGeometriesFile_,
                       getParsedGeometriesFilename(filename),
                       ad_utility::PARSED_GEOMETRY_VERSION, "parsed geometry");

  endIndex_ = computeEndIndex();
}

// ____________________________________________________________________________
template <typename V>
void GeoVocabulary<V>::close() {
  literals_.close();
  geoInfoFile_.close();
  parsedGeometriesFile_.close();
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
    std::optional<ad_utility::GeoCellGrid> grid,
    size_t parsedGeometriesMinLength)
    : underlyingWordWriter_{vocabulary.makeDiskWriterPtr(filename)},
      geoInfoFile_{getGeoInfoFilename(filename), "w"},
      grid_{grid},
      parsedGeometriesFile_{getParsedGeometriesFilename(filename), "w"},
      parsedGeometriesMinLength_{parsedGeometriesMinLength} {
  // Initialize the geo info file and the parsed geometries file with their
  // headers.
  geoInfoFile_.write(&ad_utility::GEOMETRY_INFO_VERSION, geoInfoHeader);
  parsedGeometriesFileSize_ =
      parsedGeometriesFile_.write(&ad_utility::PARSED_GEOMETRY_VERSION,
                                  sizeof(ad_utility::PARSED_GEOMETRY_VERSION));

  // The sweeper that computes the parsed geometries, if any are to be stored.
  // Its temporary files (which it deletes itself) go next to the vocabulary.
  if (parsedGeometriesMinLength_ > 0) {
    ql::filesystem::path path{filename};
    std::string dir = path.parent_path().string();
    sweeper_ = std::make_unique<sj::Sweeper>(
        ParsedGeometry::sweeperConfig(), dir.empty() ? "." : dir,
        absl::StrCat(path.filename().string(), ".spatialjoin"));
  }
}

// ____________________________________________________________________________
template <typename V>
uint64_t GeoVocabulary<V>::WordWriter::operator()(std::string_view word,
                                                  bool isExternal) {
  uint64_t index;

  // Store the WKT literal as a string in the underlying vocabulary
  index = (*underlyingWordWriter_)(word, isExternal);

  // Precompute the `GeometryInfo`.
  auto info = GeometryInfo::fromWktLiteral(word);
  if (info.has_value()) {
    if (!info.value().getMetricArea().isValid()) {
      ++numInvalidPolygonArea_;
    }
  } else {
    ++numInvalidGeometries_;
  }

  // For a valid literal of at least the minimum length, store the parsed
  // geometry (see `ParsedGeometry`) as its number of bytes followed by the
  // bytes, and remember its offset in the `GeometryInfo`.
  if (parsedGeometriesMinLength_ > 0 &&
      word.size() >= parsedGeometriesMinLength_) {
    auto parsed = info.has_value()
                      ? ParsedGeometry::fromWktLiteral(word, *sweeper_)
                      : std::nullopt;
    if (parsed.has_value()) {
      std::string bytes = parsed.value().toBytes();
      uint64_t numBytes = bytes.size();
      info.value().setParsedGeometryOffset(parsedGeometriesFileSize_);
      parsedGeometriesFileSize_ +=
          parsedGeometriesFile_.write(&numBytes, sizeof(numBytes));
      parsedGeometriesFileSize_ +=
          parsedGeometriesFile_.write(bytes.data(), bytes.size());
      ++numParsedGeometries_;
    } else {
      ++numParsedGeometriesSkipped_;
    }
  }

  // Write the `GeometryInfo` to disk, or a zero buffer of the same size
  // (indicating an invalid geometry). This is required to ensure direct
  // access by index is still possible on the file.
  const void* ptr = info.has_value() ? static_cast<const void*>(&info.value())
                                     : &invalidGeoInfoBuffer;
  geoInfoFile_.write(ptr, geoInfoOffset);

  if (grid_.has_value()) {
    AD_CORRECTNESS_CHECK(index == numWords_);
    // Keep one position free, so that `endIndex` (the past-the-end position
    // combined with the cell of the last word) is always a valid index.
    AD_CONTRACT_CHECK(numWords_ + 1 < grid_->maxNumWords(),
                      "Too many WKT literals for the configured geo cell "
                      "grid, please rebuild with a smaller grid level");
    auto cellIndex = cellIndexOfWord(grid_.value(), info, word);
    AD_CONTRACT_CHECK(
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
  parsedGeometriesFile_.close();

  if (numParsedGeometries_ + numParsedGeometriesSkipped_ > 0) {
    AD_LOG_INFO << "Stored the parsed geometries of " << numParsedGeometries_
                << " of the "
                << numParsedGeometries_ + numParsedGeometriesSkipped_
                << " WKT literals with at least " << parsedGeometriesMinLength_
                << " bytes (the others are invalid geometries or cannot be "
                   "parsed by libspatialjoin)"
                << std::endl;
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

// ____________________________________________________________________________
template <typename V>
std::optional<ParsedGeometry> GeoVocabulary<V>::getParsedGeometry(
    uint64_t index) const {
  auto info = getGeoInfo(index);
  if (!info.has_value() || info.value().getParsedGeometryOffset() < 0) {
    return std::nullopt;
  }

  // Read the number of bytes and then the bytes (see the `WordWriter`).
  uint64_t offset = info.value().getParsedGeometryOffset();
  uint64_t numBytes = 0;
  parsedGeometriesFile_.read(&numBytes, sizeof(numBytes), offset);
  std::string bytes(numBytes, '\0');
  parsedGeometriesFile_.read(bytes.data(), numBytes, offset + sizeof(numBytes));
  return ParsedGeometry::fromBytes(bytes);
}

// Explicit template instantiations
template class GeoVocabulary<CompressedVocabulary<VocabularyInternalExternal>>;
template class GeoVocabulary<VocabularyInMemory>;
