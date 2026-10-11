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

#include <boost/asio/post.hpp>
#include <boost/asio/use_future.hpp>
#include <stdexcept>
#include <vector>

#include "index/vocabulary/CompressedVocabulary.h"
#include "index/vocabulary/VocabularyConstraints.h"
#include "index/vocabulary/VocabularyInMemory.h"
#include "index/vocabulary/VocabularyInternalExternal.h"
#include "rdfTypes/GeoPoint.h"
#include "rdfTypes/GeometryInfo.h"
#include "util/Exception.h"
#include "util/File.h"
#include "util/GlobalExecutor.h"
#include "util/MemorySize/MemorySize.h"

using ad_utility::GeometryInfo;

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
  // Initialize the geo info file with its header. Like the records (see
  // `flushBatch`), it is written with a positioned write, because positioned
  // and sequential writes must not be mixed on the same file (see
  // `File::write`).
  geoInfoFile_.write(&ad_utility::GEOMETRY_INFO_VERSION, geoInfoHeader, 0);
}

using namespace ad_utility::memory_literals;

// A batch of words is handed to the thread pool (see `flushBatch`) as soon as
// it has this many words or this many bytes. A batch holds copies of its
// words, so the limits bound its memory, while a batch stays large enough to
// make the cost of a task on the pool negligible.
static constexpr size_t GEO_WRITER_BATCH_NUM_WORDS = 10'000;
static constexpr size_t GEO_WRITER_BATCH_NUM_BYTES = (10_MB).getBytes();

// ____________________________________________________________________________
template <typename V>
uint64_t GeoVocabulary<V>::WordWriter::operator()(std::string_view word,
                                                  bool isExternal) {
  uint64_t index;

  // Store the WKT literal as a string in the underlying vocabulary
  index = (*underlyingWordWriter_)(word, isExternal);

  if (grid_.has_value()) {
    AD_CORRECTNESS_CHECK(index == numWords_);
    // Keep one position free, so that `endIndex` (the past-the-end position
    // combined with the cell of the last word) is always a valid index.
    AD_CONTRACT_CHECK(numWords_ + 1 < grid_->maxNumWords(),
                      "Too many WKT literals for the configured geo cell "
                      "grid, please rebuild with a smaller grid level");
    // NOTE: This is the cell that `cellIndexOfWord` computes from the
    // `GeometryInfo` of the word (which is not known yet, see the class
    // comment), because a valid `GeometryInfo` has exactly the bounding box
    // of the literal, see `indexFromPosition`.
    auto cellIndex = grid_->cellIndexFromWktLiteral(word);
    AD_CONTRACT_CHECK(
        !lastCellIndex_.has_value() || lastCellIndex_.value() <= cellIndex,
        "WKT literals were not passed to the GeoVocabulary in the order of "
        "their geo grid cells");
    lastCellIndex_ = cellIndex;
    index = grid_->indexFromCellAndPosition(cellIndex, numWords_);
  }

  // The `GeometryInfo` is computed and written by the thread pool, see
  // `flushBatch`.
  if (currentBatch_.empty()) {
    firstPositionOfCurrentBatch_ = numWords_;
  }
  currentBatch_.emplace_back(word);
  currentBatchSize_ += word.size();
  if (currentBatch_.size() >= GEO_WRITER_BATCH_NUM_WORDS ||
      currentBatchSize_ >= GEO_WRITER_BATCH_NUM_BYTES) {
    flushBatch();
  }
  ++numWords_;
  return index;
}

// ____________________________________________________________________________
template <typename V>
auto GeoVocabulary<V>::computeGeoInfoRecord(
    std::string_view word, std::atomic<size_t>& numInvalidGeometries,
    std::atomic<size_t>& numInvalidPolygonArea) -> GeometryInfoBuffer {
  auto info = GeometryInfo::fromWktLiteral(word);
  if (!info.has_value()) {
    ++numInvalidGeometries;
    return invalidGeoInfoBuffer;
  }
  if (!info.value().getMetricArea().isValid()) {
    ++numInvalidPolygonArea;
  }
  return absl::bit_cast<GeometryInfoBuffer>(info.value());
}

// ____________________________________________________________________________
template <typename V>
void GeoVocabulary<V>::WordWriter::flushBatch() {
  if (currentBatch_.empty()) {
    return;
  }
  // Bound the number of batches in flight (each holds its words in memory).
  // The oldest batches are typically long done, so this rarely waits.
  const size_t maxNumPendingBatches =
      2 * ad_utility::globalExecutorNumThreads();
  while (pendingBatches_.size() >= maxNumPendingBatches) {
    pendingBatches_.front().get();
    pendingBatches_.pop_front();
  }
  // Compute the `GeometryInfo` of every word of the batch and write it to its
  // position in the `geoInfoFile_`, or write a zero buffer of the same size
  // (indicating an invalid geometry), so that direct access by position stays
  // possible.
  //
  // NOTE: The positioned `File::write` is a `pwrite`, so the batches can write
  // concurrently.
  auto computeAndWrite = [this, words = std::move(currentBatch_),
                          firstPosition = firstPositionOfCurrentBatch_]() {
    // The records of the batch are contiguous in the file, so they are
    // written with a single call.
    std::vector<GeometryInfoBuffer> records;
    records.reserve(words.size());
    for (const auto& word : words) {
      records.push_back(computeGeoInfoRecord(word, numInvalidGeometries_,
                                             numInvalidPolygonArea_));
    }
    auto offset =
        static_cast<off_t>(geoInfoHeader + firstPosition * geoInfoOffset);
    geoInfoFile_.write(records.data(), records.size() * geoInfoOffset, offset);
  };
  pendingBatches_.push_back(
      boost::asio::post(ad_utility::globalExecutor(),
                        boost::asio::use_future(std::move(computeAndWrite))));
  currentBatch_.clear();
  currentBatchSize_ = 0;
}

// ____________________________________________________________________________
template <typename V>
void GeoVocabulary<V>::WordWriter::finishImpl() {
  // `WordWriterBase` ensures that this is not called twice and we thus do not
  // try to close the file handle twice
  underlyingWordWriter_->finish();
  // Wait for the batches on the thread pool (and rethrow their exceptions)
  // before the file is closed.
  flushBatch();
  for (auto& batch : pendingBatches_) {
    batch.get();
  }
  pendingBatches_.clear();
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
GeoVocabulary<V>::BlockWriter::BlockWriter(
    const V& vocabulary, const std::string& filename,
    std::optional<ad_utility::GeoCellGrid> grid)
    : underlyingWriter_{vocabulary.makeBlockWriterPtr(filename)},
      geoInfoFile_{getGeoInfoFilename(filename), "w"},
      grid_{grid} {
  // The header, with a positioned write, see `WordWriter::WordWriter`.
  geoInfoFile_.write(&ad_utility::GEOMETRY_INFO_VERSION, geoInfoHeader, 0);
}

// ____________________________________________________________________________
template <typename V>
uint64_t GeoVocabulary<V>::BlockWriter::indexOf(uint64_t position,
                                                std::string_view word) const {
  if (!grid_.has_value()) {
    return position;
  }
  // The same cell as `WordWriter::operator()` assigns, see there.
  return grid_->indexFromCellAndPosition(grid_->cellIndexFromWktLiteral(word),
                                         position);
}

// ____________________________________________________________________________
template <typename V>
void GeoVocabulary<V>::BlockWriter::precomputePayload(
    std::string_view word, ql::span<char> payload) const {
  AD_CONTRACT_CHECK(payload.size() == geoInfoOffset);
  ql::ranges::copy(
      computeGeoInfoRecord(word, numInvalidGeometries_, numInvalidPolygonArea_),
      payload.begin());
}

// ____________________________________________________________________________
template <typename V>
AppendBlock GeoVocabulary<V>::BlockWriter::prepare(WordBlock block) {
  AD_CONTRACT_CHECK(block.payloadSize_ == geoInfoOffset);
  const uint64_t firstPosition = block.firstPosition_;
  std::vector<GeometryInfoBuffer> records;
  // The cells of the first and the last word (only with a grid).
  std::optional<GeoCellGrid::CellIndex> firstCellIndex;
  std::optional<GeoCellGrid::CellIndex> lastCellIndex;
  // The records come precomputed with the block (see `precomputePayload`); with
  // a grid, check that the cells are non-decreasing within the block, see
  // `WordWriter::operator()`.
  records.reserve(block.numWords());
  for (size_t i = 0; i < block.numWords(); ++i) {
    std::string_view word = block.word(i);
    // NOTE: The size of the payload has been checked at the beginning.
    ql::ranges::copy(block.payload(i), records.emplace_back().begin());
    if (grid_.has_value()) {
      const auto& record = records.back();
      std::optional<GeometryInfo> info;
      if (record != invalidGeoInfoBuffer) {
        info = absl::bit_cast<GeometryInfo>(record);
      }
      auto cellIndex = cellIndexOfWord(grid_.value(), info, word);
      AD_CONTRACT_CHECK(
          !lastCellIndex.has_value() || lastCellIndex.value() <= cellIndex,
          "WKT literals were not passed to the GeoVocabulary in "
          "the order of their geo grid cells");
      if (!firstCellIndex.has_value()) {
        firstCellIndex = cellIndex;
      }
      lastCellIndex = cellIndex;
    }
  }
  return [this, firstPosition, records = std::move(records), firstCellIndex,
          lastCellIndex,
          appendUnderlying =
              underlyingWriter_->prepare(std::move(block))]() mutable {
    appendRecords(firstPosition, std::move(records), firstCellIndex,
                  lastCellIndex, std::move(appendUnderlying));
  };
}

// ____________________________________________________________________________
template <typename V>
void GeoVocabulary<V>::BlockWriter::appendRecords(
    uint64_t firstPosition, std::vector<GeometryInfoBuffer> records,
    std::optional<GeoCellGrid::CellIndex> firstCellIndex,
    std::optional<GeoCellGrid::CellIndex> lastCellIndex,
    AppendBlock appendUnderlying) {
  AD_CONTRACT_CHECK(firstPosition == numWords_);
  const size_t numWords = records.size();
  if (grid_.has_value() && numWords > 0) {
    // Keep one position free, see `WordWriter::operator()`.
    AD_CONTRACT_CHECK(numWords_ + numWords < grid_->maxNumWords(),
                      "Too many WKT literals for the configured geo cell "
                      "grid, please rebuild with a smaller grid level");
    AD_CONTRACT_CHECK(!lastCellIndex_.has_value() ||
                          lastCellIndex_.value() <= firstCellIndex.value(),
                      "WKT literals were not passed to the GeoVocabulary in "
                      "the order of their geo grid cells");
    lastCellIndex_ = lastCellIndex;
  }
  auto offset = static_cast<off_t>(geoInfoHeader + numWords_ * geoInfoOffset);
  numWords_ += numWords;
  std::move(appendUnderlying)();
  // The records are written by the pool, see `WordWriter::flushBatch` for
  // the bound on the writes in flight.
  const size_t maxNumPendingWrites = 2 * ad_utility::globalExecutorNumThreads();
  while (pendingWrites_.size() >= maxNumPendingWrites) {
    pendingWrites_.front().get();
    pendingWrites_.pop_front();
  }
  pendingWrites_.push_back(boost::asio::post(
      ad_utility::globalExecutor(),
      boost::asio::use_future([this, records = std::move(records), offset]() {
        geoInfoFile_.write(records.data(), records.size() * geoInfoOffset,
                           offset);
      })));
}

// ____________________________________________________________________________
template <typename V>
void GeoVocabulary<V>::BlockWriter::finishImpl() {
  underlyingWriter_->finish();
  // Wait for the writes on the pool (and rethrow their exceptions) before the
  // file is closed.
  for (auto& write : pendingWrites_) {
    write.get();
  }
  pendingWrites_.clear();
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
