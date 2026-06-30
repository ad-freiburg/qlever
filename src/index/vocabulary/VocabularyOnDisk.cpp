// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#include "index/vocabulary/VocabularyOnDisk.h"

#include <algorithm>
#include <array>
#include <deque>
#include <fstream>

#include "util/ExceptionHandling.h"
#include "util/Iterators.h"
#include "util/MmapVector.h"
#include "util/StringUtils.h"

using OffsetAndSize = VocabularyOnDisk::OffsetAndSize;

// ____________________________________________________________________________
OffsetAndSize VocabularyOnDisk::getOffsetAndSize(uint64_t i) const {
  AD_CORRECTNESS_CHECK(i < size());
  // Read the offset of the word at index `i` and the offset of the next word
  // (which marks the end of the word at index `i`) in a single `pread`.
  std::array<Offset, 2> offsets{};
  // Assert no unexpected padding.
  static_assert(sizeof(offsets) == sizeof(Offset) * 2);
  offsetsFile_.read(offsets.data(), sizeof(offsets),
                    static_cast<off_t>(i * sizeof(Offset)));
  return {offsets[0], offsets[1] - offsets[0]};
}

// _____________________________________________________________________________
std::string VocabularyOnDisk::operator[](uint64_t idx) const {
  AD_CONTRACT_CHECK(idx < size());
  auto offsetAndSize = getOffsetAndSize(idx);
  std::string result(offsetAndSize.size_, '\0');
  file_.read(result.data(), offsetAndSize.size_,
             static_cast<off_t>(offsetAndSize.offset_));
  return result;
}

// _____________________________________________________________________________
VocabBatchLookupResult VocabularyOnDisk::lookupBatch(
    ql::span<const size_t> indices) const {
  const size_t numIndices = indices.size();
  if (numIndices == 0) {
    auto data = std::make_shared<VocabBatchLookupData>();
    return VocabBatchLookupData::asResult(std::move(data));
  }

  // Phase 1: Read offset pairs via io_uring from the .offsets file.
  // For each index i, read offsets_[i] and offsets_[i+1] (16 bytes) at file
  // position i * sizeof(uint64_t).
  struct OffsetPair {
    uint64_t offset_;
    uint64_t nextOffset_;
  };
  std::vector<OffsetPair> offsetPairs(numIndices);
  std::vector<size_t> offsetSizes(numIndices, sizeof(OffsetPair));
  std::vector<uint64_t> offsetFileOffsets(numIndices);
  std::vector<char*> offsetTargets(numIndices);
  for (size_t i = 0; i < numIndices; ++i) {
    AD_CONTRACT_CHECK(indices[i] < size());
    offsetFileOffsets[i] = indices[i] * sizeof(uint64_t);
    offsetTargets[i] = reinterpret_cast<char*>(&offsetPairs[i]);
  }

  auto manager = ioManagers_->pop().value();
  auto offsetHandle = manager->addBatch(offsetsFile_.fd(), offsetSizes,
                                        offsetFileOffsets, offsetTargets);
  manager->wait(offsetHandle);

  // Compute string sizes and total buffer size.
  size_t totalSize = 0;
  for (size_t i = 0; i < numIndices; ++i) {
    totalSize += offsetPairs[i].nextOffset_ - offsetPairs[i].offset_;
  }

  // Phase 2: Read string data via io_uring (reusing the same manager).
  auto data = std::make_shared<VocabBatchLookupData>();
  data->buffer().resize(totalSize);
  data->views().resize(numIndices);

  std::vector<size_t> sizes(numIndices);
  std::vector<uint64_t> fileOffsets(numIndices);
  std::vector<char*> targetPointers(numIndices);
  {
    size_t bufferOffset = 0;
    for (size_t i = 0; i < numIndices; ++i) {
      sizes[i] = offsetPairs[i].nextOffset_ - offsetPairs[i].offset_;
      fileOffsets[i] = offsetPairs[i].offset_;
      targetPointers[i] = data->buffer().data() + bufferOffset;
      bufferOffset += sizes[i];
    }
  }

  auto stringHandle =
      manager->addBatch(file_.fd(), sizes, fileOffsets, targetPointers);
  manager->wait(stringHandle);
  ioManagers_->push(std::move(manager));

  // Build string_views pointing into the buffer.
  for (size_t i = 0; i < numIndices; ++i) {
    data->views()[i] = std::string_view(targetPointers[i], sizes[i]);
  }

  return VocabBatchLookupData::asResult(std::move(data));
}

// _____________________________________________________________________________
VocabLookupOutput VocabularyOnDisk::lookupBatchesStreamed(
    VocabLookupInput rangeOfIndexBatches) const {
  // The pipeline state lives in `PipelineState` and each call to the lambda
  // below produces the next result.

  // Constants for pipelining.
  static constexpr size_t kRingSize = 256;
  static constexpr size_t kPrefetchMultiplier = 3;
  static constexpr size_t kPrefetchThreshold = kPrefetchMultiplier * kRingSize;

  // Per-batch state for the pipeline.
  struct PipelineBatch {
    struct OffsetPair {
      uint64_t offset_;
      uint64_t nextOffset_;
    };
    std::vector<OffsetPair> offsetPairs_;
    ad_utility::BatchIoManager::BatchHandle phase1Handle_;

    std::shared_ptr<VocabBatchLookupData> resultData_;
    std::vector<size_t> targetBufferSizes_;
    std::vector<char*> targetBuffers_;
    ad_utility::BatchIoManager::BatchHandle phase2Handle_;

    size_t numIndices_;
    enum class Stage { PHASE1_SUBMITTED, PHASE2_SUBMITTED };
    Stage stage_;

    static PipelineBatch submitOffsetReads(
        ql::span<const size_t> indices, const VocabularyOnDisk& self,
        ad_utility::BatchIoManager& manager) {
      const size_t numIndices = indices.size();
      PipelineBatch batch;
      batch.numIndices_ = numIndices;
      batch.offsetPairs_.resize(numIndices);

      std::vector<size_t> offsetSizes(numIndices, sizeof(OffsetPair));
      std::vector<uint64_t> offsets(numIndices);
      std::vector<char*> targetBuffers(numIndices);

      for (size_t i = 0; i < numIndices; ++i) {
        AD_CONTRACT_CHECK(indices[i] < self.size());
        offsets[i] = indices[i] * sizeof(uint64_t);
        targetBuffers[i] = reinterpret_cast<char*>(&batch.offsetPairs_[i]);
      }

      batch.phase1Handle_ = manager.addBatch(
          self.offsetsFile_.fd(), offsetSizes, offsets, targetBuffers);
      batch.stage_ = Stage::PHASE1_SUBMITTED;
      return batch;
    }

    // Wait for this batch's phase-1 (offset) reads, then allocate the result
    // buffer and submit its phase-2 (string) reads from the main file.
    // Precondition: the batch is in stage `PHASE1_SUBMITTED`;
    // afterward it is in stage `PHASE2_SUBMITTED`.
    void submitStringReads(const VocabularyOnDisk& self,
                           ad_utility::BatchIoManager& manager) {
      manager.wait(phase1Handle_);

      // compute how big a single buffer must be to hold every word's bytes
      // back-to-back.
      size_t combinedBufferSize = 0;
      for (size_t i = 0; i < numIndices_; ++i) {
        combinedBufferSize +=
            offsetPairs_[i].nextOffset_ - offsetPairs_[i].offset_;
      }

      resultData_ = std::make_shared<VocabBatchLookupData>();
      resultData_->buffer().resize(combinedBufferSize);
      resultData_->views().resize(numIndices_);

      targetBufferSizes_.resize(numIndices_);
      targetBuffers_.resize(numIndices_);
      std::vector<uint64_t> fileOffsets(numIndices_);
      size_t bufferOffset = 0;
      for (size_t i = 0; i < numIndices_; ++i) {
        targetBufferSizes_[i] =
            offsetPairs_[i].nextOffset_ - offsetPairs_[i].offset_;
        fileOffsets[i] = offsetPairs_[i].offset_;
        targetBuffers_[i] = resultData_->buffer().data() + bufferOffset;
        bufferOffset += targetBufferSizes_[i];
      }

      phase2Handle_ = manager.addBatch(self.file_.fd(), targetBufferSizes_,
                                       fileOffsets, targetBuffers_);
      stage_ = Stage::PHASE2_SUBMITTED;
    }

    // Wait for this batch's phase-2 (string) reads, build the result
    // `string_view`s into the buffer, and hand out the result. Precondition:
    // the batch is in stage `PHASE2_SUBMITTED`. Leaves `resultData_`
    // moved-from.
    VocabBatchLookupResult waitAndFinalize(
        ad_utility::BatchIoManager& manager) {
      AD_CORRECTNESS_CHECK(stage_ == Stage::PHASE2_SUBMITTED);
      manager.wait(phase2Handle_);
      for (size_t i = 0; i < numIndices_; ++i) {
        resultData_->views()[i] =
            std::string_view(targetBuffers_[i], targetBufferSizes_[i]);
      }
      return VocabBatchLookupData::asResult(std::move(resultData_));
    }
  };

  // The state of the pipeline. On destruction, wait for all in-flight I/O
  // and return the manager to the pool, even if the range is destroyed
  // mid-iteration.
  struct PipelineState {
    const VocabularyOnDisk* self_;
    VocabLookupInput input_;
    std::unique_ptr<ad_utility::BatchIoManager> manager_;
    std::deque<PipelineBatch> pipeline_;
    size_t totalSubmittedSQEs_ = 0;
    bool inputExhausted_ = false;

    PipelineState(const VocabularyOnDisk* self, VocabLookupInput input)
        : self_{self},
          input_{std::move(input)},
          manager_{self->ioManagers_->pop().value()} {}

    ~PipelineState() {
      ad_utility::terminateIfThrows(
          [this]() {
            for (const auto& b : pipeline_) {
              if (b.stage_ == PipelineBatch::Stage::PHASE1_SUBMITTED) {
                manager_->wait(b.phase1Handle_);
              }
              if (b.stage_ == PipelineBatch::Stage::PHASE2_SUBMITTED) {
                manager_->wait(b.phase2Handle_);
              }
            }
            self_->ioManagers_->push(std::move(manager_));
          },
          "Error while draining the I/O pipeline in ~PipelineState");
    }

    // Produce the next result batch, or `std::nullopt` when the stream is
    // exhausted. Each call tops up the in-flight read window (FILL), promotes
    // ready batches to their string reads (ADVANCE), and returns the front
    // batch's result (YIELD).
    std::optional<VocabBatchLookupResult> getNextResult() {
      if (auto emptyResult = fillPipeline()) {
        return emptyResult;
      }
      if (pipeline_.empty()) {
        return std::nullopt;
      }
      advanceFrontBatches();
      return yieldFront();
    }

   private:
    // FILL: submit phase-1 (offset) reads for new batches until the prefetch
    // threshold is reached or the input is exhausted. Returns an empty result
    // to be yielded immediately if an empty input batch is encountered,
    // otherwise `std::nullopt`.
    std::optional<VocabBatchLookupResult> fillPipeline() {
      while (!inputExhausted_ && totalSubmittedSQEs_ < kPrefetchThreshold) {
        auto batchOfIndices = input_.get();
        if (!batchOfIndices.has_value()) {
          inputExhausted_ = true;
          break;
        }
        auto indices = std::move(batchOfIndices.value());
        AD_CONTRACT_CHECK(!indices.empty());
        auto batch =
            PipelineBatch::submitOffsetReads(indices, *self_, *manager_);
        totalSubmittedSQEs_ += batch.numIndices_;
        pipeline_.push_back(std::move(batch));
      }
      return std::nullopt;
    }

    // ADVANCE: promote consecutive PHASE1_SUBMITTED batches at the front of
    // `pipeline_` to PHASE2_SUBMITTED by submitting their string reads.
    void advanceFrontBatches() {
      while (!pipeline_.empty() && pipeline_.front().stage_ ==
                                       PipelineBatch::Stage::PHASE1_SUBMITTED) {
        auto& batch = pipeline_.front();
        batch.submitStringReads(*self_, *manager_);
        totalSubmittedSQEs_ += batch.numIndices_;
      }
    }

    // YIELD: wait for the front batch's phase-2 reads and return its result.
    VocabBatchLookupResult yieldFront() {
      AD_CORRECTNESS_CHECK(!pipeline_.empty());
      auto& front = pipeline_.front();
      auto result = front.waitAndFinalize(*manager_);
      totalSubmittedSQEs_ -= 2 * front.numIndices_;
      pipeline_.pop_front();
      return result;
    }
  };

  auto pipelineState =
      std::make_shared<PipelineState>(this, std::move(rangeOfIndexBatches));

  auto get = [pipelineState = std::move(pipelineState)]() mutable {
    return pipelineState->getNextResult();
  };

  return VocabLookupOutput{
      ad_utility::InputRangeFromGetCallable(std::move(get))};
}

// _____________________________________________________________________________
VocabularyOnDisk::WordWriter::WordWriter(const std::string& outFilename)
    : file_{outFilename, "w"},
      offsetsFile_{absl::StrCat(outFilename, offsetSuffix_), "w"} {}

// _____________________________________________________________________________
uint64_t VocabularyOnDisk::WordWriter::operator()(
    std::string_view word, [[maybe_unused]] bool isExternalDummy) {
  offsetsFile_.write(&currentOffset_, sizeof(currentOffset_));
  currentOffset_ += file_.write(word.data(), word.size());
  return numWords_++;
}

// _____________________________________________________________________________
void VocabularyOnDisk::WordWriter::finishImpl() {
  // End offset of last vocabulary entry, also consistent with the empty
  // vocabulary.
  offsetsFile_.write(&currentOffset_, sizeof(currentOffset_));
  ++numWords_;
  // Write the `MmapVectorMetaData` trailer that older vocabulary files also
  // used. Only the `size_` field is read by `VocabularyOnDisk` (`capacity_`
  // and `bytesize_` are MmapVector-internal and unused here), but we keep
  // the full struct so that older binaries can also read these new files.
  ad_utility::MmapVectorMetaData{numWords_, numWords_,
                                 numWords_ * sizeof(uint64_t)}
      .writeToFile(offsetsFile_);
  file_.close();
  offsetsFile_.close();
}

// _____________________________________________________________________________
VocabularyOnDisk::WordWriter::~WordWriter() {
  if (!finishWasCalled()) {
    ad_utility::terminateIfThrows([this]() { this->finish(); },
                                  "Calling `finish` from the destructor of "
                                  "`VocabularyOnDisk::WordWriter`");
  }
}

// _____________________________________________________________________________
void VocabularyOnDisk::open(const std::string& filename) {
  file_.open(filename, "r");
  offsetsFile_.open(filename + offsetSuffix_, "r");
  // Disable readahead for random lookups on both files. `posix_fadvise` is a
  // Linux/POSIX-advisory call that is not available on macOS; skipping it there
  // is harmless (it is only a performance hint).
#ifdef POSIX_FADV_RANDOM
  posix_fadvise(file_.fd(), 0, 0, POSIX_FADV_RANDOM);
  posix_fadvise(offsetsFile_.fd(), 0, 0, POSIX_FADV_RANDOM);
#endif

  // Read the offset count from the `MmapVectorMetaData` trailer, which is
  // the canonical layout used by both old and new vocabulary files.
  uint64_t numOffsets =
      ad_utility::MmapVectorMetaData::readFromFile(offsetsFile_).size_;
  AD_CORRECTNESS_CHECK(numOffsets > 0);
  size_ = numOffsets - 1;

  // Initialize pool of persistent `BatchIoManager`s for `lookupBatch`.
  static constexpr size_t kNumManagers = 8;
  ioManagers_ = std::make_unique<ad_utility::data_structures::ThreadSafeQueue<
      std::unique_ptr<ad_utility::BatchIoManager>>>(kNumManagers);
  for (size_t i = 0; i < kNumManagers; ++i) {
    ioManagers_->push(std::make_unique<ad_utility::BatchIoManager>());
  }
}
