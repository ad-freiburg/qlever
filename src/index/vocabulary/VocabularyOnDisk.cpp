// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#include "index/vocabulary/VocabularyOnDisk.h"

#include <algorithm>
#include <array>
#include <deque>
#include <fstream>

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
  const size_t n = indices.size();
  if (n == 0) {
    auto data = std::make_shared<VocabBatchLookupData>();
    return VocabBatchLookupData::asResult(std::move(data));
  }

  // Phase 1: Read offset pairs via io_uring from the .offsets file.
  // For each index i, read offsets_[i] and offsets_[i+1] (16 bytes) at file
  // position i * sizeof(uint64_t).
  struct OffsetPair {
    uint64_t offset;
    uint64_t nextOffset;
  };
  std::vector<OffsetPair> offsetPairs(n);
  std::vector<size_t> offsetSizes(n, sizeof(OffsetPair));
  std::vector<uint64_t> offsetFileOffsets(n);
  std::vector<char*> offsetTargets(n);
  for (size_t i = 0; i < n; ++i) {
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
  for (size_t i = 0; i < n; ++i) {
    totalSize += offsetPairs[i].nextOffset - offsetPairs[i].offset;
  }

  // Phase 2: Read string data via io_uring (reusing the same manager).
  auto data = std::make_shared<VocabBatchLookupData>();
  data->buffer.resize(totalSize);
  data->views.resize(n);

  std::vector<size_t> sizes(n);
  std::vector<uint64_t> fileOffsets(n);
  std::vector<char*> targetPointers(n);
  {
    size_t bufferOffset = 0;
    for (size_t i = 0; i < n; ++i) {
      sizes[i] = offsetPairs[i].nextOffset - offsetPairs[i].offset;
      fileOffsets[i] = offsetPairs[i].offset;
      targetPointers[i] = data->buffer.data() + bufferOffset;
      bufferOffset += sizes[i];
    }
  }

  auto stringHandle =
      manager->addBatch(file_.fd(), sizes, fileOffsets, targetPointers);
  manager->wait(stringHandle);
  ioManagers_->push(std::move(manager));

  // Build string_views pointing into the buffer.
  for (size_t i = 0; i < n; ++i) {
    data->views[i] = std::string_view(targetPointers[i], sizes[i]);
  }

  return VocabBatchLookupData::asResult(std::move(data));
}

// _____________________________________________________________________________
VocabLookupOutput VocabularyOnDisk::lookupBatchesStreamed(
    VocabLookupInput rangeOfRowIndexBatches) const {
  // NOTE: Not implemented as a coroutine, because coroutines are not
  // available in the C++17 compatibility build. Instead, the pipeline state
  // lives in `PipelineState` and each call to the lambda below produces the
  // next result (each `return` corresponds to a `co_yield` of the former
  // coroutine implementation).

  // Constants for pipelining.
  static constexpr size_t kRingSize = 256;
  static constexpr size_t kPrefetchMultiplier = 3;
  static constexpr size_t kPrefetchThreshold = kPrefetchMultiplier * kRingSize;

  // Per-batch state for the pipeline.
  struct PipelineBatch {
    struct OffsetPair {
      uint64_t offset;
      uint64_t nextOffset;
    };
    std::vector<OffsetPair> offsetPairs;
    ad_utility::BatchIoManager::BatchHandle phase1Handle;

    std::shared_ptr<VocabBatchLookupData> data;
    std::vector<size_t> sizes;
    std::vector<char*> targetBuffers;
    ad_utility::BatchIoManager::BatchHandle phase2Handle;

    size_t numIndices;
    enum Stage { PHASE1_SUBMITTED, PHASE2_SUBMITTED } stage;
  };

  // The state of the pipeline. On destruction, wait for all in-flight I/O
  // and return the manager to the pool, even if the range is destroyed
  // mid-iteration.
  struct PipelineState {
    const VocabularyOnDisk* self_;
    std::unique_ptr<ad_utility::BatchIoManager> manager_;
    std::deque<PipelineBatch> pipeline_;
    size_t totalSubmittedSQEs_ = 0;
    bool inputExhausted_ = false;

    explicit PipelineState(const VocabularyOnDisk* self)
        : self_{self}, manager_{self->ioManagers_->pop().value()} {}

    ~PipelineState() {
      for (auto& b : pipeline_) {
        if (b.stage == PipelineBatch::PHASE1_SUBMITTED) {
          manager_->wait(b.phase1Handle);
        }
        if (b.stage == PipelineBatch::PHASE2_SUBMITTED) {
          manager_->wait(b.phase2Handle);
        }
      }
      self_->ioManagers_->push(std::move(manager_));
    }
  };

  auto pipelineState = std::make_shared<PipelineState>(this);

  auto get = [pipelineState = std::move(pipelineState),
              rangeOfRowIndexBatches =
                  std::move(rangeOfRowIndexBatches)]() mutable
      -> std::optional<VocabBatchLookupResult> {
    const auto* self = pipelineState->self_;
    auto& manager = pipelineState->manager_;
    auto& pipeline = pipelineState->pipeline_;
    auto& totalSubmittedSQEs = pipelineState->totalSubmittedSQEs_;

    // FILL: submit phase-1 (offset reads) for new batches until threshold
    // or input exhausted.
    while (!pipelineState->inputExhausted_ &&
           totalSubmittedSQEs < kPrefetchThreshold) {
      auto indexBatch = rangeOfRowIndexBatches.get();
      if (!indexBatch.has_value()) {
        pipelineState->inputExhausted_ = true;
        break;
      }
      auto indices = std::move(indexBatch.value());

      if (indices.empty()) {
        auto emptyData = std::make_shared<VocabBatchLookupData>();
        return VocabBatchLookupData::asResult(std::move(emptyData));
      }

      const size_t numIndicesInBatch = indices.size();
      PipelineBatch batch;
      batch.numIndices = numIndicesInBatch;
      batch.offsetPairs.resize(numIndicesInBatch);

      std::vector<size_t> offsetSizes(numIndicesInBatch,
                                      sizeof(PipelineBatch::OffsetPair));
      std::vector<uint64_t> offsetFileOffsets(numIndicesInBatch);
      std::vector<char*> targetBufferForOffsetValues(numIndicesInBatch);

      for (size_t i = 0; i < numIndicesInBatch; ++i) {
        AD_CONTRACT_CHECK(indices[i] < self->size());
        offsetFileOffsets[i] = indices[i] * sizeof(uint64_t);
        targetBufferForOffsetValues[i] =
            reinterpret_cast<char*>(&batch.offsetPairs[i]);
      }

      batch.phase1Handle =
          manager->addBatch(self->offsetsFile_.fd(), offsetSizes,
                            offsetFileOffsets, targetBufferForOffsetValues);
      batch.stage = PipelineBatch::PHASE1_SUBMITTED;
      totalSubmittedSQEs += numIndicesInBatch;
      pipeline.push_back(std::move(batch));
    }

    if (pipeline.empty()) {
      return std::nullopt;
    }

    // ADVANCE: move consecutive PHASE1_SUBMITTED batches at the front to
    // PHASE2_SUBMITTED by waiting for their offset reads and submitting
    // string reads.
    while (!pipeline.empty() &&
           pipeline.front().stage == PipelineBatch::PHASE1_SUBMITTED) {
      auto& batch = pipeline.front();
      manager->wait(batch.phase1Handle);

      const size_t numIndicesInBatch = batch.numIndices;
      size_t totalSize = 0;
      for (size_t i = 0; i < numIndicesInBatch; ++i) {
        totalSize +=
            batch.offsetPairs[i].nextOffset - batch.offsetPairs[i].offset;
      }

      batch.data = std::make_shared<VocabBatchLookupData>();
      batch.data->buffer.resize(totalSize);
      batch.data->views.resize(numIndicesInBatch);

      batch.sizes.resize(numIndicesInBatch);
      batch.targetBuffers.resize(numIndicesInBatch);
      std::vector<uint64_t> fileOffsets(numIndicesInBatch);
      {
        size_t bufferOffset = 0;
        for (size_t i = 0; i < numIndicesInBatch; ++i) {
          batch.sizes[i] =
              batch.offsetPairs[i].nextOffset - batch.offsetPairs[i].offset;
          fileOffsets[i] = batch.offsetPairs[i].offset;
          batch.targetBuffers[i] = batch.data->buffer.data() + bufferOffset;
          bufferOffset += batch.sizes[i];
        }
      }

      batch.phase2Handle = manager->addBatch(self->file_.fd(), batch.sizes,
                                             fileOffsets, batch.targetBuffers);
      batch.stage = PipelineBatch::PHASE2_SUBMITTED;
      totalSubmittedSQEs += numIndicesInBatch;
    }

    // YIELD: wait for front batch's phase-2, return result.
    AD_CORRECTNESS_CHECK(!pipeline.empty());
    auto& front = pipeline.front();
    AD_CORRECTNESS_CHECK(front.stage == PipelineBatch::PHASE2_SUBMITTED);
    manager->wait(front.phase2Handle);

    const size_t n = front.numIndices;
    for (size_t i = 0; i < n; ++i) {
      front.data->views[i] =
          std::string_view(front.targetBuffers[i], front.sizes[i]);
    }
    auto result = VocabBatchLookupData::asResult(std::move(front.data));

    totalSubmittedSQEs -= 2 * front.numIndices;
    pipeline.pop_front();
    return result;
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

  // Initialize pool of persistent BatchIoManagers for lookupBatch.
  static constexpr size_t kNumManagers = 8;
  ioManagers_ = std::make_unique<ad_utility::data_structures::ThreadSafeQueue<
      std::unique_ptr<ad_utility::BatchIoManager>>>(kNumManagers);
  for (size_t i = 0; i < kNumManagers; ++i) {
    ioManagers_->push(std::make_unique<ad_utility::BatchIoManager>());
  }
}
