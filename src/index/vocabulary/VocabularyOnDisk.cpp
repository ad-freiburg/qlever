// Copyright 2022 - 2026, The QLever Authors, in particular:
//
// 2022 - 2026 Johannes Kalmbach <johannes.kalmbach@gmail.com>, UFR
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/vocabulary/VocabularyOnDisk.h"

#include <absl/cleanup/cleanup.h>
#include <absl/functional/bind_front.h>

#include <algorithm>
#include <array>
#include <optional>

#include "global/Constants.h"
#include "global/RuntimeParameters.h"
#include "util/AdaptiveBatchController.h"
#include "util/ExceptionHandling.h"
#include "util/InputRangeUtils.h"
#include "util/Iterators.h"
#include "util/MmapVector.h"
#include "util/StringUtils.h"
#include "util/Views.h"

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

namespace {
// Given the `offsets` of a chunk of words and a starting position `first`
// within that chunk, return how many words starting at `first` can be read into
// a single data buffer without their combined size exceeding
// `VOCABULARY_SCAN_MAX_WORD_DATA_PER_BATCH`, but always at least one word, even
// if that single word is larger than the limit (a word must not be split).
size_t numWordsWithinLimit(ql::span<const uint64_t> offsets, size_t first) {
  // Common case: all remaining words of the chunk fit. Checking this first
  // (i.e. looking at the end right away) avoids a search in the expected case
  // where a whole batch of words comfortably fits within the limit.
  if (offsets.back() - offsets[first] <=
      VOCABULARY_SCAN_MAX_WORD_DATA_PER_BATCH.getBytes()) {
    return offsets.size() - 1 - first;
  }
  // Otherwise binary-search for the largest prefix that fits. `offsets` is
  // ascending, so the number of words that fit is the number of offsets in
  // `[first, total]` that are `<= offsets[first] + limit`, minus one.
  uint64_t threshold =
      offsets[first] + VOCABULARY_SCAN_MAX_WORD_DATA_PER_BATCH.getBytes();
  auto begin = offsets.begin() + first;
  auto it = std::upper_bound(begin, offsets.end(), threshold);
  size_t numFit = static_cast<size_t>(it - begin) - 1;
  return std::max<size_t>(numFit, 1);
}

// Turn the offsets of a chunk of words into an input range of sub-chunks, where
// each sub-chunk contains the offsets of a maximal number of words that can be
// read into a single data buffer without exceeding
// `VOCABULARY_SCAN_MAX_WORD_DATA_PER_BATCH`, but always at least one word, even
// if that single word is larger than the limit (a word must not be split).
auto chunkOffsets(ql::span<const uint64_t> offsets) {
  AD_CORRECTNESS_CHECK(!offsets.empty());
  return ad_utility::InputRangeFromGetCallable{
      [offsets,
       start = size_t{0}]() mutable -> std::optional<ql::span<const uint64_t>> {
        if (start >= offsets.size() - 1) {
          return std::nullopt;
        }
        size_t numWords = numWordsWithinLimit(offsets, start);
        size_t oldBegin = std::exchange(start, start + numWords);
        return offsets.subspan(oldBegin, numWords + 1);
      }};
}

// Map a chunk of offsets to an input range of string views, where each string
// view corresponds to a word in the chunk. The string views are backed by the
// given `data` buffer, which must contain the concatenated string data of all
// words in the chunk, starting at the offset of the first word in the chunk.
auto mapOffsetsToStringViews(ql::span<const uint64_t> offsets,
                             std::string_view data) {
  AD_CORRECTNESS_CHECK(!offsets.empty());
  auto initialStart = offsets.front();
  // `std::views::sliding` is not available in C++20, and also not in range-v3,
  // so we use `zip` with a dropped view instead, which is equivalent to a
  // sliding view of size 2.
  return ::ranges::views::zip(offsets, offsets | ::ranges::views::drop(1)) |
         ql::views::transform([data, initialStart](const auto& pair) {
           auto [begin, end] = pair;
           return std::string_view{data.data() + begin - initialStart,
                                   end - begin};
         });
}
}  // namespace

// _____________________________________________________________________________
auto VocabularyOnDisk::chunkToWords(ql::span<const uint64_t> offsets) const {
  return ad_utility::allView(ad_utility::CachingTransformInputRange{
             chunkOffsets(offsets),
             [this, data = std::string{}](
                 ql::span<const uint64_t> subOffsets) mutable {
               data.resize(subOffsets.back() - subOffsets.front());
               file_.read(data.data(), data.size(),
                          static_cast<off_t>(subOffsets.front()));
               return mapOffsetsToStringViews(subOffsets, data);
             }}) |
         ql::views::join;
}

// _____________________________________________________________________________
auto VocabularyOnDisk::readOffsetsInBatches() const {
  // For each batch, read the offsets of its words from disk to memory. We read
  // one extra offset that marks the end of the last word in the batch.
  return ad_utility::CachingTransformInputRange{
      ::ranges::views::stride(::ranges::views::iota(size_t{0}, size()),
                              VOCABULARY_SCAN_MAX_WORDS_PER_BATCH),
      [this,
       buffer =
           std::array<uint64_t, VOCABULARY_SCAN_MAX_WORDS_PER_BATCH + 1>{}](
          size_t chunkStart) mutable {
        size_t numWords = std::min<size_t>(VOCABULARY_SCAN_MAX_WORDS_PER_BATCH,
                                           size() - chunkStart);
        size_t numWordsPlusOne = numWords + 1;
        offsetsFile_.read(buffer.data(), numWordsPlusOne * sizeof(uint64_t),
                          static_cast<off_t>(chunkStart * sizeof(uint64_t)));
        return ql::span<const uint64_t>{buffer.data(), numWordsPlusOne};
      }};
}

// _____________________________________________________________________________
VocabularyScanRange VocabularyOnDisk::scanAll() const {
  // Range of all words in the vocabulary.
  auto words = ad_utility::OwningView{readOffsetsInBatches()} |
               ql::views::transform(
                   absl::bind_front(&VocabularyOnDisk::chunkToWords, this)) |
               ql::views::join;
  // Pair each word with its index in the vocabulary.
  return VocabularyScanRange{ad_utility::CachingTransformInputRange{
      std::move(words), [index = uint64_t{0}](std::string_view word) mutable {
        return IndexAndWord{index++, word};
      }}};
}

// _____________________________________________________________________________
std::optional<ad_utility::BatchManagerBase::BatchHandle>
VocabularyOnDisk::submitThroughManager(ad_utility::BatchManagerBase& manager,
                                       int fd, ql::span<const size_t> numBytes,
                                       ql::span<const uint64_t> offsets,
                                       ql::span<char*> buffers,
                                       ql::span<const size_t> positions) {
  if (positions.empty()) {
    return std::nullopt;
  }
  auto select = [&positions](auto values) {
    return ::ranges::to_vector(
        positions |
        ql::views::transform([&values](size_t i) { return values[i]; }));
  };
  auto selectedNumBytes = select(numBytes);
  auto selectedOffsets = select(offsets);
  auto selectedBuffers = select(buffers);
  return manager.addBatch(fd, selectedNumBytes, selectedOffsets,
                          selectedBuffers);
}

// _____________________________________________________________________________
void VocabularyOnDisk::readThroughManager(ad_utility::BatchManagerBase& manager,
                                          int fd,
                                          ql::span<const size_t> numBytes,
                                          ql::span<const uint64_t> offsets,
                                          ql::span<char*> buffers,
                                          ql::span<const size_t> positions) {
  auto batch =
      submitThroughManager(manager, fd, numBytes, offsets, buffers, positions);
  if (batch.has_value()) {
    manager.wait(batch.value());
  }
}

// _____________________________________________________________________________
VocabBatchLookupResult VocabularyOnDisk::readStrings(
    ad_utility::BatchManagerBase& manager,
    ql::span<const OffsetPair> offsetPairs, bool pageCacheFastPath) const {
  // Read the string data. String `i` starts at `offset_` with length
  // `nextOffset_ - offset_`; the strings are packed contiguously into the
  // builder's buffer, with one precomputed view per word at its fixed offset.
  const size_t numIndices = offsetPairs.size();
  std::vector<size_t> sizes(numIndices);
  std::vector<uint64_t> fileOffsets(numIndices);
  for (auto&& [size, fileOffset, offsetPair] :
       ::ranges::views::zip(sizes, fileOffsets, offsetPairs)) {
    size = offsetPair.wordSize();
    fileOffset = offsetPair.offset();
  }

  // `lookupBatch` rejects empty input, so `sizes` is non-empty here, as the
  // builder requires.
  AD_CORRECTNESS_CHECK(!sizes.empty());
  ContiguousVocabBatchBuilder builder(sizes);
  // Bind the returned array: `addBatch` takes a span, and the pointers must
  // stay alive until `wait` returns.
  auto targets = builder.targets();
  ql::span<char*> targetSpan{targets};
  if (pageCacheFastPath) {
    auto missed = ad_utility::readPageCacheHits(file_.fd(), sizes, fileOffsets,
                                                targetSpan);
    readThroughManager(manager, file_.fd(), sizes, fileOffsets, targetSpan,
                       missed);
  } else {
    manager.wait(manager.addBatch(file_.fd(), sizes, fileOffsets, targetSpan));
  }
  return std::move(builder).finalize();
}

// _____________________________________________________________________________
std::unique_ptr<VocabLookupHandleBase> VocabularyOnDisk::beginLookup(
    ql::span<const size_t> indices) const {
  AD_CONTRACT_CHECK(!indices.empty());

  auto handle = std::make_unique<LookupHandle>();
  handle->vocab_ = this;
  // Take a pooled `IoManager` into the handle as its sole owner immediately,
  // before any code that can throw. From here on the handle's destructor is the
  // only code that returns the manager to the pool, so it is returned on every
  // exit path (including exceptions such as an out-of-range index) without a
  // separate cleanup that could double-return it.
  handle->manager_ = ioManagers_->acquire();
  handle->indices_.assign(indices.begin(), indices.end());

  // Submit the offset reads (Phase 1) without waiting for them: the caller
  // decides when to block (in `finishLookup`), which is what allows the reads
  // of the next batch to be in flight while the current batch is consumed.
  const size_t numIndices = handle->indices_.size();
  handle->offsetPairs_.resize(numIndices);
  std::vector sizes(numIndices, sizeof(OffsetPair));
  std::vector<uint64_t> fileOffsets(numIndices);
  std::vector<char*> targets(numIndices);
  for (auto&& [fileOffset, index, target, offsetPair] : ::ranges::views::zip(
           fileOffsets, handle->indices_, targets, handle->offsetPairs_)) {
    AD_CONTRACT_CHECK(index < size());
    fileOffset = index * sizeof(uint64_t);
    target = reinterpret_cast<char*>(&offsetPair);
  }
  handle->pageCacheFastPath_ =
      getRuntimeParameter<
          &RuntimeParameters::vocabularyIouringPageCacheFastPath_>() &&
      ad_utility::pageCacheFastPathIsSupported();
  if (!handle->pageCacheFastPath_) {
    handle->offsetBatch_ = handle->manager_->addBatch(offsetsFile_.fd(), sizes,
                                                      fileOffsets, targets);
    return handle;
  }

  // Page-cache fast path: `preadv2(RWF_NOWAIT)` never blocks, so it is done
  // right here. The pairs of consecutive indices overlap in the file, so read
  // each run of consecutive indices `[runBegins[r], runBegins[r + 1])` as one
  // range of `runLength + 1` offsets into `runOffsets`.
  const auto& indicesRef = handle->indices_;
  std::vector<size_t> runBegins{0};
  for (size_t i = 1; i < numIndices; ++i) {
    if (indicesRef[i] != indicesRef[i - 1] + 1) {
      runBegins.push_back(i);
    }
  }
  runBegins.push_back(numIndices);
  const size_t numRuns = runBegins.size() - 1;
  std::vector<uint64_t> runOffsets(numIndices + numRuns);
  std::vector<size_t> runSizes(numRuns);
  std::vector<uint64_t> runFileOffsets(numRuns);
  std::vector<char*> runTargets(numRuns);
  for (size_t run = 0; run < numRuns; ++run) {
    const size_t begin = runBegins[run];
    const size_t length = runBegins[run + 1] - begin;
    runSizes[run] = (length + 1) * sizeof(uint64_t);
    runFileOffsets[run] = fileOffsets[begin];
    runTargets[run] = reinterpret_cast<char*>(runOffsets.data() + begin + run);
  }
  auto missedRuns = ad_utility::readPageCacheHits(offsetsFile_.fd(), runSizes,
                                                  runFileOffsets, runTargets);

  // Fill the pairs of the served runs, and submit the pairs of the missed runs
  // to the `manager_` (without waiting, like the path without the fast path).
  std::vector<size_t> missedPositions;
  auto missedRun = missedRuns.begin();
  for (size_t run = 0; run < numRuns; ++run) {
    const size_t begin = runBegins[run];
    const size_t end = runBegins[run + 1];
    if (missedRun != missedRuns.end() && *missedRun == run) {
      ++missedRun;
      for (size_t i = begin; i < end; ++i) {
        missedPositions.push_back(i);
      }
      continue;
    }
    const uint64_t* runStart = runOffsets.data() + begin + run;
    for (size_t i = begin; i < end; ++i) {
      handle->offsetPairs_[i] =
          OffsetPair{runStart[i - begin], runStart[i - begin + 1]};
    }
  }
  handle->offsetBatch_ =
      submitThroughManager(*handle->manager_, offsetsFile_.fd(), sizes,
                           fileOffsets, targets, missedPositions);
  return handle;
}

// _____________________________________________________________________________
VocabBatchLookupResult VocabularyOnDisk::finishLookup(
    std::unique_ptr<VocabLookupHandleBase> handleBase) const {
  AD_CONTRACT_CHECK(handleBase != nullptr);
  return handleBase->finish();
}

// _____________________________________________________________________________
VocabBatchLookupResult VocabularyOnDisk::LookupHandle::finish() {
  // Complete the lookup and hand the `manager` back to the pool on every exit
  // path (including exceptions, e.g. an I/O error while waiting). The handle
  // itself (and not `VocabularyOnDisk::finishLookup`) owns the return, because
  // wrapping vocabularies (e.g. `CompressedVocabulary`) complete the lookup
  // through the type-erased `VocabLookupHandleBase` interface, where the
  // concrete `VocabularyOnDisk::finishLookup` cannot be called.
  absl::Cleanup returnManager{[this]() {
    ad_utility::terminateIfThrows([this]() { returnManagerToPool(); },
                                  "returning the `IoManager` to the pool in "
                                  "`VocabularyOnDisk::LookupHandle::finish`");
  }};
  // Wait for the offset reads submitted by `beginLookup`, then read the string
  // data (Phase 2) and return it.
  // With the page-cache fast path, all offset pairs may already have been
  // read by `beginLookup`, in which case no batch was submitted.
  if (offsetBatch_.has_value()) {
    manager_->wait(offsetBatch_.value());
  }
  return vocab_->readStrings(*manager_, offsetPairs_, pageCacheFastPath_);
}

// _____________________________________________________________________________
VocabularyOnDisk::LookupHandle::~LookupHandle() {
  // If `finish` was never called (e.g. an exception between `beginLookup` and
  // `finishLookup`), drain the offset reads first. Those reads target
  // `offsetPairs_`, which dies with this handle. Returning a busy manager
  // would let the next pool owner reuse the ring while the kernel still
  // writes into freed memory.
  if (manager_) {
    ad_utility::terminateIfThrows(
        [this]() {
          if (offsetBatch_.has_value()) {
            manager_->wait(offsetBatch_.value());
          }
          returnManagerToPool();
        },
        "draining in-flight offset reads and returning the `IoManager` in "
        "the destructor of `VocabularyOnDisk::LookupHandle`");
  }
}

// _____________________________________________________________________________
void VocabularyOnDisk::LookupHandle::returnManagerToPool() {
  vocab_->ioManagers_->release(std::move(manager_));
}

// _____________________________________________________________________________
VocabBatchLookupResult VocabularyOnDisk::lookupBatch(
    ql::span<const size_t> indices) const {
  return finishLookup(beginLookup(indices));
}

// _____________________________________________________________________________
VocabLookupOutput VocabularyOnDisk::lookupBatchesStreamed(
    VocabLookupInput rangeOfIndexBatches) const {
  return ad_utility::vocabulary::lookupBatchesStreamed(
      *this, std::move(rangeOfIndexBatches));
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

  // Read the offset count from the `MmapVectorMetaData` trailer, which is
  // the canonical layout used by both old and new vocabulary files.
  uint64_t numOffsets =
      ad_utility::MmapVectorMetaData::readFromFile(offsetsFile_).size_;
  AD_CORRECTNESS_CHECK(numOffsets > 0);
  size_ = numOffsets - 1;

  // Configure the opt-in adaptive io_uring batch sizing of the vocabulary's
  // batch managers from the runtime parameters `iouring-adaptive-batch-*`.
  // Without it (the default), every manager created by `makeBatchManager`
  // submits with the fixed window of `DEFAULT_IO_URING_RING_SIZE` reads.
  std::optional<ad_utility::AdaptiveBatchController> adaptiveBatchController;
  if (getRuntimeParameter<&RuntimeParameters::ioUringAdaptiveBatchEnabled_>()) {
    adaptiveBatchController.emplace(ad_utility::AdaptiveBatchController{
        getRuntimeParameter<&RuntimeParameters::ioUringAdaptiveBatchMinSize_>(),
        getRuntimeParameter<
            &RuntimeParameters::ioUringAdaptiveBatchMaxSize_>()});
  }
  // Pool of persistent `BatchIoManager`s for `beginLookup`.
  ioManagers_ = std::make_unique<IoManagerPool>(NUM_VOCAB_BATCH_IO_MANAGERS,
                                                adaptiveBatchController);
}

// _____________________________________________________________________________
VocabularyOnDisk::IoManagerPool::IoManagerPool(
    size_t initialSize,
    std::optional<ad_utility::AdaptiveBatchController> adaptiveBatchController)
    : adaptiveBatchController_{adaptiveBatchController} {
  idle_.reserve(initialSize);
  for (size_t i = 0; i < initialSize; ++i) {
    idle_.push_back(makeManager());
  }
}

// _____________________________________________________________________________
std::unique_ptr<ad_utility::BatchManagerBase>
VocabularyOnDisk::IoManagerPool::makeManager() {
  ++numManagers_;
  return ad_utility::makeBatchManager(preferIoUring_,
                                      ad_utility::DEFAULT_IO_URING_RING_SIZE,
                                      adaptiveBatchController_);
}

// _____________________________________________________________________________
std::unique_ptr<ad_utility::BatchManagerBase>
VocabularyOnDisk::IoManagerPool::acquire() {
  std::lock_guard lock{mutex_};
  if (idle_.empty()) {
    return makeManager();
  }
  auto manager = std::move(idle_.back());
  idle_.pop_back();
  return manager;
}

// _____________________________________________________________________________
void VocabularyOnDisk::IoManagerPool::release(
    std::unique_ptr<ad_utility::BatchManagerBase> manager) {
  AD_CORRECTNESS_CHECK(manager != nullptr);
  std::lock_guard lock{mutex_};
  idle_.push_back(std::move(manager));
}

// _____________________________________________________________________________
size_t VocabularyOnDisk::IoManagerPool::numManagers() const {
  std::lock_guard lock{mutex_};
  return numManagers_;
}
