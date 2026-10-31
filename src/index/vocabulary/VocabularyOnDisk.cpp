// Copyright 2022 - 2026, The QLever Authors, in particular:
//
// 2022 Johannes Kalmbach <johannes.kalmbach@gmail.com>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/vocabulary/VocabularyOnDisk.h"

#include <absl/cleanup/cleanup.h>
#include <absl/functional/bind_front.h>

#include <algorithm>
#include <array>
#include <atomic>

#include "global/Constants.h"
#include "global/RuntimeParameters.h"
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
  readConsecutiveOffsets(i, offsets);
  return {offsets[0], offsets[1] - offsets[0]};
}

// _____________________________________________________________________________
void VocabularyOnDisk::readConsecutiveOffsets(
    uint64_t first, ql::span<Offset> offsetsOut) const {
  size_t numBytes = offsetsOut.size() * sizeof(Offset);
  auto numBytesRead = offsetsFile_.read(
      offsetsOut.data(), numBytes, static_cast<off_t>(first * sizeof(Offset)));
  // A short read means a truncated or corrupt offsets file.
  AD_CORRECTNESS_CHECK(numBytesRead == static_cast<ssize_t>(numBytes));
}

// _____________________________________________________________________________
void VocabularyOnDisk::readConsecutiveWordBatch(
    Offset beginOffset, Offset endOffset,
    ad_utility::UninitializedVector<char>& dataOut) const {
  dataOut.resize(endOffset - beginOffset);
  auto numBytesRead = file_.read(dataOut.data(), dataOut.size(),
                                 static_cast<off_t>(beginOffset));
  // A short read means a truncated or corrupt data file. The check also
  // guarantees that the uninitialized `dataOut` is completely written.
  AD_CORRECTNESS_CHECK(numBytesRead == static_cast<ssize_t>(dataOut.size()));
}

// _____________________________________________________________________________
std::string VocabularyOnDisk::readWord(uint64_t idx) const {
  auto offsetAndSize = getOffsetAndSize(idx);
  std::string result(offsetAndSize.size_, '\0');
  file_.read(result.data(), offsetAndSize.size_,
             static_cast<off_t>(offsetAndSize.offset_));
  return result;
}

namespace {
// Given the `offsets` of a chunk of words and a starting position `first`
// within that chunk, return how many words starting at `first` can be read into
// a single data buffer without their combined size exceeding the `limit`, but
// always at least one word, even if that single word is larger than the limit
// (a word must not be split).
size_t numWordsWithinLimit(
    ql::span<const uint64_t> offsets, size_t first,
    ad_utility::MemorySize limit = VOCABULARY_SCAN_MAX_WORD_DATA_PER_BATCH) {
  // Common case: all remaining words of the chunk fit. Checking this first
  // (i.e. looking at the end right away) avoids a search in the expected case
  // where a whole batch of words comfortably fits within the limit.
  if (offsets.back() - offsets[first] <= limit.getBytes()) {
    return offsets.size() - 1 - first;
  }
  // Otherwise binary-search for the largest prefix that fits. `offsets` is
  // ascending, so the number of words that fit is the number of offsets in
  // `[first, total]` that are `<= offsets[first] + limit`, minus one.
  uint64_t threshold = offsets[first] + limit.getBytes();
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

// The read-ahead state of `VocabularyOnDisk::operator[]` for a single thread.
struct VocabularyOnDisk::ReadAheadState {
  // The `id_` of the vocabulary of the most recent lookup (0 for none).
  uint64_t mostRecentVocabId_ = 0;
  // The index of the most recent lookup.
  uint64_t mostRecentIdx_ = 0;
  // The size of the most recent window.
  size_t mostRecentWindowSize_ = 1;
  // The cached words `cachedFirstIdx_, cachedFirstIdx_ + 1, ...` of the
  // vocabulary `mostRecentVocabId_`: their offsets (plus the offset after the
  // last word) and their data. Empty if nothing is cached (see `readWindow`).
  uint64_t cachedFirstIdx_ = 0;
  ad_utility::UninitializedVector<Offset> cachedOffsets_;
  ad_utility::UninitializedVector<char> cachedData_;

  // Return the word `idx` of the `vocab`, from the cache if possible. Otherwise
  // read it from disk, together with the following words if the lookups are
  // sequential.
  std::string lookup(const VocabularyOnDisk& vocab, uint64_t idx) {
    bool isCached = isCachedByThisState(vocab.id_, idx);
    if (!isCached) {
      size_t windowSize = nextWindowSize(vocab.id_, idx);
      isCached = readWindow(vocab, idx, windowSize);
      mostRecentVocabId_ = vocab.id_;
      cachedFirstIdx_ = idx;
      mostRecentWindowSize_ = windowSize;
    }
    mostRecentIdx_ = idx;
    return isCached ? cachedWord(idx) : vocab.readWord(idx);
  }

 private:
  // Return true iff the word `idx` of the vocabulary `vocabId` is cached.
  bool isCachedByThisState(uint64_t vocabId, uint64_t idx) const {
    return vocabId == mostRecentVocabId_ && idx >= cachedFirstIdx_ &&
           idx - cachedFirstIdx_ + 1 < cachedOffsets_.size();
  }

  // Return the window size for a lookup of the word `idx` of the vocabulary
  // `vocabId` that is not cached: the most recent window size doubled if the
  // lookup is sequential (see `VOCABULARY_READ_AHEAD_MAX_GAP`), else 1.
  size_t nextWindowSize(uint64_t vocabId, uint64_t idx) const {
    bool isSequential = vocabId == mostRecentVocabId_ && idx > mostRecentIdx_ &&
                        idx - mostRecentIdx_ <= VOCABULARY_READ_AHEAD_MAX_GAP;
    return isSequential ? std::min(2 * mostRecentWindowSize_,
                                   VOCABULARY_READ_AHEAD_MAX_WORDS)
                        : 1;
  }

  // Cache the `windowSize` words of the `vocab` starting at `first`, but only
  // up to the end of the vocabulary and `VOCABULARY_READ_AHEAD_MAX_WORD_DATA`
  // bytes of data. If that leaves fewer than two words, cache nothing and
  // return false.
  //
  // NOTE: A single word is cheaper to read via `readWord` (the same two reads,
  // but no extra copy), and a single word above the byte limit would grow the
  // buffers of this thread for good, because they never shrink.
  bool readWindow(const VocabularyOnDisk& vocab, uint64_t first,
                  size_t windowSize) {
    // Invalidate first, so that the state stays consistent if a read throws.
    mostRecentVocabId_ = 0;
    cachedOffsets_.clear();
    size_t numWords = std::min<size_t>(windowSize, vocab.size() - first);
    if (numWords < 2) {
      return false;
    }
    cachedOffsets_.resize(numWords + 1);
    vocab.readConsecutiveOffsets(first, cachedOffsets_);
    numWords = numWordsWithinLimit(cachedOffsets_, 0,
                                   VOCABULARY_READ_AHEAD_MAX_WORD_DATA);
    if (numWords < 2) {
      cachedOffsets_.clear();
      return false;
    }
    cachedOffsets_.resize(numWords + 1);
    vocab.readConsecutiveWordBatch(cachedOffsets_.front(),
                                   cachedOffsets_.back(), cachedData_);
    return true;
  }

  // Return the word `idx`, which must be cached.
  std::string cachedWord(uint64_t idx) const {
    size_t i = idx - cachedFirstIdx_;
    return std::string(
        cachedData_.data() + (cachedOffsets_[i] - cachedOffsets_.front()),
        cachedOffsets_[i + 1] - cachedOffsets_[i]);
  }
};

// _____________________________________________________________________________
std::string VocabularyOnDisk::operator[](uint64_t idx) const {
  AD_CONTRACT_CHECK(idx < size());
  thread_local ReadAheadState state;
  return state.lookup(*this, idx);
}

// _____________________________________________________________________________
auto VocabularyOnDisk::chunkToWords(ql::span<const uint64_t> offsets) const {
  return ad_utility::allView(ad_utility::CachingTransformInputRange{
             chunkOffsets(offsets),
             [this, data = ad_utility::UninitializedVector<char>{}](
                 ql::span<const uint64_t> subOffsets) mutable {
               readConsecutiveWordBatch(subOffsets.front(), subOffsets.back(),
                                        data);
               return mapOffsetsToStringViews(
                   subOffsets, std::string_view{data.data(), data.size()});
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
        ql::span<uint64_t> offsets{buffer.data(), numWords + 1};
        readConsecutiveOffsets(chunkStart, offsets);
        return ql::span<const uint64_t>{offsets};
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

namespace {
// A run of consecutive vocabulary indices, starting at `firstIndex_`, together
// with the entries of the `.offsets` file that bound their words: entries `i`
// and `i + 1` of `offsets_` bound the word of index `firstIndex_ + i`, so a run
// of `k` indices has `k + 1` entries, which are read as one range. `offsets_`
// points into the buffer passed to `splitIntoOffsetRuns`.
struct OffsetRun {
  size_t firstIndex_;
  ql::span<uint64_t> offsets_;
};

// _____________________________________________________________________________
// Split `indices` into runs of consecutive indices and give each run its
// entries in `offsets`, which is resized to hold all of them. The result points
// into `offsets`, so `offsets` must outlive it and must not be resized.
std::vector<OffsetRun> splitIntoOffsetRuns(ql::span<const size_t> indices,
                                           std::vector<uint64_t>& offsets) {
  // The first index and the number of indices of each run (`chunk_by` yields
  // no empty chunks).
  const auto runs = ::ranges::to<std::vector<std::pair<size_t, size_t>>>(
      indices | ::ranges::views::chunk_by([](size_t previous, size_t next) {
        return next == previous + 1;
      }) |
      ::ranges::views::transform([](const auto& run) {
        return std::pair{*::ranges::begin(run),
                         static_cast<size_t>(::ranges::distance(run))};
      }));

  offsets.resize(indices.size() + runs.size());
  std::vector<OffsetRun> offsetRuns;
  offsetRuns.reserve(runs.size());
  size_t nextOffset = 0;
  for (auto [firstIndex, numIndices] : runs) {
    const size_t numOffsets = numIndices + 1;
    offsetRuns.push_back({firstIndex, ql::span<uint64_t>{offsets}.subspan(
                                          nextOffset, numOffsets)});
    nextOffset += numOffsets;
  }
  return offsetRuns;
}

// _____________________________________________________________________________
// Read the offsets of all `runs` from the `.offsets` file `fd` in one batch.
void readOffsetRuns(ad_utility::BatchManagerBase& manager, int fd,
                    ql::span<const OffsetRun> runs) {
  const auto sizes = ::ranges::to<std::vector<size_t>>(
      runs | ::ranges::views::transform([](const OffsetRun& run) {
        return run.offsets_.size_bytes();
      }));
  const auto fileOffsets = ::ranges::to<std::vector<uint64_t>>(
      runs | ::ranges::views::transform([](const OffsetRun& run) {
        return uint64_t{run.firstIndex_ * sizeof(uint64_t)};
      }));
  auto targets = ::ranges::to<std::vector<char*>>(
      runs | ::ranges::views::transform([](const OffsetRun& run) {
        return reinterpret_cast<char*>(run.offsets_.data());
      }));
  manager.wait(manager.addBatch(fd, sizes, fileOffsets, targets));
}
}  // namespace

// _____________________________________________________________________________
std::vector<VocabularyOnDisk::OffsetPair> VocabularyOnDisk::readOffsetPairs(
    ad_utility::BatchManagerBase& manager,
    ql::span<const size_t> indices) const {
  for (size_t index : indices) {
    AD_CONTRACT_CHECK(index < size());
  }
  std::vector<uint64_t> offsets;
  const auto runs = splitIntoOffsetRuns(indices, offsets);
  readOffsetRuns(manager, offsetsFile_.fd(), runs);

  // Each pair of neighbouring offsets of a run bounds one word.
  std::vector<OffsetPair> offsetPairs;
  offsetPairs.reserve(indices.size());
  for (const OffsetRun& run : runs) {
    for (auto [offset, nextOffset] : ::ranges::views::zip(
             run.offsets_, run.offsets_ | ::ranges::views::drop(1))) {
      offsetPairs.push_back(OffsetPair{offset, nextOffset});
    }
  }
  return offsetPairs;
}

// _____________________________________________________________________________
VocabBatchLookupResult VocabularyOnDisk::readStrings(
    ad_utility::BatchManagerBase& manager,
    ql::span<const OffsetPair> offsetPairs) const {
  // Read the string data. String `i` starts at `offset_` with length
  // `nextOffset_ - offset_`; the strings are packed contiguously into `buffer`.
  const size_t numIndices = offsetPairs.size();
  std::vector<size_t> sizes(numIndices);
  std::vector<uint64_t> fileOffsets(numIndices);
  for (auto&& [size, fileOffset, offsetPair] :
       ::ranges::views::zip(sizes, fileOffsets, offsetPairs)) {
    size = offsetPair.nextOffset_ - offsetPair.offset_;
    fileOffset = offsetPair.offset_;
  }

  auto data = std::make_shared<VocabBatchLookupData>();
  data->buffer().resize(::ranges::accumulate(sizes, size_t{0}));
  data->views().resize(numIndices);

  std::vector<char*> targets(numIndices);
  size_t bufferOffset = 0;
  for (auto&& [target, view, size] :
       ::ranges::views::zip(targets, data->views(), sizes)) {
    target = data->buffer().data() + bufferOffset;
    view = std::string_view(target, size);
    bufferOffset += size;
  }

  manager.wait(manager.addBatch(file_.fd(), sizes, fileOffsets, targets));
  return VocabBatchLookupData::asResult(std::move(data));
}

// _____________________________________________________________________________
VocabBatchLookupResult VocabularyOnDisk::lookupBatch(
    ql::span<const size_t> indices) const {
  AD_CONTRACT_CHECK(!indices.empty());

  auto manager = ioManagers_->pop().value();
  // Return the `manager` to the pool on every exit path (including exceptions,
  // e.g. an out-of-range index in phase 1), so we never leak an `IoManager`
  // (and its io_uring buffers) out of the pool.
  absl::Cleanup returnManager{[this, &manager]() {
    ad_utility::terminateIfThrows(
        [this, &manager]() { ioManagers_->push(std::move(manager)); },
        "returning the `IoManager` to the pool in "
        "`VocabularyOnDisk::lookupBatch`");
  }};

  // The page-cache fast path belongs to the manager; the runtime parameter can
  // change between two lookups.
  manager->setPageCacheFastPath(
      getRuntimeParameter<&RuntimeParameters::vocabularyPageCacheFastPath_>());
  auto offsetPairs = readOffsetPairs(*manager, indices);
  return readStrings(*manager, offsetPairs);
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
uint64_t VocabularyOnDisk::WordWriter::reserveBlock(const WordBlock& block) {
  AD_CONTRACT_CHECK(block.firstPosition_ == numWords_);
  uint64_t dataOffset = currentOffset_;
  currentOffset_ += block.data().size();
  numWords_ += block.numWords();
  return dataOffset;
}

// _____________________________________________________________________________
void VocabularyOnDisk::WordWriter::writeBlockAt(const WordBlock& block,
                                                uint64_t dataOffset) const {
  const size_t numWords = block.numWords();
  // The offsets of the block are relative to its data, the ones in the file
  // are absolute.
  std::vector<uint64_t> offsets(numWords);
  for (size_t i = 0; i < numWords; ++i) {
    offsets[i] = dataOffset + block.offsets()[i];
  }
  // NOTE: The positioned `write` throws if not all the bytes could be written.
  offsetsFile_.write(
      offsets.data(), numWords * sizeof(uint64_t),
      static_cast<off_t>(block.firstPosition_ * sizeof(uint64_t)));
  file_.write(block.data().data(), block.data().size(),
              static_cast<off_t>(dataOffset));
}

// _____________________________________________________________________________
void VocabularyOnDisk::WordWriter::finishImpl() {
  // The blocks are written with positioned writes (see `writeBlockAt`), which
  // bypass the position of the files, so the trailer has to go to the end
  // explicitly. This is a no-op for words that were added one by one.
  offsetsFile_.seek(0, SEEK_END);
  file_.seek(0, SEEK_END);
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
  static std::atomic<uint64_t> nextId = 1;
  id_ = nextId++;
  file_.open(filename, "r");
  offsetsFile_.open(filename + offsetSuffix_, "r");

  // Read the offset count from the `MmapVectorMetaData` trailer, which is
  // the canonical layout used by both old and new vocabulary files.
  uint64_t numOffsets =
      ad_utility::MmapVectorMetaData::readFromFile(offsetsFile_).size_;
  AD_CORRECTNESS_CHECK(numOffsets > 0);
  size_ = numOffsets - 1;

  // Initialize pool of persistent `BatchIoManager`s for `lookupBatch`.
  ioManagers_ = std::make_unique<ad_utility::data_structures::ThreadSafeQueue<
      std::unique_ptr<ad_utility::BatchManagerBase>>>(
      NUM_VOCAB_BATCH_IO_MANAGERS);
  bool preferIoUring = true;
  auto makeManager = [&preferIoUring]() {
    return ad_utility::makeBatchManager(preferIoUring);
  };
  for (auto&& manager :
       ::ranges::views::generate_n(makeManager, NUM_VOCAB_BATCH_IO_MANAGERS)) {
    ioManagers_->push(std::move(manager));
  }
}
