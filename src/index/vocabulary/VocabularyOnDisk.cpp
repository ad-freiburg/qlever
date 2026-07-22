// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#include "index/vocabulary/VocabularyOnDisk.h"

#include <absl/cleanup/cleanup.h>
#include <absl/functional/bind_front.h>

#include <algorithm>
#include <array>

#include "global/Constants.h"
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
          size_t chunkStart) mutable -> ql::span<const uint64_t> {
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
std::vector<VocabularyOnDisk::OffsetPair> VocabularyOnDisk::readOffsetPairs(
    ad_utility::BatchManagerBase& manager,
    ql::span<const size_t> indices) const {
  // For each requested index `i`, read its offset together with the next offset
  // (which bounds the string) as one 16-byte pair from `.offsets`.
  const size_t numIndices = indices.size();
  std::vector<OffsetPair> offsetPairs(numIndices);
  std::vector<size_t> sizes(numIndices, sizeof(OffsetPair));
  std::vector<uint64_t> fileOffsets(numIndices);
  std::vector<char*> targets(numIndices);
  for (auto&& [fileOffset, index, target, offsetPair] :
       ::ranges::views::zip(fileOffsets, indices, targets, offsetPairs)) {
    AD_CONTRACT_CHECK(index < size());
    fileOffset = index * sizeof(uint64_t);
    target = reinterpret_cast<char*>(&offsetPair);
  }
  manager.wait(
      manager.addBatch(offsetsFile_.fd(), sizes, fileOffsets, targets));
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

  // Initialize pool of persistent `BatchIoManager`s for `lookupBatch`.
  ioManagers_ = std::make_unique<ad_utility::data_structures::ThreadSafeQueue<
      std::unique_ptr<ad_utility::BatchManagerBase>>>(
      NUM_VOCAB_BATCH_IO_MANAGERS);
  bool preferIoUring = true;
  for (size_t i = 0; i < NUM_VOCAB_BATCH_IO_MANAGERS; ++i) {
    ioManagers_->push(ad_utility::makeBatchManager(preferIoUring));
  }
}
