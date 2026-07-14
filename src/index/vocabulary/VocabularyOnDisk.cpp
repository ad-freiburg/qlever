// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#include "index/vocabulary/VocabularyOnDisk.h"

#include <algorithm>
#include <array>

#include "util/MmapVector.h"
#include "util/StringUtils.h"

using namespace ad_utility::memory_literals;
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

// The maximum number of words whose offsets the batched `scanAll` reads per
// underlying access. `BATCH_SIZE` words are chosen to comfortably fit within
// `UPPER_LIMIT` for regular datasets, so that usually the whole chunk's word
// data can be read in a single read.
constexpr size_t BATCH_SIZE = 10'000;

// Limit how many bytes of word data are read into memory at once. If an
// individual word is larger than this limit, we have no choice but to surpass
// it anyways.
constexpr ad_utility::MemorySize UPPER_LIMIT = 10_MB;

namespace {
// Given the `offsets` of a chunk of words and a starting position `first`
// within that chunk (with `total` words in the chunk), return how many words
// starting at `first` can be read into a single data buffer without their
// combined size exceeding `UPPER_LIMIT` -- but always at least one word, even
// if that single word is larger than `UPPER_LIMIT` (a word must not be split).
size_t numWordsWithinLimit(const std::vector<uint64_t>& offsets, size_t first,
                           size_t total) {
  // Common case: all remaining words of the chunk fit. Checking this first
  // (i.e. looking at the end right away) avoids a search in the expected case
  // where `BATCH_SIZE` words comfortably fit within `UPPER_LIMIT`.
  if (offsets[total] - offsets[first] <= UPPER_LIMIT.getBytes()) {
    return total - first;
  }
  // Otherwise binary-search for the largest prefix that fits. `offsets` is
  // ascending, so the number of words that fit is the number of offsets in
  // `[first, total]` that are `<= offsets[first] + UPPER_LIMIT`, minus one.
  uint64_t threshold = offsets[first] + UPPER_LIMIT.getBytes();
  auto begin = offsets.begin() + first;
  auto it = std::upper_bound(begin, offsets.begin() + total + 1, threshold);
  size_t numFit = static_cast<size_t>(it - begin) - 1;
  return std::max<size_t>(numFit, 1);
}
}  // namespace

// _____________________________________________________________________________
VocabularyScanRange VocabularyOnDisk::scanAll() const {
  // The words are read in two nested levels of batching, each level using a
  // single large sequential read (instead of two small `pread`s per word as in
  // `operator[]`):
  //  1. The offsets are read in chunks of up to `BATCH_SIZE` words and kept in
  //     memory for the whole chunk (they are small: 8 bytes per word).
  //  2. Within a chunk, the (much larger) word data is read in sub-batches
  //  whose
  //     size is capped at `UPPER_LIMIT` bytes (but always at least one word).
  //     This uses the offsets already in memory, so they are never read twice.
  // Each word is yielded as a `string_view` into the current data buffer,
  // together with its (contiguous) index.
  auto getWord =
      [this, chunkStart = uint64_t{0}, offsets = std::vector<uint64_t>{},
       data = std::string{}, dataFirst = size_t{0}, numInData = size_t{0},
       posInData = size_t{0}]() mutable -> std::optional<IndexAndWord> {
    while (posInData >= numInData) {
      // All words whose data is currently in memory have been yielded. Advance
      // to the next sub-batch of word data, reading the next offset chunk first
      // if the current one is fully consumed.
      dataFirst += numInData;
      size_t numWordsInChunk = offsets.empty() ? 0 : offsets.size() - 1;
      if (dataFirst >= numWordsInChunk) {
        chunkStart += numWordsInChunk;
        if (chunkStart >= size()) {
          return std::nullopt;
        }
        numWordsInChunk = std::min<size_t>(BATCH_SIZE, size() - chunkStart);
        offsets.resize(numWordsInChunk + 1);
        offsetsFile_.read(offsets.data(),
                          (numWordsInChunk + 1) * sizeof(uint64_t),
                          static_cast<off_t>(chunkStart * sizeof(uint64_t)));
        dataFirst = 0;
      }
      // Read the word data for as many words (starting at `dataFirst`) as fit
      // within `UPPER_LIMIT`, in a single read.
      numInData = numWordsWithinLimit(offsets, dataFirst, numWordsInChunk);
      data.resize(offsets[dataFirst + numInData] - offsets[dataFirst]);
      file_.read(data.data(), data.size(),
                 static_cast<off_t>(offsets[dataFirst]));
      posInData = 0;
    }
    size_t idxInChunk = dataFirst + posInData;
    size_t begin = offsets[idxInChunk] - offsets[dataFirst];
    size_t end = offsets[idxInChunk + 1] - offsets[dataFirst];
    uint64_t index = chunkStart + idxInChunk;
    ++posInData;
    return IndexAndWord{index,
                        std::string_view{data.data() + begin, end - begin}};
  };
  return VocabularyScanRange{
      ad_utility::InputRangeFromGetCallable{std::move(getWord)}};
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
}
