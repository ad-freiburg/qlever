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

// The maximum number of words that the batched `scanAll` implementation reads
// per underlying access.
constexpr size_t BATCH_SIZE = 100'000;

// Limit how many bytes of word data are read into memory at once. If an
// individual word is larger than this limit, we have no choice but to surpass
// it anyways.
constexpr ad_utility::MemorySize UPPER_LIMIT = 10_MB;

// _____________________________________________________________________________
VocabularyScanRange VocabularyOnDisk::scanAll() const {
  // The words are read in batches: for each batch, the offsets and the
  // concatenated word data are read with a single large sequential read each
  // (instead of two small `pread`s per word as in `operator[]`). A batch holds
  // at most `BATCH_SIZE` words and at most `UPPER_LIMIT` bytes of word data
  // (but always at least one word). The individual words are then yielded as
  // `string_view`s into the batch's `data` buffer, together with their
  // (contiguous) index.
  auto getWord = [this, batchStart = uint64_t{0}, data = std::string{},
                  offsets = std::vector<uint64_t>{},
                  posInBatch =
                      size_t{0}]() mutable -> std::optional<IndexAndWord> {
    if (posInBatch + 1 >= offsets.size()) {
      // The current batch is exhausted (or we haven't read any batch yet).
      batchStart += posInBatch;
      if (batchStart >= size()) {
        return std::nullopt;
      }
      // Read the offsets of the next `numWords` words plus the end offset of
      // the last one in a single read.
      size_t numWords = std::min<size_t>(BATCH_SIZE, size() - batchStart);
      offsets.resize(numWords + 1);
      offsetsFile_.read(offsets.data(), (numWords + 1) * sizeof(uint64_t),
                        static_cast<off_t>(batchStart * sizeof(uint64_t)));
      // Shrink the batch so that the word data (`data`) stays within
      // `UPPER_LIMIT`. We always keep at least one word, even if that single
      // word is larger than `UPPER_LIMIT` (a word must not be split across
      // batches).
      size_t numWordsInBatch = 1;
      while (numWordsInBatch < numWords &&
             offsets[numWordsInBatch + 1] - offsets[0] <=
                 UPPER_LIMIT.getBytes()) {
        ++numWordsInBatch;
      }
      offsets.resize(numWordsInBatch + 1);
      // Read the word data of the (possibly shrunk) batch in a single read.
      data.resize(offsets[numWordsInBatch] - offsets[0]);
      file_.read(data.data(), data.size(), static_cast<off_t>(offsets[0]));
      posInBatch = 0;
    }
    size_t begin = offsets[posInBatch] - offsets[0];
    size_t end = offsets[posInBatch + 1] - offsets[0];
    uint64_t index = batchStart + posInBatch;
    ++posInBatch;
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
