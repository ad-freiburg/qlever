// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#include "index/VocabularyOnDisk.h"

#include <fstream>

#include "util/Generator.h"
#include "util/StringUtils.h"

using OffsetAndSize = VocabularyOnDisk::OffsetAndSize;

// ____________________________________________________________________________
std::optional<OffsetAndSize> VocabularyOnDisk::getOffsetAndSize(
    uint64_t idx) const {
  IndexAndOffset idAndDummyOffset{idx, 0};
  auto it = std::ranges::lower_bound(idsAndOffsets_, idAndDummyOffset);
  if (it >= idsAndOffsets_.end() - 1 || it->idx_ != idx) {
    return std::nullopt;
  }
  return getOffsetAndSizeForIthElement(it - idsAndOffsets_.begin());
}

// ____________________________________________________________________________
VocabularyOnDisk::OffsetSizeId VocabularyOnDisk::getOffsetSizeIdForIthElement(
    uint64_t i) const {
  AD_CONTRACT_CHECK(i < size());
  const auto offset = idsAndOffsets_[i].offset_;
  const auto nextOffset = idsAndOffsets_[i + 1].offset_;
  return OffsetSizeId{offset, nextOffset - offset, idsAndOffsets_[i].idx_};
}

// _____________________________________________________________________________
std::optional<string> VocabularyOnDisk::operator[](uint64_t idx) const {
  auto optionalOffsetAndSize = getOffsetAndSize(idx);
  if (!optionalOffsetAndSize.has_value()) {
    return std::nullopt;
  }

  string result(optionalOffsetAndSize->_size, '\0');
  file_.read(result.data(), optionalOffsetAndSize->_size,
             optionalOffsetAndSize->_offset);
  return result;
}

// _____________________________________________________________________________
template <typename Iterable>
void VocabularyOnDisk::buildFromIterable(Iterable&& it,
                                         const string& fileName) {
  {
    file_.open(fileName.c_str(), "w");
    ad_utility::MmapVector<IndexAndOffset> idsAndOffsets(
        fileName + offsetSuffix_, ad_utility::CreateTag{});
    uint64_t currentOffset = 0;
    std::optional<uint64_t> previousId = std::nullopt;
    for (const auto& [word, id] : it) {
      AD_CONTRACT_CHECK(!previousId.has_value() || previousId.value() < id);
      idsAndOffsets.push_back(IndexAndOffset{id, currentOffset});
      currentOffset += file_.write(word.data(), word.size());
      previousId = id;
    }

    // End offset of last vocabulary entry, also consistent with the empty
    // vocabulary.
    if (previousId.has_value()) {
      idsAndOffsets.push_back(
          IndexAndOffset{previousId.value() + 1, currentOffset});
    } else {
      idsAndOffsets.push_back(IndexAndOffset{highestIdx_ + 1, currentOffset});
    }
    file_.close();
  }  // After this close, the destructor of MmapVector is called, whoch dumps
     // everything to disk.
  open(fileName);
}

// _____________________________________________________________________________
VocabularyOnDisk::WordWriter::WordWriter(const std::string& outFilename)
    : file_{outFilename.c_str(), "w"},
      idsAndOffsets_{absl::StrCat(outFilename, VocabularyOnDisk::offsetSuffix_),
                     ad_utility::CreateTag{}} {}

// _____________________________________________________________________________
void VocabularyOnDisk::WordWriter::operator()(std::string_view word) {
  AD_CONTRACT_CHECK(!previousId_.has_value() ||
                    previousId_.value() < currentIndex_);
  idsAndOffsets_.push_back(IndexAndOffset{currentIndex_, currentOffset_});
  currentOffset_ += file_.write(word.data(), word.size());
  previousId_ = currentIndex_;
  ++currentIndex_;
}

// _____________________________________________________________________________
void VocabularyOnDisk::WordWriter::finish() {
  if (std::exchange(isFinished_, true)) {
    return;
  }
  // End offset of last vocabulary entry, also consistent with the empty
  // vocabulary.
  auto endIndex = previousId_.value_or(VocabularyOnDisk::highestIndexEmpty_);
  idsAndOffsets_.push_back(IndexAndOffset{endIndex, currentOffset_});
  file_.close();
  idsAndOffsets_.close();
}

// _____________________________________________________________________________
VocabularyOnDisk::WordWriter::~WordWriter() {
  throwInDestructorIfSafe_([this]() { finish(); },
                           "`~VocabularyOnDisk::WordWriter`");
}

// _____________________________________________________________________________
void VocabularyOnDisk::buildFromStringsAndIds(
    const std::vector<std::pair<std::string, uint64_t>>& wordsAndIds,
    const std::string& fileName) {
  return buildFromIterable(wordsAndIds, fileName);
}

// _____________________________________________________________________________
void VocabularyOnDisk::open(const std::string& filename) {
  file_.open(filename.c_str(), "r");
  idsAndOffsets_.open(filename + offsetSuffix_);
  AD_CONTRACT_CHECK(idsAndOffsets_.size() > 0);
  size_ = idsAndOffsets_.size() - 1;
  if (size_ > 0) {
    highestIdx_ = (*(end() - 1))._index;
  }
}

// ____________________________________________________________________________
WordAndIndex VocabularyOnDisk::getIthElement(size_t n) const {
  AD_CONTRACT_CHECK(n < idsAndOffsets_.size());
  auto offsetSizeId = getOffsetSizeIdForIthElement(n);

  std::string result(offsetSizeId._size, '\0');
  file_.read(result.data(), offsetSizeId._size, offsetSizeId._offset);

  return {std::move(result), offsetSizeId._id};
}
