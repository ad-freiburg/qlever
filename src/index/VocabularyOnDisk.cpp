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
  if (idx >= offsets_.size()) {
    return std::nullopt;
  }
  return getOffsetAndSizeForIthElement(idx);
}

// ____________________________________________________________________________
VocabularyOnDisk::OffsetSizeId VocabularyOnDisk::getOffsetSizeIdForIthElement(
    uint64_t i) const {
  AD_CONTRACT_CHECK(i < size());
  const auto offset = offsets_[i];
  const auto nextOffset = offsets_[i + 1];
  return OffsetSizeId{offset, nextOffset - offset, i};
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
    ad_utility::MmapVector<Offset> idsAndOffsets(fileName + offsetSuffix_,
                                                 ad_utility::CreateTag{});
    uint64_t currentOffset = 0;
    uint64_t nextId = 0;
    for (const auto& [word, id] : it) {
      AD_CONTRACT_CHECK(nextId == id);
      ++nextId;
      idsAndOffsets.push_back(currentOffset);
      currentOffset += file_.write(word.data(), word.size());
    }

    // End offset of last vocabulary entry, also consistent with the empty
    // vocabulary.
    idsAndOffsets.push_back(currentOffset);
    file_.close();
  }  // After this close, the destructor of MmapVector is called, which dumps
     // everything to disk.
  open(fileName);
}

// _____________________________________________________________________________
VocabularyOnDisk::WordWriter::WordWriter(const std::string& outFilename)
    : file_{outFilename.c_str(), "w"},
      offsets_{absl::StrCat(outFilename, VocabularyOnDisk::offsetSuffix_),
               ad_utility::CreateTag{}} {}

// _____________________________________________________________________________
void VocabularyOnDisk::WordWriter::operator()(std::string_view word) {
  offsets_.push_back(currentOffset_);
  currentOffset_ += file_.write(word.data(), word.size());
}

// _____________________________________________________________________________
void VocabularyOnDisk::WordWriter::finish() {
  if (std::exchange(isFinished_, true)) {
    return;
  }
  // End offset of last vocabulary entry, also consistent with the empty
  // vocabulary.
  offsets_.push_back(currentOffset_);
  file_.close();
  offsets_.close();
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
  offsets_.open(filename + offsetSuffix_);
  AD_CONTRACT_CHECK(offsets_.size() > 0);
  size_ = offsets_.size() - 1;
  if (size_ > 0) {
    highestIdx_ = (*(end() - 1))._index;
  }
}

// ____________________________________________________________________________
WordAndIndex VocabularyOnDisk::getIthElement(size_t n) const {
  AD_CONTRACT_CHECK(n < offsets_.size());
  auto offsetSizeId = getOffsetSizeIdForIthElement(n);

  std::string result(offsetSizeId._size, '\0');
  file_.read(result.data(), offsetSizeId._size, offsetSizeId._offset);

  return {std::move(result), offsetSizeId._id};
}
