// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#include "index/vocabulary/VocabularyOnDisk.h"

#include <fstream>

#include "util/Generator.h"
#include "util/StringUtils.h"

using OffsetAndSize = VocabularyOnDisk::OffsetAndSize;

// ____________________________________________________________________________
OffsetAndSize VocabularyOnDisk::getOffsetAndSize(uint64_t i) const {
  AD_CORRECTNESS_CHECK(i < size());
  const auto offset = offsets_[i];
  const auto nextOffset = offsets_[i + 1];
  return {offset, nextOffset - offset};
}

// _____________________________________________________________________________
std::string VocabularyOnDisk::operator[](uint64_t idx) const {
  AD_CONTRACT_CHECK(idx < size());
  auto offsetAndSize = getOffsetAndSize(idx);
  string result(offsetAndSize.size_, '\0');
  file_.read(result.data(), offsetAndSize.size_, offsetAndSize.offset_);
  return result;
}

// _____________________________________________________________________________
template <typename Iterable>
void VocabularyOnDisk::buildFromIterable(Iterable&& it,
                                         const string& fileName) {
  {
    file_.open(fileName.c_str(), "w");
    ad_utility::MmapVector<Offset> offsets(fileName + offsetSuffix_,
                                           ad_utility::CreateTag{});
    uint64_t currentOffset = 0;
    uint64_t nextId = 0;
    for (const auto& [word, id] : it) {
      AD_CONTRACT_CHECK(nextId == id);
      ++nextId;
      offsets.push_back(currentOffset);
      currentOffset += file_.write(word.data(), word.size());
    }

    // End offset of last vocabulary entry, also consistent with the empty
    // vocabulary.
    offsets.push_back(currentOffset);
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
uint64_t VocabularyOnDisk::WordWriter::operator()(
    std::string_view word, [[maybe_unused]] bool isExternalDummy) {
  offsets_.push_back(currentOffset_);
  currentOffset_ += file_.write(word.data(), word.size());
  return offsets_.size() - 1;
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
  buildFromIterable(wordsAndIds, fileName);
}

// _____________________________________________________________________________
void VocabularyOnDisk::open(const std::string& filename) {
  file_.open(filename.c_str(), "r");
  offsets_.open(filename + offsetSuffix_);
  AD_CORRECTNESS_CHECK(offsets_.size() > 0);
  size_ = offsets_.size() - 1;
}
