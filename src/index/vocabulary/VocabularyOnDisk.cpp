// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#include "index/vocabulary/VocabularyOnDisk.h"

#include <array>

#include "util/StringUtils.h"

using OffsetAndSize = VocabularyOnDisk::OffsetAndSize;

namespace {
// The offsets file uses the same on-disk format that
// `ad_utility::MmapVector<uint64_t>` produces, so that vocabularies that were
// built by older versions of QLever (which stored the offsets in an
// `MmapVector`) can still be read. The file consists of the array of offsets
// (one `uint64_t` per word, plus a final offset), optionally followed by unused
// capacity, followed by a metadata trailer at the very end of the file with the
// following layout (see `MmapVector::writeMetaDataToEnd`):
//   size      (uint64_t)  number of offsets stored in the array
//   capacity  (uint64_t)  number of offsets the array region can hold
//   bytesize  (uint64_t)  size of the array region in bytes
//   magic     (uint32_t)  magic number identifying the format
//   version   (uint32_t)  format version
// Note that reading only requires the `size` and the magic number and version
// (for validation); the offsets themselves are always stored at the very
// beginning of the file and are read via `pread`, so any unused capacity is
// simply ignored.
constexpr uint32_t mmapMagicNumber = 7601577;
constexpr uint32_t mmapVersion = 0;
constexpr off_t mmapTrailerSize = 3 * sizeof(uint64_t) + 2 * sizeof(uint32_t);

// Append the metadata trailer for `numOffsets` offsets to the current position
// of `file` (which must be at the end of the offsets array).
void writeOffsetsTrailer(ad_utility::File& file, uint64_t numOffsets) {
  const uint64_t capacity = numOffsets;
  const uint64_t bytesize = numOffsets * sizeof(uint64_t);
  file.write(&numOffsets, sizeof(numOffsets));
  file.write(&capacity, sizeof(capacity));
  file.write(&bytesize, sizeof(bytesize));
  file.write(&mmapMagicNumber, sizeof(mmapMagicNumber));
  file.write(&mmapVersion, sizeof(mmapVersion));
}

// Read the number of offsets stored in `file` from its metadata trailer and
// validate the magic number and the format version.
uint64_t readNumOffsets(ad_utility::File& file) {
  const off_t fileSize = file.sizeOfFile();
  AD_CORRECTNESS_CHECK(fileSize >= mmapTrailerSize);
  const off_t trailerStart = fileSize - mmapTrailerSize;
  uint64_t numOffsets = 0;
  uint32_t magicNumber = 0;
  uint32_t version = 0;
  file.read(&numOffsets, sizeof(numOffsets), trailerStart);
  file.read(&magicNumber, sizeof(magicNumber),
            trailerStart + 3 * static_cast<off_t>(sizeof(uint64_t)));
  file.read(&version, sizeof(version),
            trailerStart + 3 * static_cast<off_t>(sizeof(uint64_t)) +
                static_cast<off_t>(sizeof(uint32_t)));
  AD_CORRECTNESS_CHECK(
      magicNumber == mmapMagicNumber,
      "Vocabulary offsets file did not contain the correct magic number.");
  AD_CORRECTNESS_CHECK(
      version == mmapVersion,
      "Vocabulary offsets file did not contain the correct version number.");
  return numOffsets;
}
}  // namespace

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
  writeOffsetsTrailer(offsetsFile_, numWords_);
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
  const uint64_t numOffsets = readNumOffsets(offsetsFile_);
  AD_CORRECTNESS_CHECK(numOffsets > 0);
  size_ = numOffsets - 1;
}
