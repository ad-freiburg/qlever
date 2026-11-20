// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/compressionUsingZstd/SectionedZstdFrame.h"

#include <zstd.h>

#include <map>
#include <string>
#include <string_view>

#include "backports/algorithm.h"
#include "util/CryptographicHashUtils.h"
#include "util/Exception.h"
#include "util/compressionUsingZstd/ZstdFrameBlocks.h"
#include "util/compressionUsingZstd/ZstdFrameHeader.h"
#include "util/compressionUsingZstd/ZstdIndependentUnit.h"
#include "util/compressionUsingZstd/ZstdSkippableFrame.h"
#include "util/serializer/ByteBufferSerializer.h"

namespace ad_utility::zstd {

// The serialization of a `UnitInfo` (for the `SectionInfo` see the header).
AD_SERIALIZE_FUNCTION(UnitInfo) {
  serializer | arg.numSections_;
  serializer | arg.compressedSize_;
  // The flag is stored as a byte that has to be 0 or 1.
  uint8_t flag = arg.endsInStartState_;
  serializer | flag;
  if constexpr (ad_utility::serialization::ReadSerializer<S>) {
    if (flag > 1) {
      throw ad_utility::serialization::SerializationException{
          "Invalid flag in the index of a sectioned ZSTD frame"};
    }
    arg.endsInStartState_ = flag == 1;
  }
}

namespace {

using namespace ad_utility::serialization;

// The `SectionIndex` is stored in a skippable frame with this magic variant
// (one of 16). Its content is:
//   8 bytes   the magic bytes `QLVRZSEC`
//   uint16_t  the version of the index format
//   uint64_t  the number of sections, and the number of units
//   the sections (40 bytes each), and the units (17 bytes each)
constexpr auto indexMagicVariant = SkippableMagicVariant::make<0xB>();
constexpr std::array<char, 8> indexMagic{'Q', 'L', 'V', 'R',
                                         'Z', 'S', 'E', 'C'};
constexpr uint16_t indexVersion = 1;
constexpr size_t indexHeaderSize =
    indexMagic.size() + sizeof(indexVersion) + 2 * sizeof(uint64_t);
constexpr size_t serializedSectionSize = sizeof(uint64_t) + sizeof(ContentHash);
constexpr size_t serializedUnitSize = 2 * sizeof(uint64_t) + sizeof(uint8_t);

// Append the skippable frame with the `index` to `out`. Return an error if the
// index is too large for a skippable frame.
Expected<void> appendIndexFrame(std::vector<char>& out,
                                const SectionIndex& index) {
  ByteBufferWriteSerializer writer;
  writer << indexMagic;
  writer << indexVersion;
  writer << static_cast<uint64_t>(index.sections_.size());
  writer << static_cast<uint64_t>(index.units_.size());
  for (const auto& section : index.sections_) {
    writer << section;
  }
  for (const auto& unit : index.units_) {
    writer << unit;
  }
  return appendTrailingSkippableFrame(out, indexMagicVariant, writer.data());
}

// Parse the content of the skippable frame with the index. Return an error if
// it is malformed.
Expected<SectionIndex> parseIndex(ql::span<const char> content) {
  if (content.size() < indexHeaderSize) {
    return ZstdError{"The index of a sectioned ZSTD frame is truncated"};
  }
  ByteBufferReadSerializerT reader{content};
  std::array<char, indexMagic.size()> magic;
  uint16_t version;
  uint64_t numSections;
  uint64_t numUnits;
  reader >> magic;
  reader >> version;
  reader >> numSections;
  reader >> numUnits;
  // Check the size before anything is allocated (the numbers might be huge).
  const size_t remaining = content.size() - indexHeaderSize;
  if (magic != indexMagic) {
    return ZstdError{
        "The index of a sectioned ZSTD frame has wrong magic bytes"};
  }
  if (version != indexVersion) {
    return ZstdError{
        "The index of a sectioned ZSTD frame has the unsupported "
        "version " +
        std::to_string(version)};
  }
  if (numSections > remaining / serializedSectionSize ||
      numUnits > remaining / serializedUnitSize ||
      numSections * serializedSectionSize + numUnits * serializedUnitSize !=
          remaining) {
    return ZstdError{
        "The size of the index of a sectioned ZSTD frame does not match the "
        "number of its sections and units"};
  }
  SectionIndex index;
  index.sections_.resize(numSections);
  index.units_.resize(numUnits);
  try {
    for (auto& section : index.sections_) {
      reader >> section;
    }
    for (auto& unit : index.units_) {
      reader >> unit;
    }
  } catch (const SerializationException& e) {
    // The serializer of `UnitInfo` throws for an invalid flag. This exception
    // does not leave this function.
    return ZstdError{e.what()};
  }
  return index;
}

// The error that is returned when a unit or section is appended to a
// `SectionedFrameBuilder` after a unit that does not end in the start state.
ZstdError cannotAppendError() {
  return ZstdError{
      "Nothing can be appended to a sectioned ZSTD frame after a unit that "
      "does not end in the start state"};
}

// Add `value` to `sum`. Return false if this overflows.
bool addWithoutOverflow(uint64_t& sum, uint64_t value) {
  if (sum + value < sum) {
    return false;
  }
  sum += value;
  return true;
}

}  // namespace

// _____________________________________________________________________________
ContentHash hashOfContent(ql::span<const char> content) {
  auto digest = HashSha256{}(std::string_view{content.data(), content.size()});
  ContentHash result;
  AD_CORRECTNESS_CHECK(digest.size() == result.size());
  ql::ranges::copy(digest, result.begin());
  return result;
}

// _____________________________________________________________________________
Expected<SectionedFrame> SectionedFrame::read(ql::span<const char> compressed) {
  auto indexContent = readTrailingSkippableFrame(compressed, indexMagicVariant);
  if (!indexContent.hasValue()) {
    return std::move(indexContent).error();
  }
  auto index = parseIndex(indexContent.value());
  if (!index.hasValue()) {
    return std::move(index).error();
  }
  if (index->units_.empty()) {
    return ZstdError{"A sectioned ZSTD frame must have at least one unit"};
  }
  SectionedFrame result;
  result.index_ = std::move(index.value());
  const auto& sections = result.index_.sections_;
  const auto& units = result.index_.units_;

  // The frame before the index.
  const size_t frameEnd = compressed.size() - indexContent->size() -
                          skippableFrameHeaderSize - sizeof(uint32_t);
  auto frame = compressed.first(frameEnd);
  auto header = parseFrameHeader(frame);
  if (!header.hasValue()) {
    return std::move(header).error();
  }
  // The frames that are assembled by `SectionedFrameBuilder` never have a
  // checksum (see `SingleSegmentFrame`).
  if (header->hasChecksum_ ||
      ZSTD_findFrameCompressedSize(frame.data(), frame.size()) != frameEnd) {
    return ZstdError{
        "The data before the index of a sectioned ZSTD frame is not exactly "
        "one ZSTD frame without a checksum"};
  }
  result.compressed_ = frame;

  // The offsets of the sections.
  uint64_t contentSize = 0;
  for (const auto& section : sections) {
    result.sectionBegins_.push_back(contentSize);
    if (!addWithoutOverflow(contentSize, section.size_)) {
      return ZstdError{
          "The sizes of the sections of a sectioned ZSTD frame overflow"};
    }
  }
  result.sectionBegins_.push_back(contentSize);
  if (ZSTD_getFrameContentSize(frame.data(), frame.size()) != contentSize) {
    return ZstdError{
        "The content size of a sectioned ZSTD frame does not match the sizes "
        "of its sections"};
  }

  // The units and their blocks.
  uint64_t blockBegin = header->headerSize_;
  uint64_t firstSection = 0;
  for (size_t u = 0; u < units.size(); ++u) {
    const auto& unit = units[u];
    result.unitBegins_.push_back(blockBegin);
    result.unitFirstSections_.push_back(firstSection);
    // All units except the last have to end in the start state.
    const bool isLastUnit = u + 1 == units.size();
    if (!addWithoutOverflow(firstSection, unit.numSections_) ||
        firstSection > sections.size() ||
        (!isLastUnit && !unit.endsInStartState_) || unit.compressedSize_ == 0 ||
        unit.compressedSize_ > frameEnd - blockBegin) {
      return ZstdError{"Unit " + std::to_string(u) +
                       " of a sectioned ZSTD frame is invalid"};
    }
    auto blocks = frame.subspan(blockBegin, unit.compressedSize_);
    blockBegin += unit.compressedSize_;
    // Only the last block of the last unit is the last block of the frame.
    auto lastHeader = lastBlockHeaderOffset(blocks);
    if (!lastHeader.hasValue()) {
      return std::move(lastHeader).error();
    }
    if (BlockHeader::read(blocks.subspan(lastHeader.value()))->isLast_ !=
        isLastUnit) {
      return ZstdError{
          "Only the last unit of a sectioned ZSTD frame may end "
          "with the last block of the frame"};
    }
    result.unitLastBlockHeaders_.push_back(lastHeader.value());
  }
  result.unitBegins_.push_back(blockBegin);
  result.unitFirstSections_.push_back(firstSection);
  if (blockBegin != frameEnd || firstSection != sections.size()) {
    return ZstdError{
        "The units of a sectioned ZSTD frame do not cover the whole frame and "
        "all its sections"};
  }
  return result;
}

// _____________________________________________________________________________
Expected<size_t> SectionedFrame::unitOfSection(size_t i) const {
  if (i >= numSections()) {
    return ZstdError{"Section " + std::to_string(i) +
                     " does not exist in a sectioned ZSTD frame with " +
                     std::to_string(numSections()) + " sections"};
  }
  // The last unit whose first section is at most `i`.
  auto it = ql::ranges::upper_bound(unitFirstSections_.begin(),
                                    unitFirstSections_.end() - 1, i);
  return static_cast<size_t>(it - unitFirstSections_.begin()) - 1;
}

// _____________________________________________________________________________
ql::span<const char> SectionedFrame::unitBlocks(size_t u) const {
  return compressed_.subspan(unitBegins_.at(u),
                             unitBegins_.at(u + 1) - unitBegins_.at(u));
}

// _____________________________________________________________________________
Expected<UninitializedBuffer> SectionedFrame::decompressUnit(size_t u) const {
  if (u >= numUnits()) {
    return ZstdError{"Unit " + std::to_string(u) +
                     " does not exist in a sectioned ZSTD frame"};
  }
  const uint64_t size = sectionBegin(firstSectionOfUnit(u + 1)) -
                        sectionBegin(firstSectionOfUnit(u));
  UninitializedBuffer result(size);
  auto decompressed =
      SingleSegmentFrame::decompressBlocks(unitBlocks(u), size, result);
  if (!decompressed.hasValue()) {
    return std::move(decompressed).error();
  }
  return result;
}

// _____________________________________________________________________________
SectionedFrameBuilder::SectionedFrameBuilder(int compressionLevel)
    : compressionLevel_{compressionLevel} {
  // The content size is not known yet, it is written by `finish`.
  SingleSegmentFrame::appendHeader(frame_, 0);
}

// _____________________________________________________________________________
Expected<void> SectionedFrameBuilder::addSection(
    ql::span<const char> content, std::optional<ContentHash> hash) {
  if (!canAppend_) {
    return cannotAppendError();
  }
  // If the pending sections cannot end in the start state, they stay pending
  // and the new section is added to their unit.
  auto compressed = tryToCompressPending(false);
  if (!compressed.hasValue()) {
    return std::move(compressed).error();
  }
  pendingParts_.push_back(content);
  pendingSections_.push_back(
      SectionInfo{content.size(),
                  hash.has_value() ? hash.value() : hashOfContent(content)});
  return {};
}

// _____________________________________________________________________________
Expected<void> SectionedFrameBuilder::copyUnit(const SectionedFrame& base,
                                               size_t u) {
  if (!canAppend_) {
    return cannotAppendError();
  }
  if (u >= base.numUnits()) {
    return ZstdError{"Unit " + std::to_string(u) +
                     " does not exist in the base frame"};
  }
  auto sections = ql::span<const SectionInfo>{base.index().sections_}.subspan(
      base.firstSectionOfUnit(u), base.unit(u).numSections_);
  auto compressed = tryToCompressPending(false);
  if (!compressed.hasValue()) {
    return std::move(compressed).error();
  }
  if (!compressed.value()) {
    // The unit cannot follow the pending sections, so it is decompressed and
    // added to their unit.
    auto content = base.decompressUnit(u);
    if (!content.hasValue()) {
      return std::move(content).error();
    }
    ownedParts_.push_back(std::move(content).value());
    pendingParts_.push_back(ownedParts_.back());
    pendingSections_.insert(pendingSections_.end(), sections.begin(),
                            sections.end());
    return {};
  }
  appendUnit(base.unitBlocks(u), base.unitLastBlockHeader(u), base.unit(u),
             sections);
  canAppend_ = base.unit(u).endsInStartState_;
  return {};
}

// _____________________________________________________________________________
Expected<bool> SectionedFrameBuilder::tryToCompressPending(bool isLast) {
  // NOTE: The last unit is also ended in the start state if possible, such
  // that the compressed form of a unit only depends on its content: it can then
  // be copied to any position, and copying it gives the same result as
  // compressing it again.
  if (pendingSections_.empty()) {
    return true;
  }
  // Concatenate the pending parts if there is more than one.
  std::vector<char> concatenation;
  ql::span<const char> content = pendingParts_.front();
  if (pendingParts_.size() > 1) {
    for (auto part : pendingParts_) {
      concatenation.insert(concatenation.end(), part.begin(), part.end());
    }
    content = concatenation;
  }
  auto unit = compressIndependentUnit(content, compressionLevel_, true);
  if (!unit.hasValue()) {
    return std::move(unit).error();
  }
  // Only the last unit may fail to end in the start state.
  if (!unit->endsInStartState_ && !isLast) {
    return false;
  }
  // The blocks were just written by `compressIndependentUnit`.
  auto lastHeader = lastBlockHeaderOffset(unit->blocks_);
  AD_CORRECTNESS_CHECK(lastHeader.hasValue());
  appendUnit(unit->blocks_, lastHeader.value(),
             UnitInfo{pendingSections_.size(), unit->blocks_.size(),
                      unit->endsInStartState_},
             pendingSections_);
  pendingParts_.clear();
  pendingSections_.clear();
  ownedParts_.clear();
  return true;
}

// _____________________________________________________________________________
void SectionedFrameBuilder::appendUnit(ql::span<const char> blocks,
                                       size_t lastBlockHeader, UnitInfo info,
                                       ql::span<const SectionInfo> sections) {
  const size_t begin = frame_.size();
  frame_.insert(frame_.end(), blocks.begin(), blocks.end());
  lastBlockHeader_ = begin + lastBlockHeader;
  // The unit might have been the last unit of another frame.
  AD_CORRECTNESS_CHECK(
      setLastBlockFlag(frame_, lastBlockHeader_.value(), false).hasValue());
  index_.units_.push_back(info);
  for (const auto& section : sections) {
    index_.sections_.push_back(section);
    contentSize_ += section.size_;
  }
}

// _____________________________________________________________________________
Expected<std::vector<char>> SectionedFrameBuilder::finish() && {
  auto compressed = tryToCompressPending(true);
  if (!compressed.hasValue()) {
    return std::move(compressed).error();
  }
  // With `isLast`, the pending sections are always compressed.
  AD_CORRECTNESS_CHECK(compressed.value());
  if (index_.units_.empty()) {
    // A frame needs at least one block, so a frame without sections consists
    // of a single empty unit.
    auto unit = compressIndependentUnit({}, compressionLevel_, false);
    if (!unit.hasValue()) {
      return std::move(unit).error();
    }
    appendUnit(unit->blocks_, lastBlockHeaderOffset(unit->blocks_).value(),
               UnitInfo{0, unit->blocks_.size(), false}, {});
  }
  AD_CORRECTNESS_CHECK(
      setLastBlockFlag(frame_, lastBlockHeader_.value(), true).hasValue());
  std::vector<char> header;
  SingleSegmentFrame::appendHeader(header, contentSize_);
  ql::ranges::copy(header, frame_.begin());
  auto indexFrame = appendIndexFrame(frame_, index_);
  if (!indexFrame.hasValue()) {
    return std::move(indexFrame).error();
  }
  return std::move(frame_);
}

// _____________________________________________________________________________
Expected<std::vector<char>> compressSectioned(
    ql::span<const char> data, ql::span<const uint64_t> sectionSizes,
    int compressionLevel, const SectionedFrame* reusableBase) {
  const ZstdError sectionSizesError{
      "The section sizes must add up to the size of the data"};
  // The sections of `data`.
  std::vector<ql::span<const char>> contents;
  std::vector<SectionInfo> sections;
  uint64_t begin = 0;
  for (uint64_t size : sectionSizes) {
    if (size > data.size() - begin) {
      return sectionSizesError;
    }
    contents.push_back(data.subspan(begin, size));
    sections.push_back(SectionInfo{size, hashOfContent(contents.back())});
    begin += size;
  }
  if (begin != data.size()) {
    return sectionSizesError;
  }

  // The units of the `reusableBase` by their first section.
  std::map<std::pair<uint64_t, ContentHash>, std::vector<size_t>> baseUnits;
  if (reusableBase != nullptr) {
    for (size_t u = 0; u < reusableBase->numUnits(); ++u) {
      if (reusableBase->unit(u).numSections_ > 0) {
        const auto& first =
            reusableBase->section(reusableBase->firstSectionOfUnit(u));
        baseUnits[{first.size_, first.hash_}].push_back(u);
      }
    }
  }
  // Return true iff unit `u` of the `reusableBase` can be copied for the
  // sections starting at `i`.
  auto isReusable = [&sections, reusableBase](size_t i, size_t u) {
    const auto& unit = reusableBase->unit(u);
    const size_t first = reusableBase->firstSectionOfUnit(u);
    if (unit.numSections_ > sections.size() - i ||
        (!unit.endsInStartState_ && i + unit.numSections_ != sections.size())) {
      return false;
    }
    for (size_t k = 0; k < unit.numSections_; ++k) {
      if (sections[i + k] != reusableBase->section(first + k)) {
        return false;
      }
    }
    return true;
  };

  SectionedFrameBuilder builder{compressionLevel};
  for (size_t i = 0; i < sections.size();) {
    auto candidates = baseUnits.find({sections[i].size_, sections[i].hash_});
    std::optional<size_t> reusableUnit;
    if (candidates != baseUnits.end()) {
      for (size_t u : candidates->second) {
        if (isReusable(i, u)) {
          reusableUnit = u;
          break;
        }
      }
    }
    Expected<void> appended;
    if (reusableUnit.has_value()) {
      appended = builder.copyUnit(*reusableBase, reusableUnit.value());
      i += reusableBase->unit(reusableUnit.value()).numSections_;
    } else {
      appended = builder.addSection(contents[i], sections[i].hash_);
      ++i;
    }
    if (!appended.hasValue()) {
      return std::move(appended).error();
    }
  }
  return std::move(builder).finish();
}

}  // namespace ad_utility::zstd
