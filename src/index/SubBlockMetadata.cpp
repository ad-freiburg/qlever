// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/SubBlockMetadata.h"

#include <absl/numeric/bits.h>
#include <absl/strings/str_cat.h>

#include <limits>
#include <sstream>

namespace {
// Return the number of bits that are needed to store `value`.
size_t bitWidth(uint64_t value) { return absl::bit_width(value); }

// Return the largest triple (with `Id::max()` as the graph) that is smaller
// than `next` when ignoring the graph and that agrees with `next` on the first
// `numSharedColumns` columns. If the column `numSharedColumns` of `next` is
// zero, there is no such triple, then return the largest triple that is
// smaller than `next` (by borrowing from the previous columns). The
// computation is based on the bits of the `Id`s, which is valid for the
// triples that are stored on disk (they never contain IDs from a
// `LocalVocab`).
SubBlockMetadata::PermutedTriple largestTripleBefore(
    const std::array<Id, 3>& next, size_t numSharedColumns) {
  AD_CORRECTNESS_CHECK(numSharedColumns < 3);
  std::array<uint64_t, 3> bits{next[0].getBits(), next[1].getBits(),
                               next[2].getBits()};
  constexpr uint64_t maxBits = Id::max().getBits();
  for (size_t i = numSharedColumns + 1; i < 3; ++i) {
    bits[i] = maxBits;
  }
  // Decrement the column `numSharedColumns` (with a borrow from the previous
  // columns if necessary).
  size_t column = numSharedColumns;
  while (bits[column] == 0) {
    AD_CORRECTNESS_CHECK(column > 0);
    bits[column] = maxBits;
    --column;
  }
  --bits[column];
  return {Id::fromBits(bits[0]), Id::fromBits(bits[1]), Id::fromBits(bits[2]),
          Id::max()};
}
}  // namespace

// _____________________________________________________________________________
bool SubBlockMetadata::containsInconsistentTriples(size_t columnIndex) const {
  return !permutedTriplesAgreeOnPrefix(firstTriple_, lastTriple_, columnIndex);
}

// _____________________________________________________________________________
bool SubBlockMetadata::isConsistentWith(const SubBlockMetadata& other,
                                        size_t columnIndex) const {
  return permutedTriplesAgreeOnPrefix(lastTriple_, other.firstTriple_,
                                      columnIndex);
}

// _____________________________________________________________________________
const CompressedBlockMetadata::GraphInfo& SubBlockMetadata::graphInfo() const {
  static const CompressedBlockMetadata::GraphInfo noInfo = std::nullopt;
  if (permutationMetadata_ == nullptr) {
    return noInfo;
  }
  return completeBlock().graphInfo_;
}

// _____________________________________________________________________________
const CompressedBlockMetadata& SubBlockMetadata::completeBlock() const {
  AD_CONTRACT_CHECK(permutationMetadata_ != nullptr);
  return permutationMetadata_->blocks().at(blockIndex_);
}

// _____________________________________________________________________________
bool SubBlockMetadata::canBeMergedWith(const SubBlockMetadata& next) const {
  return permutationMetadata_ != nullptr &&
         permutationMetadata_ == next.permutationMetadata_ &&
         blockIndex_ == next.blockIndex_ &&
         subBlockIndex_ + numSubBlocks_ == next.subBlockIndex_;
}

// _____________________________________________________________________________
void SubBlockMetadata::mergeWithNext(const SubBlockMetadata& next) {
  AD_CONTRACT_CHECK(canBeMergedWith(next));
  lastTriple_ = next.lastTriple_;
  numRows_ += next.numRows_;
  containsDuplicatesWithDifferentGraphs_ |=
      next.containsDuplicatesWithDifferentGraphs_;
  numSubBlocks_ += next.numSubBlocks_;
}

// _____________________________________________________________________________
std::ostream& operator<<(std::ostream& str, const SubBlockMetadata& metadata) {
  str << "#SubBlockMetadata\n(first) " << metadata.firstTriple_ << "(last) "
      << metadata.lastTriple_ << "num. rows: " << metadata.numRows_ << ".\n"
      << "[possibly] contains duplicates: "
      << metadata.containsDuplicatesWithDifferentGraphs_ << '\n'
      << "block index: " << metadata.blockIndex_
      << ", sub-block index: " << metadata.subBlockIndex_
      << ", num. sub-blocks: " << metadata.numSubBlocks_ << '\n';
  return str;
}

// _____________________________________________________________________________
void SubBlockTable::appendBits(uint64_t value, size_t width) {
  AD_CORRECTNESS_CHECK(width <= 64);
  AD_CORRECTNESS_CHECK(width == 64 || value < (uint64_t{1} << width));
  if (width == 0) {
    return;
  }
  size_t offset = numBits_ % 64;
  if (offset == 0) {
    bits_.push_back(0);
  }
  bits_.back() |= value << offset;
  if (offset + width > 64) {
    // `offset > 0` holds here, so the shift is well-defined.
    bits_.push_back(value >> (64 - offset));
  }
  numBits_ += width;
}

// _____________________________________________________________________________
uint64_t SubBlockTable::readBits(size_t position, size_t width) const {
  if (width == 0) {
    return 0;
  }
  AD_EXPENSIVE_CHECK(position + width <= numBits_);
  size_t word = position / 64;
  size_t offset = position % 64;
  uint64_t result = bits_[word] >> offset;
  if (offset + width > 64) {
    result |= bits_[word + 1] << (64 - offset);
  }
  return width == 64 ? result : result & ((uint64_t{1} << width) - 1);
}

// _____________________________________________________________________________
void SubBlockTable::addBlock(ql::span<const SubBlock> subBlocks) {
  AD_CONTRACT_CHECK(!subBlocks.empty());
  // Compute the frame of reference and the widths.
  std::array<uint64_t, 3> bases;
  std::array<uint64_t, 3> maxValues;
  bases.fill(std::numeric_limits<uint64_t>::max());
  maxValues.fill(0);
  uint64_t maxNumRows = 0;
  std::vector<uint64_t> maxCompressedSizes(numColumns_, 0);
  for (const auto& subBlock : subBlocks) {
    for (size_t i = 0; i < 3; ++i) {
      uint64_t bits = subBlock.firstTriple_[i].getBits();
      bases[i] = std::min(bases[i], bits);
      maxValues[i] = std::max(maxValues[i], bits);
    }
    maxNumRows = std::max(maxNumRows, uint64_t{subBlock.numRows_});
    AD_CONTRACT_CHECK(subBlock.compressedSizes_.size() == numColumns_);
    for (size_t c = 0; c < numColumns_; ++c) {
      maxCompressedSizes[c] = std::max(maxCompressedSizes[c],
                                       uint64_t{subBlock.compressedSizes_[c]});
    }
    AD_CONTRACT_CHECK(subBlock.numColumnsSharedWithNext_ < 3);
  }
  std::array<size_t, 3> idWidths;
  for (size_t i = 0; i < 3; ++i) {
    idWidths[i] = bitWidth(maxValues[i] - bases[i]);
  }
  size_t numRowsWidth = bitWidth(maxNumRows);

  // Write the header.
  bitOffsetOfBlock_.push_back(numBits_);
  for (uint64_t base : bases) {
    appendBits(base, 64);
  }
  for (size_t width : idWidths) {
    appendBits(width, bitsPerWidth);
  }
  appendBits(numRowsWidth, bitsPerWidth);
  std::vector<size_t> compressedSizeWidths;
  for (uint64_t maxSize : maxCompressedSizes) {
    compressedSizeWidths.push_back(bitWidth(maxSize));
  }
  size_t recordWidth = idWidths[0] + idWidths[1] + idWidths[2] + numRowsWidth +
                       ::ranges::accumulate(compressedSizeWidths, size_t{0}) +
                       3;
  AD_CORRECTNESS_CHECK(recordWidth < (size_t{1} << bitsPerRecordWidth));
  appendBits(recordWidth, bitsPerRecordWidth);
  for (size_t width : compressedSizeWidths) {
    appendBits(width, bitsPerWidth);
  }

  // Write the records.
  for (const auto& subBlock : subBlocks) {
    for (size_t i = 0; i < 3; ++i) {
      appendBits(subBlock.firstTriple_[i].getBits() - bases[i], idWidths[i]);
    }
    appendBits(subBlock.numRows_, numRowsWidth);
    for (size_t c = 0; c < numColumns_; ++c) {
      appendBits(subBlock.compressedSizes_[c], compressedSizeWidths[c]);
    }
    appendBits(subBlock.numColumnsSharedWithNext_, 2);
    appendBits(subBlock.containsDuplicatesWithDifferentGraphs_, 1);
  }
  AD_CORRECTNESS_CHECK(numSubBlocks() + subBlocks.size() <=
                       std::numeric_limits<uint32_t>::max());
  firstSubBlockOfBlock_.push_back(
      static_cast<uint32_t>(numSubBlocks() + subBlocks.size()));
  computeSamples();
}

// _____________________________________________________________________________
void SubBlockTable::computeSamples() {
  size_t numSamples = (numSubBlocks() + sampleDistance - 1) / sampleDistance;
  // Only compute the samples that are missing (when called from `addBlock`,
  // at most one sample is missing).
  size_t blockIndex =
      blockOfSampledSubBlock_.empty() ? 0 : blockOfSampledSubBlock_.back();
  for (size_t i = blockOfSampledSubBlock_.size(); i < numSamples; ++i) {
    size_t subBlockIndex = i * sampleDistance;
    while (firstSubBlockOfBlock_[blockIndex + 1] <= subBlockIndex) {
      ++blockIndex;
    }
    blockOfSampledSubBlock_.push_back(static_cast<uint32_t>(blockIndex));
  }
}

// _____________________________________________________________________________
size_t SubBlockTable::blockOfSubBlock(size_t subBlockIndex) const {
  AD_CONTRACT_CHECK(subBlockIndex < numSubBlocks());
  size_t blockIndex = blockOfSampledSubBlock_[subBlockIndex / sampleDistance];
  while (firstSubBlockOfBlock_[blockIndex + 1] <= subBlockIndex) {
    ++blockIndex;
  }
  return blockIndex;
}

// _____________________________________________________________________________
auto SubBlockTable::readHeader(size_t blockIndex) const -> Header {
  Header header;
  size_t position = bitOffsetOfBlock_.at(blockIndex);
  for (auto& base : header.bases_) {
    base = readBits(position, 64);
    position += 64;
  }
  size_t widthOfTriple = 0;
  for (auto& width : header.idWidths_) {
    width = static_cast<uint8_t>(readBits(position, bitsPerWidth));
    widthOfTriple += width;
    position += bitsPerWidth;
  }
  header.numRowsWidth_ = static_cast<uint8_t>(readBits(position, bitsPerWidth));
  position += bitsPerWidth;
  header.compressedSizesOffset_ = widthOfTriple + header.numRowsWidth_;
  header.recordWidth_ = readBits(position, bitsPerRecordWidth);
  position += bitsPerRecordWidth;
  // Skip the widths of the compressed sizes.
  header.recordsBegin_ = position + numColumns_ * bitsPerWidth;
  return header;
}

// _____________________________________________________________________________
uint8_t SubBlockTable::compressedSizeWidth(size_t blockIndex,
                                           size_t column) const {
  size_t position = bitOffsetOfBlock_.at(blockIndex) + 3 * 64 +
                    (4 + column) * bitsPerWidth + bitsPerRecordWidth;
  return static_cast<uint8_t>(readBits(position, bitsPerWidth));
}

// _____________________________________________________________________________
auto SubBlockTable::get(size_t subBlockIndex) const -> DecodedSubBlock {
  size_t blockIndex = blockOfSubBlock(subBlockIndex);
  return decode(readHeader(blockIndex),
                subBlockIndex - firstSubBlockOfBlock_[blockIndex]);
}

// _____________________________________________________________________________
auto SubBlockTable::getWithContext(size_t subBlockIndex) const
    -> DecodedSubBlockWithContext {
  size_t blockIndex = blockOfSubBlock(subBlockIndex);
  auto [begin, end] = subBlocksOfBlock(blockIndex);
  auto header = readHeader(blockIndex);
  DecodedSubBlockWithContext result{decode(header, subBlockIndex - begin),
                                    blockIndex, begin, end, std::nullopt};
  if (subBlockIndex + 1 < end) {
    result.nextFirstTriple_ =
        decode(header, subBlockIndex + 1 - begin).firstTriple_;
  }
  return result;
}

// _____________________________________________________________________________
auto SubBlockTable::decode(const Header& header,
                           size_t indexInBlock) const -> DecodedSubBlock {
  size_t position = header.recordsBegin_ + indexInBlock * header.recordWidth_;
  DecodedSubBlock result;
  for (size_t i = 0; i < 3; ++i) {
    result.firstTriple_[i] = Id::fromBits(
        header.bases_[i] + readBits(position, header.idWidths_[i]));
    position += header.idWidths_[i];
  }
  result.numRows_ = readBits(position, header.numRowsWidth_);
  position += header.numRowsWidth_;
  // Skip the compressed sizes.
  position = position - header.compressedSizesOffset_ + header.recordWidth_ - 3;
  result.numColumnsSharedWithNext_ =
      static_cast<uint8_t>(readBits(position, 2));
  result.containsDuplicatesWithDifferentGraphs_ = readBits(position + 2, 1);
  return result;
}

// _____________________________________________________________________________
auto SubBlockTable::getWithCompressedSizes(size_t subBlockIndex) const
    -> SubBlock {
  auto decoded = get(subBlockIndex);
  SubBlock result{decoded.firstTriple_,
                  decoded.numRows_,
                  {},
                  decoded.numColumnsSharedWithNext_,
                  decoded.containsDuplicatesWithDifferentGraphs_};
  for (size_t c = 0; c < numColumns_; ++c) {
    result.compressedSizes_.push_back(
        getOffsetAndCompressedSize(subBlockIndex, c).second);
  }
  return result;
}

// _____________________________________________________________________________
std::pair<size_t, size_t> SubBlockTable::getOffsetAndCompressedSize(
    size_t subBlockIndex, size_t column) const {
  AD_CONTRACT_CHECK(column < numColumns_);
  size_t blockIndex = blockOfSubBlock(subBlockIndex);
  auto header = readHeader(blockIndex);
  size_t offsetInRecord = header.compressedSizesOffset_;
  for (size_t c = 0; c < column; ++c) {
    offsetInRecord += compressedSizeWidth(blockIndex, c);
  }
  size_t width = compressedSizeWidth(blockIndex, column);
  size_t firstSubBlock = firstSubBlockOfBlock_[blockIndex];
  // The offset is the sum of the compressed sizes of all the previous
  // sub-blocks of the same block.
  size_t offset = 0;
  size_t position = header.recordsBegin_ + offsetInRecord;
  for (size_t i = firstSubBlock; i < subBlockIndex; ++i) {
    offset += readBits(position, width);
    position += header.recordWidth_;
  }
  return {offset, readBits(position, width)};
}

// _____________________________________________________________________________
size_t SubBlockTable::getMemoryUsageInBytes() const {
  return bits_.size() * sizeof(uint64_t) +
         bitOffsetOfBlock_.size() * sizeof(uint64_t) +
         firstSubBlockOfBlock_.size() * sizeof(uint32_t) +
         blockOfSampledSubBlock_.size() * sizeof(uint32_t);
}

// _____________________________________________________________________________
PermutationBlockMetadata::PermutationBlockMetadata(
    std::shared_ptr<const Blocks> blocks,
    std::shared_ptr<const SubBlockTable> subBlocks,
    std::shared_ptr<const std::vector<bool>> blocksWithUpdates)
    : blocks_{std::move(blocks)},
      subBlocks_{std::move(subBlocks)},
      blocksWithUpdates_{std::move(blocksWithUpdates)} {
  AD_CONTRACT_CHECK(blocks_ != nullptr && subBlocks_ != nullptr &&
                    blocksWithUpdates_ != nullptr);
  // Only the last block (that consists of `LocatedTriples` only) may be
  // missing in a non-empty `SubBlockTable`.
  AD_CONTRACT_CHECK(subBlocks_->empty() ||
                    subBlocks_->numBlocks() == blocks_->size() ||
                    subBlocks_->numBlocks() + 1 == blocks_->size());
  if constexpr (ad_utility::areExpensiveChecksEnabled) {
    checkInvariants();
  }
}

// _____________________________________________________________________________
void PermutationBlockMetadata::checkInvariants() const {
  auto toString = [](const SubBlockMetadata& subBlock) {
    std::ostringstream str;
    str << subBlock;
    return std::move(str).str();
  };
  std::optional<SubBlockMetadata> previous;
  for (size_t i = 0; i < numSubBlocks(); ++i) {
    auto current = subBlock(i);
    AD_CORRECTNESS_CHECK(current.firstTriple_ <= current.lastTriple_, [&]() {
      return absl::StrCat("The bounds of a sub-block are not sorted:\n",
                          toString(current));
    });
    if (previous.has_value()) {
      AD_CORRECTNESS_CHECK(
          previous->lastTriple_ <= current.firstTriple_, [&]() {
            return absl::StrCat("Two consecutive sub-blocks are not sorted:\n",
                                toString(previous.value()), toString(current));
          });
    }
    previous = std::move(current);
  }
}

// _____________________________________________________________________________
PermutationBlockMetadata PermutationBlockMetadata::withAugmentedBlocks(
    Blocks blocks, std::vector<bool> blocksWithUpdates) const {
  AD_CONTRACT_CHECK(blocks.size() == blocks_->size() ||
                    blocks.size() == blocks_->size() + 1);
  return PermutationBlockMetadata{
      std::make_shared<const Blocks>(std::move(blocks)), subBlocks_,
      std::make_shared<const std::vector<bool>>(std::move(blocksWithUpdates))};
}

// _____________________________________________________________________________
size_t PermutationBlockMetadata::numSubBlocks() const {
  return subBlocks_->numSubBlocks() + blocks_->size() - subBlocks_->numBlocks();
}

// _____________________________________________________________________________
std::pair<size_t, size_t> PermutationBlockMetadata::subBlocksOfBlock(
    size_t blockIndex) const {
  AD_CONTRACT_CHECK(blockIndex < blocks_->size());
  const auto& table = *subBlocks_;
  if (blockIndex < table.numBlocks()) {
    return table.subBlocksOfBlock(blockIndex);
  }
  size_t subBlockIndex =
      table.numSubBlocks() + (blockIndex - table.numBlocks());
  return {subBlockIndex, subBlockIndex + 1};
}

// _____________________________________________________________________________
SubBlockMetadata PermutationBlockMetadata::subBlock(
    size_t subBlockIndex) const {
  const auto& table = *subBlocks_;
  // Handle a block that consists of a single sub-block.
  if (subBlockIndex >= table.numSubBlocks()) {
    size_t blockIndex =
        table.numBlocks() + (subBlockIndex - table.numSubBlocks());
    const auto& block = blocks_->at(blockIndex);
    return {block.firstTriple_,
            block.lastTriple_,
            block.numRows_,
            block.containsDuplicatesWithDifferentGraphs_,
            blockIndex,
            subBlockIndex,
            1,
            this};
  }
  auto [decoded, blockIndex, begin, end, next] =
      table.getWithContext(subBlockIndex);
  const auto& block = (*blocks_)[blockIndex];
  bool hasUpdates = blockHasUpdates(blockIndex);

  SubBlockMetadata result;
  result.firstTriple_ =
      subBlockIndex == begin
          ? block.firstTriple_
          : PermutedTriple{decoded.firstTriple_[0], decoded.firstTriple_[1],
                           decoded.firstTriple_[2], Id::min()};
  if (!next.has_value()) {
    result.lastTriple_ = block.lastTriple_;
  } else {
    // For blocks with updates, see the comment for `blocksWithUpdates_`.
    const auto& nextFirst = next.value();
    result.lastTriple_ =
        hasUpdates
            ? PermutedTriple{nextFirst[0], nextFirst[1], nextFirst[2],
                             Id::min()}
            : largestTripleBefore(nextFirst, decoded.numColumnsSharedWithNext_);
  }
  result.numRows_ = decoded.numRows_;
  // The duplicates that are introduced by `LocatedTriples` are only tracked
  // per block (see `updateGraphMetadata` in `LocatedTriples.cpp`).
  result.containsDuplicatesWithDifferentGraphs_ =
      decoded.containsDuplicatesWithDifferentGraphs_ ||
      (hasUpdates && block.containsDuplicatesWithDifferentGraphs_);
  result.blockIndex_ = blockIndex;
  result.subBlockIndex_ = subBlockIndex;
  result.numSubBlocks_ = 1;
  result.permutationMetadata_ = this;
  return result;
}

// _____________________________________________________________________________
SubBlockMetadataSpan PermutationBlockMetadata::subBlocks() const {
  return SubBlockMetadataSpan{*this};
}

// _____________________________________________________________________________
SubBlockMetadata::OffsetAndCompressedSize
PermutationBlockMetadata::getOffsetAndCompressedSize(
    const SubBlockMetadata& subBlocks, ColumnIndex column) const {
  const auto& block = blocks_->at(subBlocks.blockIndex_);
  auto locationOfBlock = block.getOffsetAndCompressedSizeForColumn(column);
  auto [begin, end, first, last] = getPosition(subBlocks);
  if (first == begin && last + 1 == end) {
    return locationOfBlock;
  }
  auto [offset, firstSize] =
      subBlocks_->getOffsetAndCompressedSize(first, column);
  auto [offsetOfLast, lastSize] =
      first == last ? std::pair{offset, firstSize}
                    : subBlocks_->getOffsetAndCompressedSize(last, column);
  return {locationOfBlock.offsetInFile_ + static_cast<off_t>(offset),
          offsetOfLast + lastSize - offset};
}

// _____________________________________________________________________________
auto PermutationBlockMetadata::getPosition(
    const SubBlockMetadata& subBlocks) const -> Position {
  auto [begin, end] = subBlocksOfBlock(subBlocks.blockIndex_);
  size_t first = subBlocks.subBlockIndex_;
  size_t last = first + subBlocks.numSubBlocks_ - 1;
  AD_CONTRACT_CHECK(begin <= first && last < end);
  return {begin, end, first, last};
}

// _____________________________________________________________________________
SubBlockTripleRange PermutationBlockMetadata::getTripleRange(
    const SubBlockMetadata& subBlocks) const {
  auto [begin, end, first, last] = getPosition(subBlocks);
  SubBlockTripleRange result;
  if (first != begin) {
    result.begin_ = subBlocks_->get(first).firstTriple_;
  }
  if (last + 1 != end) {
    result.end_ = subBlocks_->get(last + 1).firstTriple_;
  }
  return result;
}
