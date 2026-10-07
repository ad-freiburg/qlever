// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "libqlever/BlobDiff.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <optional>
#include <variant>

#include "backports/algorithm.h"
#include "libqlever/NamedCachedQueryBlobManager.h"
#include "util/Exception.h"
#include "util/HashMap.h"
#include "util/Serializer/ByteBufferSerializer.h"

namespace qlever {

namespace {
using ad_utility::BinaryDiffApplier;
using detail::noBaseRow;
using Manager = NamedCachedQueryBlobManager;

constexpr std::array<char, 8> diffFileMagic{'Q', 'L', 'V', 'R',
                                            'B', 'D', 'I', 'F'};
constexpr uint16_t diffFileVersion = 1;

// Return the columns of the table of `entry`, which are views into `blob`.
std::vector<ql::span<const Id>> idColumnsOf(ql::span<const char> blob,
                                            const EntryLayout& entry) {
  std::vector<ql::span<const Id>> columns;
  for (const auto& range : entry.columnPayloads_) {
    AD_CONTRACT_CHECK(range.end_ <= blob.size() && range.begin_ % 8 == 0);
    AD_CONTRACT_CHECK(
        reinterpret_cast<std::uintptr_t>(blob.data()) % alignof(Id) == 0);
    columns.emplace_back(
        reinterpret_cast<const Id*>(blob.data() + range.begin_),
        entry.numRows_);
  }
  return columns;
}

// Return a copy of the `uint64_t` array with the payload `range` of `blob`.
std::vector<uint64_t> readU64Array(ql::span<const char> blob, ByteRange range) {
  AD_CONTRACT_CHECK(range.end_ <= blob.size());
  std::vector<uint64_t> result(range.size() / sizeof(uint64_t));
  if (!result.empty()) {
    std::memcpy(result.data(), blob.data() + range.begin_, range.size());
  }
  return result;
}

// Build the instructions of a diff by walking the target from front to back.
class DiffBuilder {
  ql::span<const char> base_;
  ql::span<const char> target_;
  BinaryDiffApplier diff_;
  // All bytes of the target before this offset are already covered.
  uint64_t cursor_ = 0;
  // The offset in the base directly after the last copied range, if the last
  // instruction was a copy. It is used to turn literal bytes that happen to
  // continue the copied range of the base into a copy as well, such that
  // unchanged parts of the blob result in a single copy instruction.
  std::optional<uint64_t> nextBaseOffset_ = 0;

 public:
  DiffBuilder(ql::span<const char> base, ql::span<const char> target)
      : base_{base}, target_{target}, diff_{base} {}

  BinaryDiffApplier finish() && {
    insertUntil(target_.size());
    AD_CORRECTNESS_CHECK(diff_.targetSize() == target_.size());
    return std::move(diff_);
  }

  // Insert the literal target bytes up to (excluding) `position`.
  void insertUntil(uint64_t position) {
    AD_CORRECTNESS_CHECK(position >= cursor_ && position <= target_.size());
    if (position > cursor_) {
      emitLiteral(target_.subspan(cursor_, position - cursor_));
      cursor_ = position;
    }
  }

  // Cover the target range `targetRange`: copy the base range `baseRange` if
  // it is given and has exactly the same bytes, and insert the bytes
  // otherwise.
  void copyOrInsert(ByteRange targetRange,
                    std::optional<ByteRange> baseRange = std::nullopt) {
    insertUntil(targetRange.begin_);
    if (targetRange.size() == 0) {
      return;
    }
    if (baseRange.has_value() && baseRange->size() == targetRange.size() &&
        baseRange->end_ <= base_.size() &&
        std::memcmp(base_.data() + baseRange->begin_,
                    target_.data() + targetRange.begin_,
                    targetRange.size()) == 0) {
      emitCopy(baseRange->begin_, baseRange->size());
    } else {
      emitInsert(target_.subspan(targetRange.begin_, targetRange.size()));
    }
    cursor_ = targetRange.end_;
  }

  // Cover the array of 8-byte elements with the payload `targetRange` of the
  // target. Element `i` is copied from element `basePositions[i]` of the array
  // with payload `baseRange` of the base, or inserted if that is `noBaseRow`.
  void emitRuns(ByteRange targetRange, ByteRange baseRange,
                ql::span<const size_t> basePositions) {
    constexpr size_t elementSize = sizeof(uint64_t);
    insertUntil(targetRange.begin_);
    size_t numElements = targetRange.size() / elementSize;
    AD_CORRECTNESS_CHECK(basePositions.size() == numElements);
    size_t runStart = 0;
    auto flush = [&](size_t runEnd) {
      if (runEnd == runStart) {
        return;
      }
      size_t length = (runEnd - runStart) * elementSize;
      if (basePositions[runStart] == noBaseRow) {
        emitInsert(target_.subspan(targetRange.begin_ + runStart * elementSize,
                                   length));
      } else {
        AD_CORRECTNESS_CHECK(
            baseRange.begin_ + (basePositions[runStart] + 1) * elementSize <=
                baseRange.end_ &&
            baseRange.begin_ + (basePositions[runEnd - 1] + 1) * elementSize <=
                baseRange.end_);
        emitCopy(baseRange.begin_ + basePositions[runStart] * elementSize,
                 length);
      }
      runStart = runEnd;
    };
    for (size_t i = 1; i < numElements; ++i) {
      bool prevMatched = basePositions[i - 1] != noBaseRow;
      bool matched = basePositions[i] != noBaseRow;
      bool continues =
          prevMatched
              ? (matched && basePositions[i] == basePositions[i - 1] + 1)
              : !matched;
      if (!continues) {
        flush(i);
      }
    }
    flush(numElements);
    cursor_ = targetRange.end_;
  }

  // Cover the secondary vocabulary.
  void secondaryVocab(const BlobLayout& baseLayout,
                      const BlobLayout& targetLayout) {
    const auto& t = targetLayout.secondaryVocab_;
    const auto& b = baseLayout.secondaryVocab_;
    if (!t.present_) {
      return;
    }
    for (size_t i = 0; i < t.segments_.size(); ++i) {
      std::optional<ByteRange> baseSegment;
      if (b.present_ && i < b.segments_.size()) {
        baseSegment = b.segments_[i];
      }
      copyOrInsert(t.segments_[i], baseSegment);
    }
    if (!b.present_) {
      return;
    }
    copyOrInsert(t.segmentOffsets_, b.segmentOffsets_);
    auto baseIndices = readU64Array(base_, b.sortedIndices_);
    auto targetIndices = readU64Array(target_, t.sortedIndices_);
    emitRuns(t.sortedIndices_, b.sortedIndices_,
             detail::greedyMatch(baseIndices, targetIndices));
  }

  // Cover the named cache entry `t` of the target, for which `b` is the entry
  // with the same key of the base (or `nullptr`).
  void entry(const EntryLayout& t, const EntryLayout* b,
             uint16_t targetEntriesVersion, uint16_t baseEntriesVersion) {
    insertUntil(t.whole_.begin_);
    if (b != nullptr) {
      // Fast path for an unchanged entry.
      std::optional<ByteRange> baseRange = b->whole_;
      if (baseRange->size() == t.whole_.size()) {
        copyOrInsertIfEqual(t.whole_, *baseRange);
        if (cursor_ == t.whole_.end_) {
          return;
        }
      }
    }
    // Row merge, if both tables are canonical.
    std::vector<size_t> rowMapping;
    bool canMerge = false;
    if (b != nullptr && b->numColumns_ == t.numColumns_ && t.numColumns_ > 0 &&
        b->sortedOnColumns_ == t.sortedOnColumns_) {
      auto baseColumns = idColumnsOf(base_, *b);
      auto targetColumns = idColumnsOf(target_, t);
      if (detail::isCanonicallySorted(baseColumns, b->sortedOnColumns_) &&
          detail::isCanonicallySorted(targetColumns, t.sortedOnColumns_)) {
        canMerge = true;
        rowMapping =
            detail::mergeRows(baseColumns, targetColumns, t.sortedOnColumns_);
      }
    }
    for (size_t c = 0; c < t.columnPayloads_.size(); ++c) {
      if (canMerge) {
        emitRuns(t.columnPayloads_[c], b->columnPayloads_[c], rowMapping);
      } else {
        std::optional<ByteRange> baseColumn;
        if (b != nullptr && c < b->columnPayloads_.size()) {
          baseColumn = b->columnPayloads_[c];
        }
        copyOrInsert(t.columnPayloads_[c], baseColumn);
      }
    }
    if (!t.hasGeoIndex_) {
      return;
    }
    bool baseHasGeo = b != nullptr && b->hasGeoIndex_;
    if (targetEntriesVersion == 2 && baseHasGeo && baseEntriesVersion == 2) {
      for (size_t i = 0; i < t.geo_.segmentPayloads_.size(); ++i) {
        std::optional<ByteRange> baseSegment;
        if (i < b->geo_.segmentPayloads_.size()) {
          baseSegment = b->geo_.segmentPayloads_[i];
        }
        copyOrInsert(t.geo_.segmentPayloads_[i], baseSegment);
      }
      auto baseShapes = readU64Array(base_, b->geo_.rowToShape_);
      auto targetShapes = readU64Array(target_, t.geo_.rowToShape_);
      std::vector<size_t> shapePositions;
      if (canMerge) {
        shapePositions = rowMapping;
        for (size_t i = 0; i < shapePositions.size(); ++i) {
          if (shapePositions[i] != noBaseRow &&
              baseShapes[shapePositions[i]] != targetShapes[i]) {
            shapePositions[i] = noBaseRow;
          }
        }
      } else {
        shapePositions = detail::greedyMatch(baseShapes, targetShapes);
      }
      emitRuns(t.geo_.rowToShape_, b->geo_.rowToShape_, shapePositions);
    } else {
      std::optional<ByteRange> baseGeo;
      if (baseHasGeo) {
        baseGeo = b->geo_.whole_;
      }
      copyOrInsert(t.geo_.whole_, baseGeo);
    }
  }

 private:
  void emitCopy(uint64_t baseOffset, uint64_t length) {
    diff_.addCopy(baseOffset, length);
    nextBaseOffset_ = baseOffset + length;
  }
  void emitInsert(ql::span<const char> bytes) {
    diff_.addInsert(bytes);
    nextBaseOffset_ = std::nullopt;
  }
  // Emit the literal target bytes `bytes`, as a copy if they continue the
  // last copied range of the base, and as an insert otherwise.
  void emitLiteral(ql::span<const char> bytes) {
    if (nextBaseOffset_.has_value() &&
        nextBaseOffset_.value() + bytes.size() <= base_.size() &&
        std::memcmp(base_.data() + nextBaseOffset_.value(), bytes.data(),
                    bytes.size()) == 0) {
      emitCopy(nextBaseOffset_.value(), bytes.size());
    } else {
      emitInsert(bytes);
    }
  }

  // Like `copyOrInsert`, but do nothing if the bytes are not equal.
  void copyOrInsertIfEqual(ByteRange targetRange, ByteRange baseRange) {
    if (baseRange.end_ <= base_.size() &&
        std::memcmp(base_.data() + baseRange.begin_,
                    target_.data() + targetRange.begin_,
                    targetRange.size()) == 0) {
      copyOrInsert(targetRange, baseRange);
    }
  }
};
}  // namespace

namespace detail {
namespace {
// Return the order in which the columns are compared, or `std::nullopt` if
// `sortedOn` is invalid.
std::optional<std::vector<size_t>> comparisonOrder(
    size_t numColumns, ql::span<const uint64_t> sortedOn) {
  std::vector<size_t> order;
  std::vector<bool> used(numColumns, false);
  for (uint64_t column : sortedOn) {
    if (column >= numColumns) {
      return std::nullopt;
    }
    if (!used[column]) {
      used[column] = true;
      order.push_back(column);
    }
  }
  for (size_t column = 0; column < numColumns; ++column) {
    if (!used[column]) {
      order.push_back(column);
    }
  }
  return order;
}

// Lexicographically compare row `i` of `a` with row `j` of `b` in the given
// column `order`, comparing the bits of the `Id`s.
int compareRows(IdColumns a, size_t i, IdColumns b, size_t j,
                const std::vector<size_t>& order) {
  for (size_t column : order) {
    auto x = a[column][i].getBits();
    auto y = b[column][j].getBits();
    if (x != y) {
      return x < y ? -1 : 1;
    }
  }
  return 0;
}
}  // namespace

// _____________________________________________________________________________
bool isCanonicallySorted(IdColumns columns, ql::span<const uint64_t> sortedOn) {
  auto order = comparisonOrder(columns.size(), sortedOn);
  if (!order.has_value()) {
    return false;
  }
  size_t numRows = columns.empty() ? 0 : columns[0].size();
  for (size_t i = 1; i < numRows; ++i) {
    if (compareRows(columns, i - 1, columns, i, order.value()) > 0) {
      return false;
    }
  }
  return true;
}

// _____________________________________________________________________________
std::vector<size_t> mergeRows(IdColumns base, IdColumns target,
                              ql::span<const uint64_t> sortedOn) {
  AD_CONTRACT_CHECK(base.size() == target.size());
  auto order = comparisonOrder(base.size(), sortedOn);
  AD_CONTRACT_CHECK(order.has_value());
  size_t numBase = base.empty() ? 0 : base[0].size();
  size_t numTarget = target.empty() ? 0 : target[0].size();
  std::vector<size_t> result(numTarget, noBaseRow);
  size_t i = 0;
  size_t j = 0;
  while (i < numBase && j < numTarget) {
    int cmp = compareRows(base, i, target, j, order.value());
    if (cmp < 0) {
      ++i;
    } else if (cmp > 0) {
      ++j;
    } else {
      result[j] = i;
      ++i;
      ++j;
    }
  }
  return result;
}

// _____________________________________________________________________________
std::vector<size_t> greedyMatch(ql::span<const uint64_t> base,
                                ql::span<const uint64_t> target) {
  std::vector<size_t> result(target.size(), noBaseRow);
  size_t j = 0;
  for (size_t i = 0; i < target.size(); ++i) {
    if (j < base.size() && base[j] == target[i]) {
      result[i] = j++;
    }
  }
  return result;
}
}  // namespace detail

// _____________________________________________________________________________
BinaryDiffApplier computeBlobDiff(ql::span<const char> base,
                                  const BlobLayout& baseLayout,
                                  ql::span<const char> target,
                                  const BlobLayout& targetLayout) {
  AD_CONTRACT_CHECK(baseLayout.totalSize_ == base.size());
  AD_CONTRACT_CHECK(targetLayout.totalSize_ == target.size());
  DiffBuilder builder{base, target};
  builder.copyOrInsert(targetLayout.metadata_, baseLayout.metadata_);
  builder.copyOrInsert(targetLayout.vocabulary_, baseLayout.vocabulary_);
  builder.secondaryVocab(baseLayout, targetLayout);

  ad_utility::HashMap<std::string_view, const EntryLayout*> baseEntries;
  for (const auto& entry : baseLayout.entryLayouts_) {
    baseEntries.emplace(entry.key_, &entry);
  }
  for (const auto& entry : targetLayout.entryLayouts_) {
    auto it = baseEntries.find(entry.key_);
    builder.entry(entry, it == baseEntries.end() ? nullptr : it->second,
                  targetLayout.entriesVersion_, baseLayout.entriesVersion_);
  }
  return std::move(builder).finish();
}

// _____________________________________________________________________________
std::vector<char> serializeBlobDiffToFile(const BinaryDiffApplier& diff) {
  ad_utility::serialization::AlignedByteBufferWriteSerializer writer;
  writer << diffFileMagic;
  writer << diffFileVersion;
  writer << diff;
  auto data = std::move(writer).data();
  return Manager::compressBlob(ql::span<const char>{data});
}

// _____________________________________________________________________________
BinaryDiffApplier readBlobDiffFromFile(ql::span<const char> compressedDiff) {
  auto decompressed = Manager::tryToDecompressBlob(compressedDiff, {});
  if (auto* error = std::get_if<Manager::BlobError>(&decompressed)) {
    AD_THROW(absl::StrCat("The given blob diff file cannot be decompressed: ",
                          error->message_));
  }
  auto& buffer =
      std::get<std::vector<char, Manager::BlobAllocator>>(decompressed);
  ad_utility::serialization::ByteBufferReadSerializerT<true,
                                                       ql::span<const char>>
      reader{ql::span<const char>{buffer}};
  std::array<char, 8> magic{};
  uint16_t version = 0;
  reader >> magic;
  AD_CONTRACT_CHECK(magic == diffFileMagic,
                    "The given file is not a blob diff file");
  reader >> version;
  AD_CONTRACT_CHECK(version == diffFileVersion,
                    "The blob diff file has the unsupported format version ",
                    version);
  BinaryDiffApplier diff;
  reader >> diff;
  return diff;
}

// _____________________________________________________________________________
std::vector<char> applyBlobDiff(ql::span<const char> compressedBaseBlob,
                                ql::span<const char> compressedDiff) {
  auto diff = readBlobDiffFromFile(compressedDiff);
  auto base = Manager::tryToDecompressBlob(compressedBaseBlob, {});
  if (auto* error = std::get_if<Manager::BlobError>(&base)) {
    AD_THROW(absl::StrCat("The base blob cannot be decompressed: ",
                          error->message_));
  }
  auto& baseBuffer = std::get<std::vector<char, Manager::BlobAllocator>>(base);
  auto target = diff.apply(ql::span<const char>{baseBuffer});
  return Manager::compressBlob(ql::span<const char>{target});
}

// _____________________________________________________________________________
const BlobDiffStatistics::Section& BlobDiffStatistics::section(
    std::string_view name) const {
  auto it = ql::ranges::find(sections_, name, &Section::name_);
  AD_CONTRACT_CHECK(it != sections_.end(), "No section with the name ", name);
  return *it;
}

// _____________________________________________________________________________
std::string BlobDiffStatistics::toString() const {
  std::string result = absl::StrFormat(
      "%d copy, %d insert, %d align instructions; %d bytes copied, %d bytes "
      "inserted\n",
      instructions_.numCopyInstructions_, instructions_.numInsertInstructions_,
      instructions_.numAlignInstructions_, instructions_.numCopiedBytes_,
      instructions_.numInsertedBytes_);
  for (const auto& s : sections_) {
    absl::StrAppendFormat(&result,
                          "%-48s size %12d, copied %12d, inserted %12d\n",
                          s.name_, s.size_, s.copiedBytes_, s.insertedBytes_);
  }
  return result;
}

namespace {
// A range of the target that belongs to the section with index `section_`.
struct Leaf {
  uint64_t begin_;
  uint64_t end_;
  size_t section_;
};
}  // namespace

// _____________________________________________________________________________
BlobDiffStatistics computeBlobDiffStatistics(const BinaryDiffApplier& diff,
                                             const BlobLayout& targetLayout) {
  AD_CONTRACT_CHECK(diff.targetSize() == targetLayout.totalSize_,
                    "The diff does not belong to the given target layout");
  BlobDiffStatistics result;
  result.instructions_ = diff.statistics();
  std::vector<Leaf> leaves;
  uint64_t cursor = 0;
  auto sectionIndex = [&result](std::string_view name) {
    auto it = ql::ranges::find(result.sections_, name,
                               &BlobDiffStatistics::Section::name_);
    if (it == result.sections_.end()) {
      result.sections_.push_back({std::string{name}});
      return result.sections_.size() - 1;
    }
    return static_cast<size_t>(it - result.sections_.begin());
  };
  // Add `range` as a leaf of the section `name`; the gap before it becomes a
  // leaf of the section `fill`.
  auto addLeaf = [&](ByteRange range, std::string_view name,
                     std::string_view fill = "framing") {
    if (range.begin_ > cursor) {
      leaves.push_back({cursor, range.begin_, sectionIndex(fill)});
    }
    if (range.end_ > range.begin_) {
      leaves.push_back({range.begin_, range.end_, sectionIndex(name)});
    }
    cursor = std::max(cursor, range.end_);
  };
  // Make sure that the sections appear in layout order.
  sectionIndex("framing");
  addLeaf(targetLayout.metadata_, "metadata");
  addLeaf(targetLayout.vocabulary_, "main vocabulary");
  if (targetLayout.secondaryVocab_.present_) {
    addLeaf(targetLayout.secondaryVocab_.whole_, "secondary vocabulary");
  }
  for (const auto& entry : targetLayout.entryLayouts_) {
    std::string columns = absl::StrCat("entry ", entry.key_, ": columns");
    std::string geo = absl::StrCat("entry ", entry.key_, ": geo index");
    std::string other = absl::StrCat("entry ", entry.key_, ": other");
    // The beginning of the entry is not framing, but belongs to the entry.
    if (entry.whole_.begin_ > cursor) {
      leaves.push_back({cursor, entry.whole_.begin_, sectionIndex("framing")});
      cursor = entry.whole_.begin_;
    }
    for (const auto& column : entry.columnPayloads_) {
      addLeaf(column, columns, other);
    }
    if (entry.hasGeoIndex_) {
      addLeaf(entry.geo_.whole_, geo, other);
    }
    addLeaf({entry.whole_.end_, entry.whole_.end_}, other, other);
  }
  addLeaf({targetLayout.totalSize_, targetLayout.totalSize_}, "framing");
  for (const auto& leaf : leaves) {
    result.sections_[leaf.section_].size_ += leaf.end_ - leaf.begin_;
  }

  size_t leafIndex = 0;
  uint64_t offset = 0;
  auto account = [&](uint64_t length,
                     uint64_t BlobDiffStatistics::Section::*counter) {
    uint64_t end = offset + length;
    while (offset < end) {
      while (leafIndex < leaves.size() && leaves[leafIndex].end_ <= offset) {
        ++leafIndex;
      }
      AD_CORRECTNESS_CHECK(leafIndex < leaves.size() &&
                           leaves[leafIndex].begin_ <= offset);
      uint64_t stop = std::min(end, leaves[leafIndex].end_);
      result.sections_[leaves[leafIndex].section_].*counter += stop - offset;
      offset = stop;
    }
  };
  for (const auto& instruction : diff.instructions()) {
    if (const auto* copy = std::get_if<BinaryDiffApplier::Copy>(&instruction)) {
      account(copy->length_, &BlobDiffStatistics::Section::copiedBytes_);
    } else if (const auto* insert =
                   std::get_if<BinaryDiffApplier::Insert>(&instruction)) {
      account(insert->bytes_.size(),
              &BlobDiffStatistics::Section::insertedBytes_);
    } else {
      uint64_t alignment =
          std::get<BinaryDiffApplier::Align>(instruction).alignment_;
      uint64_t padding = (alignment - offset % alignment) % alignment;
      account(padding, &BlobDiffStatistics::Section::paddedBytes_);
    }
  }
  return result;
}

// _____________________________________________________________________________
std::string describeBlobDiff(const BinaryDiffApplier& diff,
                             const BlobLayout& targetLayout) {
  return computeBlobDiffStatistics(diff, targetLayout).toString();
}

}  // namespace qlever
