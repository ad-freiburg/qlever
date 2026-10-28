// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_SUBBLOCKMETADATA_H
#define QLEVER_SRC_INDEX_SUBBLOCKMETADATA_H

#include <array>
#include <cstdint>
#include <iterator>
#include <memory>
#include <optional>
#include <ostream>
#include <tuple>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "backports/span.h"
#include "backports/three_way_comparison.h"
#include "global/Id.h"
#include "index/CompressedRelationMetadata.h"
#include "util/Exception.h"
#include "util/Serializer/SerializeVector.h"
#include "util/Serializer/Serializer.h"

// OVERVIEW
//
// The blocks of a permutation (described by `CompressedBlockMetadata`) are
// split into smaller sub-blocks, which can be read from disk in isolation. The
// sub-blocks make the selection of blocks (for a scan, for a join, or for a
// `PrefilterExpression`) more fine-grained, without the memory overhead of the
// metadata of many small `CompressedBlockMetadata`s:
//
// 1. Each column of a block is stored as the concatenation of the compressed
//    columns of its sub-blocks, so the per-column `OffsetAndCompressedSize`
//    of a `CompressedBlockMetadata` still describes the complete column of
//    the block.
// 2. Everything that is expensive (in particular the graph info) and
//    everything that can change because of updates (the `LocatedTriples` are
//    still stored per block) remains at the `CompressedBlockMetadata`. The
//    sub-blocks therefore never change after the index has been built, and
//    their metadata is stored in a bit-packed `SubBlockTable`.
// 3. The interface to all of this is the `PermutationBlockMetadata`, which
//    yields one `SubBlockMetadata` (a small value type) per sub-block. All the
//    code that selects blocks operates on those. Indices that have been built
//    without sub-blocks (including all indices that have been built before
//    sub-blocks were introduced) are represented by an empty `SubBlockTable`,
//    in which case each block consists of exactly one sub-block.

class PermutationBlockMetadata;

// The metadata of a sub-block (or of a range of consecutive sub-blocks of the
// same block, see `numSubBlocks_`) as it is used by the scans. Values of this
// type are not stored permanently, but computed on the fly by
// `PermutationBlockMetadata`.
struct SubBlockMetadata {
  using PermutedTriple = CompressedBlockMetadata::PermutedTriple;
  using OffsetAndCompressedSize =
      CompressedBlockMetadata::OffsetAndCompressedSize;

  // A lower bound for the first triple of the sub-block, and an upper bound
  // for its last triple (both inclusive). The bounds are exact for the first
  // triple of the first sub-block and for the last triple of the last
  // sub-block of a block. All other bounds are only exact when ignoring the
  // graph ID (for the first triple), or in the columns that the last triple
  // shares with the first triple of the next sub-block (for the last triple,
  // see `SubBlockTable` for details). This is all that the selection of
  // blocks (which ignores the graph IDs) requires.
  PermutedTriple firstTriple_;
  PermutedTriple lastTriple_;
  // The number of rows that are stored on disk for this sub-block.
  size_t numRows_ = 0;
  // True if this sub-block may contain (adjacent) triples which only differ in
  // their graph ID.
  bool containsDuplicatesWithDifferentGraphs_ = false;
  // The index of the block to which this sub-block belongs. The
  // `LocatedTriples` are stored per block, so this is the index that is used
  // to look them up.
  size_t blockIndex_ = 0;
  // The global index of the sub-block in the permutation. It is unique and
  // ascending, and the sub-blocks of the same block have consecutive indices.
  size_t subBlockIndex_ = 0;
  // The number of consecutive sub-blocks (starting at `subBlockIndex_`, all in
  // the same block) that are described by this value. This is `1` unless
  // several sub-blocks have been merged via `mergeWithNext`, which is done to
  // read them from disk at once.
  size_t numSubBlocks_ = 1;
  // The metadata from which this value was obtained. It is used to access the
  // information that is stored per block (graph info, offsets in the file,
  // the boundaries of the sub-blocks for the `LocatedTriples`). It may be
  // `nullptr` (only in unit tests), then this value describes a complete
  // block without graph info that cannot be read from disk.
  const PermutationBlockMetadata* permutationMetadata_ = nullptr;

  // Return `true` iff the column values of `firstTriple_` and `lastTriple_`
  // differ in one of the columns `< columnIndex`.
  bool containsInconsistentTriples(size_t columnIndex) const;

  // Return `true` iff `lastTriple_` of this sub-block and `firstTriple_` of
  // `other` agree on all the columns `< columnIndex`.
  bool isConsistentWith(const SubBlockMetadata& other,
                        size_t columnIndex) const;

  // Return the graph info of the block to which this sub-block belongs (see
  // `CompressedBlockMetadata::graphInfo_`). Without `permutationMetadata_` this
  // is `std::nullopt` (no information).
  const CompressedBlockMetadata::GraphInfo& graphInfo() const;

  // Return the metadata of the block to which this sub-block belongs. The
  // `permutationMetadata_` must be set.
  const CompressedBlockMetadata& completeBlock() const;

  // Return `true` iff `next` describes the sub-blocks that directly follow the
  // sub-blocks of this value in the same block.
  bool canBeMergedWith(const SubBlockMetadata& next) const;

  // Extend this value by the directly following sub-blocks in `next`, see
  // `canBeMergedWith`, which must be true.
  void mergeWithNext(const SubBlockMetadata& next);

  // Two of these are equal if all members except for the
  // `permutationMetadata_` are equal.
  bool operator==(const SubBlockMetadata& other) const {
    auto tie = [](const SubBlockMetadata& m) {
      return std::tie(m.firstTriple_, m.lastTriple_, m.numRows_,
                      m.containsDuplicatesWithDifferentGraphs_, m.blockIndex_,
                      m.subBlockIndex_, m.numSubBlocks_);
    };
    return tie(*this) == tie(other);
  }
  bool operator!=(const SubBlockMetadata& other) const {
    return !(*this == other);
  }

  // Write a human-readable representation of the `metadata` to `str` (for
  // debugging).
  friend std::ostream& operator<<(std::ostream& str,
                                  const SubBlockMetadata& metadata);
};

// The range of triples (when ignoring the graph) that belong to a range of
// sub-blocks of the same block. Both bounds are optional, `std::nullopt` means
// "unbounded" (which is the case at the borders of the block). The range is
// half-open, that is, a triple `t` belongs to the range iff `begin_ <= t <
// end_`. This range is used to assign the `LocatedTriples` of a block to its
// sub-blocks.
struct SubBlockTripleRange {
  std::optional<std::array<Id, 3>> begin_;
  std::optional<std::array<Id, 3>> end_;

  // Return `true` iff the triple, given by its first three `Id`s, is contained
  // in this range.
  bool contains(const std::array<Id, 3>& triple) const {
    return (!begin_.has_value() || begin_.value() <= triple) &&
           (!end_.has_value() || triple < end_.value());
  }
};

// The bit-packed metadata of all the sub-blocks of a permutation. For each
// block, the following is stored:
//
// 1. A header with a frame of reference (one base value per triple column)
//    and the bit widths of the fields of the sub-blocks (one width per triple
//    column, for the number of rows, for the complete record, and for each
//    column's compressed size).
// 2. For each sub-block a record of fixed width with
//    - the first triple without the graph (relative to the bases),
//    - the number of rows,
//    - the compressed size of each column,
//    - the number of leading columns (0, 1, or 2) that the last triple of the
//      sub-block shares with the first triple of the next sub-block of the
//      same block. This allows to compute a tight upper bound for the last
//      triple without storing it (see
//      `PermutationBlockMetadata::subBlock`),
//    - whether the sub-block contains duplicates with different graphs.
//
// For a block with a single relation and sub-blocks of 4096 rows, a record
// typically needs between 100 and 160 bits.
class SubBlockTable {
 public:
  // The information about a single sub-block. This is used to build the table
  // and also returned by the decoding functions.
  struct SubBlock {
    std::array<Id, 3> firstTriple_;
    size_t numRows_ = 0;
    // The compressed size of each of the columns.
    std::vector<size_t> compressedSizes_;
    // The number of leading columns that the last triple of this sub-block
    // shares with the first triple of the next sub-block (`0` for the last
    // sub-block of a block).
    uint8_t numColumnsSharedWithNext_ = 0;
    bool containsDuplicatesWithDifferentGraphs_ = false;

    QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(
        SubBlock, firstTriple_, numRows_, compressedSizes_,
        numColumnsSharedWithNext_, containsDuplicatesWithDifferentGraphs_)
  };

  // The decoded information about a single sub-block that is needed for the
  // block selection (everything except for the compressed sizes).
  struct DecodedSubBlock {
    std::array<Id, 3> firstTriple_;
    size_t numRows_;
    uint8_t numColumnsSharedWithNext_;
    bool containsDuplicatesWithDifferentGraphs_;
  };

  // The result of `getWithContext`.
  struct DecodedSubBlockWithContext {
    DecodedSubBlock subBlock_;
    // The index of the block, and the range of its sub-blocks.
    size_t blockIndex_;
    size_t beginOfBlock_;
    size_t endOfBlock_;
    // The first triple of the next sub-block of the same block, `std::nullopt`
    // if this is the last sub-block of its block.
    std::optional<std::array<Id, 3>> nextFirstTriple_;
  };

 private:
  // The widths of all the fields of the records of a block, and some derived
  // positions.
  struct Header {
    std::array<uint64_t, 3> bases_;
    std::array<uint8_t, 3> idWidths_;
    uint8_t numRowsWidth_;
    // The offset of the first compressed size in a record.
    size_t compressedSizesOffset_;
    // The position of the first record in `bits_`.
    size_t recordsBegin_;
    // The width of a single record.
    size_t recordWidth_;
  };

  // The number of columns of the blocks.
  size_t numColumns_ = 0;
  // The bit-packed headers and records of all blocks.
  std::vector<uint64_t> bits_;
  size_t numBits_ = 0;
  // For each block, the position of its header in `bits_`.
  std::vector<uint64_t> bitOffsetOfBlock_;
  // For each block `b`, its sub-blocks are `[firstSubBlockOfBlock_[b],
  // firstSubBlockOfBlock_[b + 1])`, so this vector has one more element than
  // there are blocks.
  std::vector<uint32_t> firstSubBlockOfBlock_{0};
  // For each `i`, the block that contains the sub-block `i * sampleDistance`.
  // This speeds up `blockOfSubBlock`. It is not serialized, but recomputed.
  std::vector<uint32_t> blockOfSampledSubBlock_;
  static constexpr size_t sampleDistance = 64;

  // The bit width of the bit widths that are stored in the header.
  static constexpr size_t bitsPerWidth = 7;
  // The bit width of the width of a record, which is also stored in the
  // header.
  static constexpr size_t bitsPerRecordWidth = 16;

 public:
  // Create an empty table (no sub-blocks at all, each block then consists of
  // exactly one sub-block, see `PermutationBlockMetadata`).
  SubBlockTable() = default;
  // Create a table for blocks with `numColumns` columns.
  explicit SubBlockTable(size_t numColumns) : numColumns_{numColumns} {}

  // Append a block with the given sub-blocks. The blocks have to be added in
  // order.
  void addBlock(ql::span<const SubBlock> subBlocks);

  size_t numColumns() const { return numColumns_; }
  size_t numBlocks() const { return firstSubBlockOfBlock_.size() - 1; }
  size_t numSubBlocks() const { return firstSubBlockOfBlock_.back(); }
  bool empty() const { return numBlocks() == 0; }

  // Return the (global) index range `[begin, end)` of the sub-blocks of the
  // block with the index `blockIndex`.
  std::pair<size_t, size_t> subBlocksOfBlock(size_t blockIndex) const {
    return {firstSubBlockOfBlock_.at(blockIndex),
            firstSubBlockOfBlock_.at(blockIndex + 1)};
  }

  // Return the index of the block that contains the sub-block with the
  // (global) index `subBlockIndex`.
  size_t blockOfSubBlock(size_t subBlockIndex) const;

  // Decode the sub-block with the (global) index `subBlockIndex`.
  DecodedSubBlock get(size_t subBlockIndex) const;

  // Decode the sub-block with the (global) index `subBlockIndex` together with
  // the information that is needed to compute its `SubBlockMetadata` (see
  // `PermutationBlockMetadata::subBlock`). This is cheaper than the
  // corresponding calls to `get`, `blockOfSubBlock`, and `subBlocksOfBlock`.
  DecodedSubBlockWithContext getWithContext(size_t subBlockIndex) const;

  // Decode the sub-block with the (global) index `subBlockIndex` including its
  // compressed sizes. This is mostly useful for testing.
  SubBlock getWithCompressedSizes(size_t subBlockIndex) const;

  // Return the offset of column `column` of the sub-block `subBlockIndex`
  // relative to the beginning of the same column of its block, and its
  // compressed size.
  std::pair<size_t, size_t> getOffsetAndCompressedSize(size_t subBlockIndex,
                                                       size_t column) const;

  // Return the number of bytes that this table occupies in RAM (without the
  // overhead of the `std::vector`s themselves).
  size_t getMemoryUsageInBytes() const;

  // Two tables are equal if their serialized contents are equal.
  bool operator==(const SubBlockTable& other) const {
    return numColumns_ == other.numColumns_ && bits_ == other.bits_ &&
           numBits_ == other.numBits_ &&
           bitOffsetOfBlock_ == other.bitOffsetOfBlock_ &&
           firstSubBlockOfBlock_ == other.firstSubBlockOfBlock_;
  }
  bool operator!=(const SubBlockTable& other) const {
    return !(*this == other);
  }

  // Serialize or deserialize the table (the samples for `blockOfSubBlock` are
  // recomputed after deserialization).
  AD_SERIALIZE_FRIEND_FUNCTION(SubBlockTable) {
    serializer | arg.numColumns_;
    serializer | arg.numBits_;
    serializer | arg.bits_;
    serializer | arg.bitOffsetOfBlock_;
    serializer | arg.firstSubBlockOfBlock_;
    if constexpr (ad_utility::serialization::ReadSerializer<S>) {
      AD_CORRECTNESS_CHECK(!arg.firstSubBlockOfBlock_.empty());
      AD_CORRECTNESS_CHECK(arg.bitOffsetOfBlock_.size() ==
                           arg.firstSubBlockOfBlock_.size() - 1);
      arg.computeSamples();
    }
  }

 private:
  // Read the header of the block with the index `blockIndex`.
  Header readHeader(size_t blockIndex) const;

  // Decode the sub-block with the given index (relative to the first sub-block
  // of its block) using the `header` of its block.
  DecodedSubBlock decode(const Header& header, size_t indexInBlock) const;

  // Return the width of the compressed size of column `column` (stored in the
  // header of the block with the index `blockIndex`).
  uint8_t compressedSizeWidth(size_t blockIndex, size_t column) const;

  // Recompute `blockOfSampledSubBlock_`.
  void computeSamples();

  // Append the `width` lowest bits of `value` to `bits_` (all the other bits
  // of `value` must be zero).
  void appendBits(uint64_t value, size_t width);
  // Read `width` bits starting at the bit `position` of `bits_`.
  uint64_t readBits(size_t position, size_t width) const;
};

// The block metadata of a permutation as it is written by the
// `CompressedRelationWriter` and stored on disk.
struct BlocksAndSubBlocks {
  std::vector<CompressedBlockMetadata> blocks_;
  SubBlockTable subBlocks_;
};

class SubBlockMetadataSpan;

// The metadata of all blocks and sub-blocks of a permutation. This is the
// interface between the stored metadata (the `CompressedBlockMetadata` of the
// blocks, and the `SubBlockTable` of the sub-blocks) and the code that selects
// and reads (sub-)blocks. Copying this class is cheap, the actual data is
// shared.
class PermutationBlockMetadata {
 public:
  using Blocks = std::vector<CompressedBlockMetadata>;
  using PermutedTriple = CompressedBlockMetadata::PermutedTriple;

 private:
  // The metadata of the blocks. With updates, the last block may contain only
  // `LocatedTriples` (and no triples on disk). Such a block (as well as all
  // blocks if `subBlocks_` is empty) consists of exactly one sub-block.
  std::shared_ptr<const Blocks> blocks_ = std::make_shared<const Blocks>();
  std::shared_ptr<const SubBlockTable> subBlocks_ =
      std::make_shared<const SubBlockTable>();
  // For each block, `true` iff it contains `LocatedTriples`. This is empty if
  // the metadata has not been augmented by `withAugmentedBlocks`, and the
  // blocks beyond its end have no updates. It is used to compute safe bounds
  // for the
  // last triples of the sub-blocks of these blocks: The `LocatedTriples` that
  // belong to a sub-block (see `getTripleRange`) are only known to be smaller
  // than the first triple of the next sub-block, and they may contain
  // `LocalVocab` IDs, which are ordered between the IDs of the vocabulary, so
  // the tighter bounds that are derived from the bits of the IDs are not valid
  // for them.
  std::shared_ptr<const std::vector<bool>> blocksWithUpdates_ =
      std::make_shared<const std::vector<bool>>();

  // The position of a range of sub-blocks within its block, see
  // `getPosition`.
  struct Position {
    // The range `[beginOfBlock_, endOfBlock_)` of the sub-blocks of the block.
    size_t beginOfBlock_;
    size_t endOfBlock_;
    // The first and the last sub-block of the range (both inclusive).
    size_t first_;
    size_t last_;
  };

 public:
  PermutationBlockMetadata() = default;
  explicit PermutationBlockMetadata(
      std::shared_ptr<const Blocks> blocks,
      std::shared_ptr<const SubBlockTable> subBlocks =
          std::make_shared<const SubBlockTable>(),
      std::shared_ptr<const std::vector<bool>> blocksWithUpdates =
          std::make_shared<const std::vector<bool>>());
  // Convenience constructor for blocks without sub-blocks (mostly for
  // testing).
  explicit PermutationBlockMetadata(Blocks blocks)
      : PermutationBlockMetadata{
            std::make_shared<const Blocks>(std::move(blocks))} {}
  // Construct from the blocks and sub-blocks as they are written by the
  // `CompressedRelationWriter`.
  explicit PermutationBlockMetadata(BlocksAndSubBlocks blocksAndSubBlocks)
      : PermutationBlockMetadata{
            std::make_shared<const Blocks>(
                std::move(blocksAndSubBlocks.blocks_)),
            std::make_shared<const SubBlockTable>(
                std::move(blocksAndSubBlocks.subBlocks_))} {}

  // Return a copy of this metadata, but with the given `blocks` (which have to
  // be the blocks of this metadata, but possibly with updated bounds and graph
  // info, plus possibly one additional block at the end that only consists of
  // `LocatedTriples`) and `blocksWithUpdates` (see the member of the same
  // name).
  PermutationBlockMetadata withAugmentedBlocks(
      Blocks blocks, std::vector<bool> blocksWithUpdates) const;

  // Return the blocks and the sub-blocks.
  const Blocks& blocks() const { return *blocks_; }
  const std::shared_ptr<const Blocks>& blocksShared() const { return blocks_; }
  const SubBlockTable& subBlockTable() const { return *subBlocks_; }
  const std::shared_ptr<const SubBlockTable>& subBlockTableShared() const {
    return subBlocks_;
  }

  size_t numBlocks() const { return blocks_->size(); }
  size_t numSubBlocks() const;

  // Return the metadata of the sub-block with the (global) index
  // `subBlockIndex`. The returned value refers to `this`, which therefore has
  // to outlive it.
  SubBlockMetadata subBlock(size_t subBlockIndex) const;

  // Return all the sub-blocks.
  SubBlockMetadataSpan subBlocks() const;

  // Return the (global) index range `[begin, end)` of the sub-blocks of the
  // block with the index `blockIndex`.
  std::pair<size_t, size_t> subBlocksOfBlock(size_t blockIndex) const;

  // Return the offset and compressed size of the column `column` of the
  // sub-blocks described by `subBlocks` (which have to be obtained from
  // `this`).
  SubBlockMetadata::OffsetAndCompressedSize getOffsetAndCompressedSize(
      const SubBlockMetadata& subBlocks, ColumnIndex column) const;

  // Return the range of triples that belong to the sub-blocks described by
  // `subBlocks`, see `SubBlockTripleRange`.
  SubBlockTripleRange getTripleRange(const SubBlockMetadata& subBlocks) const;

  // Throw if the sub-blocks are not sorted, that is, if for one of them the
  // first triple is larger than its last triple, or its last triple is larger
  // than the first triple of the next sub-block. This is linear in the number
  // of sub-blocks, which is why the constructor and `withAugmentedBlocks` only
  // call it if expensive checks are enabled.
  void checkInvariants() const;

 private:
  // Return `true` iff the block with index `blockIndex` contains
  // `LocatedTriples`.
  bool blockHasUpdates(size_t blockIndex) const {
    return blockIndex < blocksWithUpdates_->size() &&
           (*blocksWithUpdates_)[blockIndex];
  }

  // Return the `Position` of the `subBlocks` (which have to be obtained from
  // `this`) within their block.
  Position getPosition(const SubBlockMetadata& subBlocks) const;
};

// A random access iterator over `SubBlockMetadata` values. The values are
// either computed on the fly by a `PermutationBlockMetadata`, or read from a
// contiguous array of materialized values (e.g. the result of a join or of a
// `PrefilterExpression`, or in unit tests). The iterator yields values (and
// not references).
class SubBlockMetadataIterator {
 public:
  using iterator_category = std::random_access_iterator_tag;
  using difference_type = int64_t;
  using value_type = SubBlockMetadata;
  using reference = SubBlockMetadata;
  using pointer = void;

  // This proxy is returned by `operator->`, which therefore also works for the
  // values that are computed on the fly.
  struct ArrowProxy {
    SubBlockMetadata value_;
    const SubBlockMetadata* operator->() const { return &value_; }
  };

 private:
  const PermutationBlockMetadata* source_ = nullptr;
  const SubBlockMetadata* materialized_ = nullptr;
  size_t index_ = 0;

 public:
  SubBlockMetadataIterator() = default;
  SubBlockMetadataIterator(const PermutationBlockMetadata* source,
                           const SubBlockMetadata* materialized, size_t index)
      : source_{source}, materialized_{materialized}, index_{index} {}

  SubBlockMetadata operator*() const {
    return materialized_ != nullptr ? materialized_[index_]
                                    : source_->subBlock(index_);
  }
  ArrowProxy operator->() const { return {**this}; }
  SubBlockMetadata operator[](difference_type n) const { return *(*this + n); }

  auto compareThreeWay(const SubBlockMetadataIterator& rhs) const {
    return ql::compareThreeWay(index_, rhs.index_);
  }
  QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL(SubBlockMetadataIterator)
  bool operator==(const SubBlockMetadataIterator& rhs) const {
    return index_ == rhs.index_;
  }
  bool operator!=(const SubBlockMetadataIterator& rhs) const {
    return index_ != rhs.index_;
  }

  SubBlockMetadataIterator& operator+=(difference_type n) {
    index_ += n;
    return *this;
  }
  SubBlockMetadataIterator& operator-=(difference_type n) {
    index_ -= n;
    return *this;
  }
  SubBlockMetadataIterator operator+(difference_type n) const {
    auto result = *this;
    result += n;
    return result;
  }
  friend SubBlockMetadataIterator operator+(
      difference_type n, const SubBlockMetadataIterator& it) {
    return it + n;
  }
  SubBlockMetadataIterator operator-(difference_type n) const {
    auto result = *this;
    result -= n;
    return result;
  }
  difference_type operator-(const SubBlockMetadataIterator& rhs) const {
    return static_cast<difference_type>(index_) -
           static_cast<difference_type>(rhs.index_);
  }
  SubBlockMetadataIterator& operator++() {
    ++index_;
    return *this;
  }
  SubBlockMetadataIterator operator++(int) {
    auto result = *this;
    ++index_;
    return result;
  }
  SubBlockMetadataIterator& operator--() {
    --index_;
    return *this;
  }
  SubBlockMetadataIterator operator--(int) {
    auto result = *this;
    --index_;
    return result;
  }

  friend class SubBlockMetadataSpan;
};

// A non-owning random access range of `SubBlockMetadata` values, see
// `SubBlockMetadataIterator`. It has the same interface as a `ql::span<const
// SubBlockMetadata>` (except that it yields values).
class SubBlockMetadataSpan {
 public:
  using iterator = SubBlockMetadataIterator;
  using const_iterator = SubBlockMetadataIterator;
  using value_type = SubBlockMetadata;
  using size_type = size_t;

 private:
  iterator begin_;
  iterator end_;

 public:
  SubBlockMetadataSpan() = default;
  // Create a span of all the sub-blocks of the `source`.
  explicit SubBlockMetadataSpan(const PermutationBlockMetadata& source)
      : begin_{&source, nullptr, 0},
        end_{&source, nullptr, source.numSubBlocks()} {}
  // Create a span of materialized values.
  SubBlockMetadataSpan(ql::span<const SubBlockMetadata> values)
      : begin_{nullptr, values.data(), 0},
        end_{nullptr, values.data(), values.size()} {}
  SubBlockMetadataSpan(const std::vector<SubBlockMetadata>& values)
      : SubBlockMetadataSpan{ql::span<const SubBlockMetadata>{values}} {}
  // Create the span `[begin, end)`.
  SubBlockMetadataSpan(iterator begin, iterator end)
      : begin_{begin}, end_{end} {
    AD_EXPENSIVE_CHECK(begin.source_ == end.source_ &&
                       begin.materialized_ == end.materialized_);
  }
  // Construct from a subrange of iterators.
  CPP_template(typename Range)(
      requires ql::concepts::same_as<
          ql::ranges::iterator_t<const Range>,
          iterator>) explicit SubBlockMetadataSpan(const Range& range)
      : SubBlockMetadataSpan{ql::ranges::begin(range), ql::ranges::end(range)} {
  }

  iterator begin() const { return begin_; }
  iterator end() const { return end_; }
  size_t size() const { return static_cast<size_t>(end_ - begin_); }
  bool empty() const { return begin_ == end_; }
  SubBlockMetadata operator[](size_t i) const { return begin_[i]; }
  SubBlockMetadata front() const { return *begin_; }
  SubBlockMetadata back() const { return *std::prev(end_); }

  // Return the subrange of `count` elements starting at `offset` (all the
  // remaining elements if `count` is `ql::dynamic_extent`).
  SubBlockMetadataSpan subspan(size_t offset,
                               size_t count = ql::dynamic_extent) const {
    AD_CONTRACT_CHECK(offset <= size());
    auto newBegin = begin_ + offset;
    auto newEnd = count == ql::dynamic_extent ? end_ : newBegin + count;
    AD_CONTRACT_CHECK(newEnd <= end_);
    return {newBegin, newEnd};
  }
};

// Iterator with respect to a `SubBlockMetadata` value of a
// `SubBlockMetadataSpan`.
using SubBlockMetadataIt = SubBlockMetadataSpan::iterator;
// Section of relevant blocks as a subrange defined by `SubBlockMetadataIt`s.
using SubBlockMetadataRange = ql::ranges::subrange<SubBlockMetadataIt>;
// Vector containing `SubBlockMetadataRange`s.
using SubBlockMetadataRanges = std::vector<SubBlockMetadataRange>;

#endif  // QLEVER_SRC_INDEX_SUBBLOCKMETADATA_H
