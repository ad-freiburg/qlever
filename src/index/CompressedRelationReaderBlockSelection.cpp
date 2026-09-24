// Copyright 2021 - 2026 The QLever Authors, in particular:
//
// 2021 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025        Hannes Baumann <baumannh@cs.uni-freiburg.de>, UFR
// 2025        Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// The parts of `CompressedRelationReader` that only operate on the block
// metadata (selecting the blocks of a scan or a join, and checking the
// invariants of the selected blocks), without reading any blocks from disk.

#include <sstream>

#include "index/CompressedRelationReader.h"

// A small helper function to obtain the begin and end iterator of a range
template <typename T>
static auto getBeginAndEnd(T& range) {
  return std::pair{ql::ranges::begin(range), ql::ranges::end(range)};
}

// TODO @realHannes:
// Move `ScanSpecAndBlocks` (containing the `BlockMetadataRanges`) and the
// helper functions below into `CompressedRelationMetadata.h`, next to the
// other metadata related structs.

// _____________________________________________________________________________
Id CompressedRelationReader::getRelevantIdFromTriple(
    CompressedBlockMetadata::PermutedTriple triple,
    const ScanSpecAndBlocksAndBounds& metadataAndBlocks) {
  // The `ScanSpecifcation`, which must ask for at least one column.
  const auto& scanSpec = metadataAndBlocks.scanSpec_;
  AD_CORRECTNESS_CHECK(!scanSpec.col2Id());

  // For a full scan, return the triples's `col0Id`.
  if (!scanSpec.col0Id().has_value()) {
    return triple.col0Id_;
  }

  // Compute the following range: If the `scanSpec` specifies both `col0Id`
  // and `col1Id`, the first and last `col2Id` of the blocks. If the
  // `scanSpec` specifies only `col0Id`, the first and last `col1Id` of the
  // blocks.
  auto [minId, maxId] = [&]() {
    const auto& [first, last] = metadataAndBlocks.firstAndLastTriple_;
    if (scanSpec.col1Id().has_value()) {
      return std::array{first.col2Id_, last.col2Id_};
    } else {
      AD_CORRECTNESS_CHECK(scanSpec.col0Id().has_value());
      return std::array{first.col1Id_, last.col1Id_};
    }
  }();

  // Helper lambda that returns `std::nullopt` if `idFromTriple` equals `id`,
  // `minId` if is smaller, and `maxId` if it is larger.
  auto idForNonMatchingBlock = [](Id idFromTriple, Id id, Id minId,
                                  Id maxId) -> std::optional<Id> {
    if (idFromTriple < id) {
      return minId;
    }
    if (idFromTriple > id) {
      return maxId;
    }
    return std::nullopt;
  };

  // If the `col0Id` of the triple does not match that of the `scanSpec`,
  // return `minId` (if it is smaller) or `maxId` (if it is larger).
  if (auto optId = idForNonMatchingBlock(
          triple.col0Id_, scanSpec.col0Id().value(), minId, maxId)) {
    return optId.value();
  }

  // If the `col0Id` of the triple matches that of the `scanSpec`, and the
  // `scanSpec` does not specify `col1Id`, return the triples's `col1Id`.
  if (!scanSpec.col1Id().has_value()) {
    return triple.col1Id_;
  }

  // If the `col1Id` of the triple matches that of the `scanSpec`, return the
  // triples's `col2Id`. Otherwise, return `minId` (if it is smaller) or
  // `maxId` (if it is larger).
  return idForNonMatchingBlock(triple.col1Id_, scanSpec.col1Id().value(), minId,
                               maxId)
      .value_or(triple.col2Id_);
}

// _____________________________________________________________________________
auto CompressedRelationReader::getBlocksForJoin(
    ql::span<const Id> joinColumn,
    const ScanSpecAndBlocksAndBounds& metadataAndBlocks)
    -> GetBlocksForJoinResult {
  if (joinColumn.empty() || metadataAndBlocks.getBlockMetadataView().empty()) {
    return {};
  }

  // `id < block` iff `id < block.firstTriple`
  auto idLessThanBlock = [&metadataAndBlocks](
                             Id id, const CompressedBlockMetadata& block) {
    return id < getRelevantIdFromTriple(block.firstTriple_, metadataAndBlocks);
  };

  // `block < id` iff `block.lastTriple < id`
  auto blockLessThanId = [&metadataAndBlocks](
                             const CompressedBlockMetadata& block, Id id) {
    return getRelevantIdFromTriple(block.lastTriple_, metadataAndBlocks) < id;
  };

  std::vector<CompressedBlockMetadata> result;
  const auto& mdView = metadataAndBlocks.getBlockMetadataView();

  auto [colIt, colEnd] = getBeginAndEnd(joinColumn);
  auto [blockIt, blockEnd] = getBeginAndEnd(mdView);
  GetBlocksForJoinResult res;

  // Manually count the number of blocks that have been fully processed in the
  // `mdView`. This includes blocks that are returned as part of the result as
  // well as blocks that are completely skipped, because they are
  // `< joinColumn.back()` but don't match any of the entries in the
  // `joinColumn`.
  auto& blockIdx = res.numHandledBlocks;
  while (true) {
    // Skip all IDs in the `joinColumn` that are strictly smaller than any
    // block that hasn't been handled so far.
    while (colIt != colEnd && idLessThanBlock(*colIt, *blockIt)) {
      ++colIt;
    }
    if (colIt == colEnd) {
      return res;
    }

    // At this point, `*blockIt <= *colIt`.
    // Now skip all blocks that are `< *colIt`.
    while (blockIt != blockEnd && blockLessThanId(*blockIt, *colIt)) {
      ++blockIt;
      ++blockIdx;
    }
    if (blockIt == blockEnd) {
      return res;
    }
    // Now it holds that `*blockIt >= *colIt`. As the entries in the
    // `joinColumn` as well as the blocks are sorted, it suffices to
    // additionally find the values where `*blockIt <= *colIt` to find
    // possibly matching blocks.
    while (blockIt != blockEnd && !idLessThanBlock(*colIt, *blockIt)) {
      res.matchingBlocks_.push_back(*blockIt);
      ++blockIt;
      ++blockIdx;
    }
    if (blockIt == blockEnd) {
      return res;
    }
  }
}

// _____________________________________________________________________________
std::array<std::vector<CompressedBlockMetadata>, 2>
CompressedRelationReader::getBlocksForJoin(
    const ScanSpecAndBlocksAndBounds& metadataAndBlocks1,
    const ScanSpecAndBlocksAndBounds& metadataAndBlocks2) {
  // Associate a block together with the relevant ID (col1 or col2) for this
  // join from the first and last triple.
  struct BlockWithFirstAndLastId {
    const CompressedBlockMetadata& block_;
    Id first_;
    Id last_;
  };

  auto blockLessThanBlock = [&](const BlockWithFirstAndLastId& block1,
                                const BlockWithFirstAndLastId& block2) {
    return block1.last_ < block2.first_;
  };

  // Transform all the relevant blocks from a `ScanSpecAndBlocksAndBounds` a
  // `BlockWithFirstAndLastId` struct (see above).
  auto getBlocksWithFirstAndLastId =
      [&blockLessThanBlock](
          const ScanSpecAndBlocksAndBounds& metadataAndBlocks) {
        auto getSingleBlock =
            [&metadataAndBlocks](const CompressedBlockMetadata& block)
            -> BlockWithFirstAndLastId {
          return {
              block,
              getRelevantIdFromTriple(block.firstTriple_, metadataAndBlocks),
              getRelevantIdFromTriple(block.lastTriple_, metadataAndBlocks)};
        };
        auto result = metadataAndBlocks.getBlockMetadataView() |
                      ql::views::transform(getSingleBlock);
        AD_CORRECTNESS_CHECK(ql::ranges::is_sorted(result, blockLessThanBlock));
        return result;
      };

  auto blocksWithFirstAndLastId1 =
      getBlocksWithFirstAndLastId(metadataAndBlocks1);
  auto blocksWithFirstAndLastId2 =
      getBlocksWithFirstAndLastId(metadataAndBlocks2);

  // Find the matching blocks on each side using a linear-time merge zipper.
  // Both sequences are sorted by `first_` with non-overlapping intervals
  // (i.e. consecutive blocks `b1, b2` from the same side satisfy
  // `b1.last_ < b2.first_`; invariant enforced above by the
  // `AD_CORRECTNESS_CHECK` on `is_sorted` under `blockLessThanBlock`). The
  // stateful pointer into `otherBlocks` never moves backward because
  // `a.first_` is non-decreasing, giving O(n + m) total.
  //
  // NOTE: it is tempting to reuse the `zipperJoinWithUndef` routine, but this
  // doesn't work because the implicit equality defined by `!lessThan(a,b) &&
  // !lessThan(b, a)` is not transitive.
  auto findMatchingBlocks = [&blockLessThanBlock](const auto& blocks,
                                                  const auto& otherBlocks) {
    std::vector<CompressedBlockMetadata> result;
    auto [it, end] = getBeginAndEnd(otherBlocks);
    for (const auto& a : blocks) {
      it = ql::ranges::find_if_not(it, end,
                                   [&blockLessThanBlock, &a](const auto& b) {
                                     return blockLessThanBlock(b, a);
                                   });
      if (it == end) {
        break;
      }
      if (!blockLessThanBlock(a, *it)) {
        result.push_back(a.block_);
      }
    }
    return result;
  };

  return {
      findMatchingBlocks(blocksWithFirstAndLastId1, blocksWithFirstAndLastId2),
      findMatchingBlocks(blocksWithFirstAndLastId2, blocksWithFirstAndLastId1)};
}
// _____________________________________________________________________________
size_t CompressedRelationReader::getNumberOfBlockMetadataValues(
    const BlockMetadataRanges& blockMetadata) {
  return ::ranges::accumulate(blockMetadata, 0ULL,
                              [](auto acc, const auto& block) {
                                return acc + ql::ranges::size(block);
                              });
}

// _____________________________________________________________________________
std::vector<CompressedBlockMetadata>
CompressedRelationReader::convertBlockMetadataRangesToVector(
    const BlockMetadataRanges& blockMetadata) {
  std::vector<CompressedBlockMetadata> blocksMaterialized;
  blocksMaterialized.reserve(getNumberOfBlockMetadataValues(blockMetadata));
  ql::ranges::copy(blockMetadata | ql::views::join,
                   std::back_inserter(blocksMaterialized));
  return blocksMaterialized;
}

// _____________________________________________________________________________
BlockMetadataRanges CompressedRelationReader::getRelevantBlocks(
    const ScanSpecification& scanSpec,
    const BlockMetadataRanges& blockMetadata) {
  // Get all the blocks  that possibly might contain our pair of col0Id and
  // col1Id
  CompressedBlockMetadata key;

  auto setOrDefault = [&scanSpec](auto getterA, auto getterB, auto& triple,
                                  auto defaultValue) {
    std::invoke(getterA, triple) =
        std::invoke(getterB, scanSpec).value_or(defaultValue);
  };
  auto setKey = [&setOrDefault, &key](auto getterA, auto getterB) {
    setOrDefault(getterA, getterB, key.firstTriple_, Id::min());
    setOrDefault(getterA, getterB, key.lastTriple_, Id::max());
  };
  using PermutedTriple = CompressedBlockMetadata::PermutedTriple;
  setKey(&PermutedTriple::col0Id_, &ScanSpecification::col0Id);
  setKey(&PermutedTriple::col1Id_, &ScanSpecification::col1Id);
  setKey(&PermutedTriple::col2Id_, &ScanSpecification::col2Id);

  // We currently don't filter by the graph ID here.
  key.firstTriple_.graphId_ = Id::min();
  key.lastTriple_.graphId_ = Id::max();

  // This comparator only returns true if a block stands completely before
  // another block without any overlap. In other words, the last triple of `a`
  // must be smaller than the first triple of `b` to return true.
  auto comp = [](const auto& blockA, const auto& blockB) {
    return blockA.lastTriple_ < blockB.firstTriple_;
  };

  // TODO:
  // Optionally implement a free function like `equal_range(YourRangeType,
  // key, comp)` that implements the equal range correctly. (1) Perform binary
  // search on the inner blocks with respect to the first and
  //     last triple.
  // (2) Perform binary search regarding the outer blocks.
  BlockMetadataRanges resultBlocks;
  ql::ranges::for_each(
      blockMetadata, [&resultBlocks, &key,
                      &comp](const BlockMetadataRange& blockMetadataSubrange) {
        auto result = ql::ranges::equal_range(blockMetadataSubrange, key, comp);
        if (result.begin() != result.end()) {
          resultBlocks.emplace_back(result.begin(), result.end());
        }
      });
  return resultBlocks;
}
// _____________________________________________________________________________
// Helper to the following block-invariant-check Impls for informative error
// message construction.
static auto createErrorMessage = [](const auto& b1, const auto& b2,
                                    const std::string& errCause) {
  auto toString = [](const auto& b) {
    std::ostringstream oss;
    oss << b;
    return oss.str();
  };
  return absl::StrCat(errCause, "First Block:\n", toString(b1),
                      "Second Block:\n", toString(b2));
};

// _____________________________________________________________________________
// Check if the provided `Range` holds less than two `CompressedBlockMetadata`
// values.
CPP_template(typename Range)(
    requires ql::ranges::input_range<
        Range>) static bool checkBlockRangeSizeLessThanTwo(const Range&
                                                               blockMetadataRange) {
  auto begin = ql::ranges::begin(blockMetadataRange);
  auto end = ql::ranges::end(blockMetadataRange);
  return begin == end || ql::ranges::next(begin) == end;
}

// _____________________________________________________________________________
CPP_template(typename Range)(
    requires ql::ranges::input_range<
        Range>) static void checkBlockMetadataInvariantOrderAndUniquenessImpl(const Range&
                                                                                  blockMetadataRange) {
  if (checkBlockRangeSizeLessThanTwo(blockMetadataRange)) {
    return;
  }

  auto checkUniquenessAndOrder = [](const auto& blockPair) {
    const auto& [b1, b2] = blockPair;
    // Blocks must be unique.
    AD_CONTRACT_CHECK(b1 != b2 && b1.blockIndex_ != b2.blockIndex_, [&] {
      return createErrorMessage(b1, b2, "Found block metadata duplicates\n");
    });
    // Blocks must adhere to ascending order.
    AD_CONTRACT_CHECK(
        b1.lastTriple_ < b2.lastTriple_ && b1.blockIndex_ < b2.blockIndex_,
        [&] {
          return createErrorMessage(b1, b2,
                                    "Found block metadata order violation\n");
        });
  };
  auto blockMetadataRangeShifted = blockMetadataRange | ql::views::drop(1);
  auto zippedBlockPairs =
      ranges::views::zip(blockMetadataRange, blockMetadataRangeShifted);
  ql::ranges::for_each(zippedBlockPairs, checkUniquenessAndOrder);
}

// ____________________________________________________________________________
CPP_template(typename Range)(requires ql::ranges::input_range<Range>) static void checkBlockMetadataInvariantBlockConsistencyImpl(
    const Range& blockMetadataRange, size_t firstFreeColIndex) {
  if (checkBlockRangeSizeLessThanTwo(blockMetadataRange)) {
    return;
  }
  auto blockMetadataRangeShifted = blockMetadataRange | ql::views::drop(1);
  auto zippedBlockPairs =
      ranges::views::zip(blockMetadataRange, blockMetadataRangeShifted);

  for (const auto& [i, blockPair] :
       ranges::views::enumerate(zippedBlockPairs)) {
    const auto& [b1, b2] = blockPair;
    // Consecutive blocks must contain equivalent values over the fixed
    // columns.
    AD_CONTRACT_CHECK(b1.isConsistentWith(b2, firstFreeColIndex), [&] {
      return createErrorMessage(
          b1, b2, "Found column inconsistency between two blocks\n");
    });
    // All blocks, except the first and last, must contain consistent column
    // values over their triples up to the first free column.
    if (i > 0) {
      AD_CONTRACT_CHECK(
          !b1.containsInconsistentTriples(firstFreeColIndex), [&] {
            return createErrorMessage(
                b1, b2,
                absl::StrCat("The following First Block contains non-constant "
                             "column values up to defined column index: ",
                             firstFreeColIndex));
          });
    }
  }
}

// _____________________________________________________________________________
CompressedRelationReader::ScanSpecAndBlocks::ScanSpecAndBlocks(
    ScanSpecification scanSpec, const BlockMetadataRanges& blockMetadataRanges)
    : scanSpec_(std::move(scanSpec)) {
  if constexpr (ad_utility::areExpensiveChecksEnabled) {
    const auto& blockRangeView = blockMetadataRanges | ql::views::join;
    checkBlockMetadataInvariantOrderAndUniquenessImpl(blockRangeView);
  }
  blockMetadata_ = getRelevantBlocks(scanSpec_, blockMetadataRanges);
  if constexpr (ad_utility::areExpensiveChecksEnabled) {
    checkBlockMetadataInvariantBlockConsistencyImpl(
        getBlockMetadataView(), scanSpec_.firstFreeColIndex());
  }
  sizeBlockMetadata_ = getNumberOfBlockMetadataValues(blockMetadata_);
}

// _____________________________________________________________________________
ql::span<const CompressedBlockMetadata>
CompressedRelationReader::ScanSpecAndBlocks::getBlockMetadataSpan() const {
  // ScanSpecAndBlocks must contain exactly one BlockMetadataRange to be
  // accessible as a span.
  AD_CONTRACT_CHECK(blockMetadata_.size() == 1);
  // `ql::span` object requires contiguous range.
  static_assert(ql::ranges::contiguous_range<BlockMetadataRange>);
  const auto& blockMetadataRange = blockMetadata_.front();
  return ql::span(blockMetadataRange.begin(), blockMetadataRange.end());
}

// _____________________________________________________________________________
void CompressedRelationReader::ScanSpecAndBlocks::checkBlockMetadataInvariant(
    ql::span<const CompressedBlockMetadata> blocks, size_t firstFreeColIndex) {
  checkBlockMetadataInvariantOrderAndUniquenessImpl(blocks);
  checkBlockMetadataInvariantBlockConsistencyImpl(blocks, firstFreeColIndex);
}

// _____________________________________________________________________________
void CompressedRelationReader::ScanSpecAndBlocks::removePrefix(
    size_t numBlocksToRemove) {
  auto it = blockMetadata_.begin();
  auto end = blockMetadata_.end();
  for (; it != end; ++it) {
    auto& subspan = *it;
    auto sz = ql::ranges::size(subspan);
    if (numBlocksToRemove < sz) {
      // Partially remove a subspan if it contains less blocks than we have
      // to remove.
      subspan.advance(numBlocksToRemove);
      break;
    } else {
      // Completely remove the subspan (via the `erase` at the end).
      numBlocksToRemove -= sz;
    }
  }
  // Remove all the blocks that are to be erased completely.
  blockMetadata_.erase(blockMetadata_.begin(), it);
}
