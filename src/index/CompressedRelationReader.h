// Copyright 2021 - 2026 The QLever Authors, in particular:
//
// 2021 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2023 - 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
// 2025        Hannes Baumann <baumannh@cs.uni-freiburg.de>, UFR
// 2025        Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
// 2026        Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_COMPRESSEDRELATIONREADER_H
#define QLEVER_SRC_INDEX_COMPRESSEDRELATIONREADER_H

#include <array>
#include <chrono>
#include <functional>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

#include "backports/algorithm.h"
#include "backports/span.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "index/CompressedRelationMetadata.h"
#include "index/ScanSpecification.h"
#include "parser/data/LimitOffsetClause.h"
#include "util/AllocatorWithLimit.h"
#include "util/CancellationHandle.h"
#include "util/File.h"
#include "util/Generator.h"
#include "util/HashSet.h"
#include "util/Iterators.h"

class LocatedTriplesPerBlock;

// Sometimes we do not read/decompress  all the columns of a block, so we have
// to use a dynamic `IdTable`.
using DecompressedBlock = IdTable;

// A decompressed block together with some metadata about the process of
// decompressing + postprocessing it.
struct DecompressedBlockAndMetadata {
  DecompressedBlock block_;
  // True iff the block had to be modified because of the contained graphs.
  bool wasPostprocessed_;
  // True iff triples this block had to be merged with the `LocatedTriples`
  // because it contained updates.
  bool containsUpdates_;
};

// After compression the columns have different sizes, so we cannot use an
// `IdTable`.
using CompressedBlock = std::vector<std::vector<char>>;

using namespace std::string_view_literals;

/// Manage the reading of relations from disk that have been previously written
/// using the `CompressedRelationWriter`.
class CompressedRelationReader {
  template <typename T>
  using vector = std::vector<T>;

 public:
  using Allocator = ad_utility::AllocatorWithLimit<Id>;
  using ColumnIndicesRef = ql::span<const ColumnIndex>;
  using ColumnIndices = std::vector<ColumnIndex>;
  using CancellationHandle = ad_utility::SharedCancellationHandle;

  // Optional override for the number of threads used to read and decompress
  // blocks in `asyncParallelBlockGenerator`. When set, it takes precedence over
  // the `lazy-index-scan-num-threads` runtime parameter. This is used by the
  // runtime index rebuild, which scans the old permutations through a dedicated
  // reader (see `Permutation::lazyScanWithUnlimitedReader`), to throttle its
  // read/decompress parallelism without affecting query scans (which use the
  // permutation's shared reader, where this stays `nullopt`).
  std::optional<size_t> lazyScanNumThreadsOverride_ = std::nullopt;

  // This struct stores a reference to the (optional) graphs by which a result
  // is filtered, the column in which the graph ID will reside in a result,
  // and the information whether this column is required as part of the output,
  // or whether it should be deleted after filtering. It can then filter a given
  // block according to those settings.
  struct FilterDuplicatesAndGraphs {
    ScanSpecification::GraphFilter graphFilter_;
    ColumnIndex graphColumn_;
    bool deleteGraphColumn_;
    // Filter `block` such that it contains only the specified graphs and no
    // duplicates. The `blockMetadata` of `block` is used for possible shortcuts
    // (for example, if we know that there are no duplicates, we do not have to
    // eliminate them). The return value is `true` if the `block` has been
    // modified because it contained duplicates or triples from unwanted graphs.
    bool postprocessBlock(IdTable& block,
                          const CompressedBlockMetadata& blockMetadata) const;

    // Return true, iff a block, specified by the `blockMetadata` contains no
    // triples from `desiredGraphs_` and therefore doesn't have to be read from
    // disk, and if this fact can be determined by `blockMetadata` alone.
    bool canBlockBeSkipped(const CompressedBlockMetadata& blockMetadata) const;

    // Delete the `graphColumn_` from `block` if `deleteGraphColumn_` is true.
    void deleteGraphColumnIfNecessary(IdTable& block) const;

   private:
    // Return a lambda that returns true if `desiredGraphs_` allows the given
    // `graph` and it is not the default graph.
    auto isGraphAllowedLambda() const;

    // Return true iff all triples from the block belong to the
    // `desiredGraphs_`, and if this fact can be determined by looking at the
    // metadata alone.
    bool blockNeedsFilteringByGraph(
        const CompressedBlockMetadata& metadata) const;

    // Implementation of the various steps of `postprocessBlock`. Each of them
    // returns `true` iff filtering the block was necessary.
    bool filterByGraphIfNecessary(
        IdTable& block, const CompressedBlockMetadata& blockMetadata) const;
    static bool filterDuplicatesIfNecessary(
        IdTable& block, const CompressedBlockMetadata& blockMetadata);
  };

  // Classes holding various subsets of parameters relevant for a scan of a
  // permutation, including a reference to the relevant located triples.
  struct ScanImplConfig {
    ColumnIndices scanColumns_;
    FilterDuplicatesAndGraphs graphFilter_;
    const LocatedTriplesPerBlock& locatedTriples_;
  };

  // The specification of scan, together with the blocks on which this scan is
  // to be performed.
  //
  // Brief explanation of `ScanSpecAndBlocks` constructor logic:
  // (1) The passed `ScanSpecification` remains as it is and is moved into
  // member variable `scanSpec_`.
  // (2) Member `blockMetadata_` is set to the `BlockMetadataRanges` computed
  // via `getRelevantBlocks` for the provided `ScanSpecification` and
  // `BlockMetadataRanges`.
  // (3) Compute `sizeBlockMetadata_`, which represents the number of
  // `CompressedBlockMetadata` values contained over all subranges in member
  // `BlockMetadataRanges_ blockMetadata_`.
  // (4) Perform an invariant check. The `CompressedBlockMetadata` values must
  // be unique, sorted in ascending order, and have consistent column values up
  // to the first free column defined by `scanSpec_`.
  struct ScanSpecAndBlocks {
    ScanSpecification scanSpec_;
    BlockMetadataRanges blockMetadata_;
    size_t sizeBlockMetadata_;

    ScanSpecAndBlocks(ScanSpecification scanSpec,
                      const BlockMetadataRanges& blockMetadataRanges);

    // Direct view access via `ql::views::join` over all
    // `CompressedBlockMetadata` values contained in `BlockMetadatatRanges
    // blockMetadata_`.
    auto getBlockMetadataView() const {
      return ql::views::join(blockMetadata_);
    }

    // If `BlockMetadataRanges blockMetadata_` contains exactly one
    // `BlockMetadataRange` (verified via AD_CONTRACT_CHECK), return the
    // corresponding CompressedBlockMetadata values as a span.
    ql::span<const CompressedBlockMetadata> getBlockMetadataSpan() const;

    // Check the provided `BlockMetadataRange`s for the following invariants:
    //   - All contained `CompressedBlockMetadata` values must be unique.
    //   - The `CompressedBlockMetadata` values must adhere to ascending order.
    //   - `firstFreeColIndex` is the column index up to which we expect
    //      constant values in `columns < firstFreeColIndex` over all blocks.
    static void checkBlockMetadataInvariant(
        ql::span<const CompressedBlockMetadata> blocks,
        size_t firstFreeColIndex);

    // Remove the first `numBlocksToRemove` from the `blockMetadata_`. This can
    // be used if it is known that those are not needed anymore, e.g. because
    // they have already been dealt with by a lazy `IndexScan` or `Join`.
    void removePrefix(size_t numBlocksToRemove);
  };

  // This struct additionally contains the first and last triple of the scan
  // result.
  struct ScanSpecAndBlocksAndBounds : public ScanSpecAndBlocks {
    // `firstAndLastTriple_` contains the first and the last triple
    // of the specified relation (and being filtered by the `col1Id` if
    // specified). This might be different from the first triple in the first
    // block (in the case of the `firstTriple_`, similarly for `lastTriple_`)
    // because the first and last block might also contain other relations, or
    // the same relation but with different `col1Id`s.
    struct FirstAndLastTriple {
      CompressedBlockMetadata::PermutedTriple firstTriple_;
      CompressedBlockMetadata::PermutedTriple lastTriple_;
    };
    FirstAndLastTriple firstAndLastTriple_;
    // Deliberately delete the default constructor such that we don't
    // accidentally forget to set the `firstAndLastTriple_`.
    ScanSpecAndBlocksAndBounds() = delete;
    ScanSpecAndBlocksAndBounds(ScanSpecAndBlocks base,
                               FirstAndLastTriple triples)
        : ScanSpecAndBlocks(std::move(base)),
          firstAndLastTriple_(std::move(triples)) {}
  };

  struct LazyScanMetadata {
    size_t numBlocksRead_ = 0;
    size_t numBlocksAll_ = 0;
    // The number of blocks that are skipped by looking only at their metadata
    // (because the graph IDs of the block did not match the query).
    size_t numBlocksSkippedBecauseOfGraph_ = 0;
    size_t numBlocksPostprocessed_ = 0;
    // The number of blocks that contain updated (inserted or deleted) triples.
    size_t numBlocksWithUpdate_ = 0;
    // If a LIMIT or OFFSET is present we possibly read more rows than we
    // actually yield.
    size_t numElementsRead_ = 0;
    size_t numElementsYielded_ = 0;
    std::chrono::milliseconds blockingTime_ = std::chrono::milliseconds::zero();

    // Update this metadata, given the metadata from `blockAndMetadata`.
    // Currently updates: `numBlocksPostprocessed_`, `numBlocksWithUpdate_`,
    // `numElementsRead_`, and `numBlocksRead_`.
    void update(const DecompressedBlockAndMetadata& blockAndMetadata);
    // `nullopt` means the block was skipped because of the graph filters, else
    // call the overload directly above.
    void update(
        const std::optional<DecompressedBlockAndMetadata>& blockAndMetadata);

    // Aggregate the metadata from `newValue` into this metadata.
    void aggregate(const LazyScanMetadata& newValue);
  };

  using IdTableGeneratorInputRange =
      ad_utility::InputRangeTypeErased<IdTable, LazyScanMetadata>;

 private:
  // The allocator used to allocate intermediate buffers.
  mutable Allocator allocator_;

  // The file that stores the actual permutations.
  ad_utility::File file_;

  // This setting controls whether filtering on the graph column and
  // deduplication of rows is performed during scanning. Deactivating this is
  // used for materialized views where repeated rows are meaningful.
  bool useGraphPostProcessing_;

 public:
  explicit CompressedRelationReader(Allocator allocator, ad_utility::File file,
                                    bool useGraphPostProcessing = true)
      : allocator_{std::move(allocator)},
        file_{std::move(file)},
        useGraphPostProcessing_{useGraphPostProcessing} {}

  // Helper function that enables a comparison of a triple with an `Id` in the
  // function `getBlocksForJoin` below.  If the given triple matches `col0Id` of
  // the given `ScanSpecification`, then `col1Id` is returned. If the given
  // triple matches neither, a sentinel value is returned `ScanSpecification`,
  // or `Id::max` if it is higher).
  static Id getRelevantIdFromTriple(
      CompressedBlockMetadata::PermutedTriple triple,
      const ScanSpecAndBlocksAndBounds& metadataAndBlocks);

  // Get the blocks (an ordered subset of the blocks that are passed in via the
  // `metadataAndBlocks`) where the `col1Id` can theoretically match one of the
  // elements in the `joinColumn` (The col0Id is fixed and specified by the
  // `metadataAndBlocks`). The join column of the scan is the first column that
  // is not fixed by the `metadataAndBlocks`, so the middle column (col1) in
  // case the `metadataAndBlocks` doesn't contain a `col1Id`, or the last column
  // (col2) else.
  // Additionally, return the number of blocks in the input that (according to
  // their metadata) might contain IDs `<=` any value in the `joinColumn`. Note
  // that this is an offset into `metadataAndBlocks` and not to be
  // confused with the globally assigned `blockIndex_`.
  struct GetBlocksForJoinResult {
    std::vector<CompressedBlockMetadata> matchingBlocks_;
    size_t numHandledBlocks{0};
  };
  static GetBlocksForJoinResult getBlocksForJoin(
      ql::span<const Id> joinColumn,
      const ScanSpecAndBlocksAndBounds& metadataAndBlocks);

  // For each of `metadataAndBlocks, metadataAndBlocks2` get the blocks (an
  // ordered subset of the blocks in the `scanMetadata` that might contain
  // matching elements in the following scenario: The result of
  // `metadataAndBlocks` is joined with the result of `metadataAndBlocks2`. For
  // each of the inputs the join column is the first column that is not fixed by
  // the metadata, so the middle column (col1) in case the `scanMetadata`
  // doesn't contain a `col1Id`, or the last column (col2) else.
  static std::array<std::vector<CompressedBlockMetadata>, 2> getBlocksForJoin(
      const ScanSpecAndBlocksAndBounds& metadataAndBlocks,
      const ScanSpecAndBlocksAndBounds& metadataAndBlocks2);

  /**
   * @brief For a permutation XYZ, retrieve all Z for given X and Y (if `col1Id`
   * is set) or all YZ for a given X (if `col1Id` is `std::nullopt`.
   *
   * @param metadata The metadata of the given X.
   * @param col1Id The ID for Y. If `std::nullopt`, then the Y will be also
   * returned as a column.
   * @param blocks The metadata of the on-disk blocks for the given
   * permutation.
   * @param file The file in which the permutation is stored.
   * @param result The ID table to which we write the result. It must have
   * exactly one column.
   * @param cancellationHandle An `CancellationException` will be thrown if the
   * cancellationHandle runs out during the execution of this function.
   *
   * The arguments `metadata`, `blocks`, and `file` must all be obtained from
   * the same `CompressedRelationWriter` (see `CompressedRelationWriter.h`).
   */
  IdTable scan(const ScanSpecAndBlocks& scanSpecAndBlocks,
               ColumnIndicesRef additionalColumns,
               const CancellationHandle& cancellationHandle,
               const LocatedTriplesPerBlock& locatedTriplesPerBlock,
               const LimitOffsetClause& limitOffset = {}) const;

  // Similar to `scan` (directly above), but the result of the scan is lazily
  // computed and returned as a generator of the single blocks that are scanned.
  // The blocks are guaranteed to be in order.
  CompressedRelationReader::IdTableGeneratorInputRange lazyScan(
      const ScanSpecification& scanSpec,
      std::vector<CompressedBlockMetadata> relevantBlockMetadata,
      ColumnIndices additionalColumns,
      const CancellationHandle& cancellationHandle,
      const LocatedTriplesPerBlock& locatedTriplesPerBlock,
      const LimitOffsetClause& limitOffset = {}) const;

  // Retrieve all triples in the given block, ignoring updates. This is used in
  // `DeltaTriples::vacuum` to determine update triples that have no effect and
  // thus can be dropped.
  IdTable readBlockWithoutLocatedTriples(CompressedBlockMetadata block,
                                         ColumnIndices additionalColumns) const;

  // Get the exact size of the result of the scan, taking the given located
  // triples into account. This requires locating the triples exactly in each
  // of the relevant blocks.
  size_t getResultSizeOfScan(
      const ScanSpecAndBlocks& scanSpecAndBlocks,
      const LocatedTriplesPerBlock& locatedTriplesPerBlock) const;

  // Get a lower and an upper bound for the size of the result of the scan. For
  // this call, it is enough that each located triple knows the block to which
  // it belongs (which is the case for `LocatedTriplesPerBlock`).
  std::pair<size_t, size_t> getSizeEstimateForScan(
      const ScanSpecAndBlocks& scanSpecAndBlocks,
      const LocatedTriplesPerBlock& locatedTriplesPerBlock) const;

 private:
  // Common implementation of `getResultSizeOfScan` and `getSizeEstimateForScan`
  // above.
  template <bool exactSize>
  std::pair<size_t, size_t> getResultSizeImpl(
      const ScanSpecAndBlocks& scanSpecAndBlocks,
      const LocatedTriplesPerBlock& locatedTriplesPerBlock) const;

 public:
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  // Lazily compute the distinct `col0Id`s of a full scan of the permutation
  // that this reader reads from (none of the columns of
  // `scanSpecAndBlocks.scanSpec_` may be fixed).
  //
  // If `addGraphColumn` is false, the yielded `IdTable`s have a single column
  // that contains the distinct `col0Id`s. If it is true, they have a second
  // column with the graph IDs, and the pairs of `col0Id` and graph ID are
  // distinct. In both cases the yielded tables are sorted, and their
  // concatenation is sorted and free of duplicates.
  //
  // If `idFilter` is specified, only `col0Id`s that are contained in it are
  // returned. It has to be sorted in ascending order and must neither contain
  // duplicates nor undefined IDs. Blocks that cannot contain any of the
  // requested IDs are then not read at all.
  //
  // Blocks whose contribution can already be determined from their metadata
  // alone (which is the case for almost all blocks that only contain a single
  // `col0Id`, see `columnValuesAreKnownFromMetadata`) are never read, which
  // makes this much cheaper than a full scan followed by a `DISTINCT`.
  //
  // The `LazyScanMetadata` of the returned generator is that of the inner scan
  // over the blocks that actually had to be read, with `numBlocksAll_` set to
  // the total number of blocks of the scan.
  //
  // NOTE: This reader and `locatedTriplesPerBlock` have to be kept alive until
  // the returned generator has been fully consumed.
  //
  // The helper classes for the implementation live in `DistinctCol0Ids.h`.
  cppcoro::generator<IdTable, LazyScanMetadata> getDistinctCol0Ids(
      ScanSpecAndBlocks scanSpecAndBlocks, bool addGraphColumn,
      std::optional<std::vector<Id>> idFilter,
      CancellationHandle cancellationHandle,
      const LocatedTriplesPerBlock& locatedTriplesPerBlock) const;
#endif

  // Return true iff the values of the first `numColumns` columns of all the
  // triples of the given block are already known from its metadata alone,
  // which is the case iff
  // 1. All the triples of the block agree on those columns. The metadata knows
  //    this because it stores the first and the last triple of the block,
  //    including the delta triples that were inserted into it (see
  //    `LocatedTriplesPerBlock::updateAugmentedMetadata`), so if those agree,
  //    then so do all the triples in between.
  // 2. The block still contains at least one triple. Delta triples might have
  //    deleted all of them, but we can rule that out if there are fewer delta
  //    triples for the block than it has rows, as each delta triple can delete
  //    at most one of them.
  //
  // NOTE: The *number* of triples of the block is not known in this case, as
  // delta triples may have deleted some of them (and inserted others). Use
  // `contentsAreKnownFromMetadata` if you need that.
  static bool columnValuesAreKnownFromMetadata(
      const CompressedBlockMetadata& block, size_t numColumns,
      const LocatedTriplesPerBlock& locatedTriples);

  // Return true iff the complete contents of the given block, restricted to
  // its first `numColumns` columns, are already known from its metadata alone,
  // including the number of triples. In addition to
  // `columnValuesAreKnownFromMetadata` this requires that there are no delta
  // triples for the block at all.
  static bool contentsAreKnownFromMetadata(
      const CompressedBlockMetadata& block, size_t numColumns,
      const LocatedTriplesPerBlock& locatedTriples);

  // Determine the distinct values and their counts for the column at
  // `columnIndex` (must be 0 or 1). Used for GROUP BY optimizations.
  IdTable getDistinctColIdsAndCounts(
      ColumnIndex columnIndex, const ScanSpecAndBlocks& scanSpecAndBlocks,
      const CancellationHandle& cancellationHandle,
      const LocatedTriplesPerBlock& locatedTriplesPerBlock,
      const LimitOffsetClause& limitOffset) const;

  std::optional<CompressedRelationMetadata> getMetadataForSmallRelation(
      const ScanSpecAndBlocks& scanSpecAndBlocks, Id col0Id,
      const LocatedTriplesPerBlock&) const;

  // Return the number of `CompressedBlockMetadata` values contained in given
  // `BlockMetadataRanges` object.
  static size_t getNumberOfBlockMetadataValues(
      const BlockMetadataRanges& blockMetadata);

  // Retrieves the corresponding materialized `CompressedBlockMetadata` vector
  // to the given `BlockMetadataRanges blockMetadata`.
  static std::vector<CompressedBlockMetadata>
  convertBlockMetadataRangesToVector(const BlockMetadataRanges& blockMetadata);

  // Get the relevant `BlockMetadataRanges` sections of the given
  // `BlockMetadataRanges blockMetadata` for the blocks that contain the
  // triples that have the relationId/col0Id specified by
  // `ScanSpecification scanSpec`. If the `col1Id` is specified (not `nullopt`),
  // then the blocks are additionally filtered by the given `col1Id`.
  static BlockMetadataRanges getRelevantBlocks(
      const ScanSpecification& scanSpec,
      const BlockMetadataRanges& blockMetadata);

  // Get the first and the last triple that the result of a `scan` with the
  // given arguments would lead to, ignoring any graph filters set for
  // `metadataAndBlocks`. So this always returns the first and last triple we
  // would get in any graph. Return `nullopt` if the scan result would be empty.
  // This function is used to more efficiently filter the blocks of index scans
  // between joining them to get better estimates for the beginning and end of
  // incomplete blocks.
  std::optional<ScanSpecAndBlocksAndBounds::FirstAndLastTriple>
  getFirstAndLastTripleIgnoringGraph(
      const ScanSpecAndBlocks& metadataAndBlocks,
      const LocatedTriplesPerBlock& locatedTriplesPerBlock) const;

  // Get access to the underlying allocator
  const Allocator& allocator() const { return allocator_; }

  // Allow to construct a `CompressedRelationReader` using a different
  // allocator. The underlying file descriptor is duplicated (instead of
  // opening the file again by name), so this also works when the file has
  // been renamed since it was opened (see `File::duplicateForReading`).
  CompressedRelationReader makeReaderWithReboundAllocator(
      Allocator allocator) const {
    return CompressedRelationReader{std::move(allocator),
                                    file_.duplicateForReading(),
                                    useGraphPostProcessing_};
  }

  // Return the set of all graph IDs that occur in the blocks of
  // `scanSpecAndBlocks`, including the `locatedTriplesPerBlock`. A block is
  // only decompressed if its metadata says that it contains a graph that has
  // not been seen before, or if the metadata contains no graph information
  // at all (more than `MAX_NUM_GRAPHS_STORED_IN_BLOCK_METADATA` graphs).
  ad_utility::HashSetWithMemoryLimit<Id::T> computeUniqueGraphIds(
      const CompressedRelationReader::ScanSpecAndBlocks& scanSpecAndBlocks,
      const LocatedTriplesPerBlock& locatedTriplesPerBlock,
      const CancellationHandle& cancellationHandle,
      const Allocator& allocator) const;

 private:
  // Read the block that is identified by the `blockMetaData` from the `file`.
  // Only the columns specified by `columnIndices` are read.
  CompressedBlock readCompressedBlockFromFile(
      const CompressedBlockMetadata& blockMetaData,
      ColumnIndicesRef columnIndices) const;

  // Decompress the `compressedBlock`. The number of rows that the block will
  // have after decompression must be passed in via the `numRowsToRead`
  // argument. It is typically obtained from the corresponding
  // `CompressedBlockMetaData`.
  DecompressedBlock decompressBlock(const CompressedBlock& compressedBlock,
                                    size_t numRowsToRead) const;

  // Helper function used by `decompressBlock` and
  // `decompressBlockToExistingIdTable`. Decompress the `compressedColumn` and
  // store the result at the `iterator`. For the `numRowsToRead` argument, see
  // the documentation of `decompressBlock`.
  template <typename Iterator>
  static void decompressColumn(const std::vector<char>& compressedColumn,
                               size_t numRowsToRead, Iterator iterator);

  // Read and decompress the parts of the block given by `blockMetaData` (which
  // identifies the block) and `scanConfig` (which specifies the part of that
  // block).
  std::optional<DecompressedBlockAndMetadata> readAndDecompressBlock(
      const CompressedBlockMetadata& blockMetaData,
      const ScanImplConfig& scanConfig) const;

  // Like `readAndDecompressBlock`, and postprocess by merging the located
  // triples (if any) and applying the graph filters (if any), both specified
  // as part of the `scanConfig`.
  DecompressedBlockAndMetadata decompressAndPostprocessBlock(
      const CompressedBlock& compressedBlock, size_t numRowsToRead,
      const CompressedRelationReader::ScanImplConfig& scanConfig,
      const CompressedBlockMetadata& metadata) const;

  // Read, decompress, and postprocess the part of the block according to
  // `blockMetadata` (which identifies the block) and `scanConfig` (which
  // specifies the part of that block, graph filters, and located triples).
  //
  // NOTE: When all triples in the block match the `col1Id`, this method makes
  // an unnecessary copy of the block. Therefore, if you know that you need the
  // whole block, use `readAndDecompressBlock` instead.
  DecompressedBlock readPossiblyIncompleteBlock(
      const ScanSpecification& scanSpec, const ScanImplConfig& scanConfig,
      const CompressedBlockMetadata& blockMetadata,
      std::optional<std::reference_wrapper<LazyScanMetadata>> scanMetadata,
      const LocatedTriplesPerBlock&) const;

  // Yield all the blocks in the range `[beginBlock, endBlock)`. If the
  // `columnIndices` are set, only the specified columns from the blocks
  // are yielded, else all columns are yielded. The blocks are yielded
  // in the correct order, but asynchronously read and decompressed using
  // multiple worker threads.
  template <typename T>
  IdTableGeneratorInputRange asyncParallelBlockGenerator(
      T beginBlock, T endBlock, const ScanImplConfig& scanConfig,
      CancellationHandle cancellationHandle,
      LimitOffsetClause& limitOffset) const;

  // Return a vector that consists of the concatenation of `baseColumns` and
  // `additionalColumns`
  static std::vector<ColumnIndex> prepareColumnIndices(
      std::initializer_list<ColumnIndex> baseColumns,
      ColumnIndicesRef additionalColumns);

  // Return the number of columns that should be read (except for the graph
  // column and any payload columns, this is one of 0, 1, 2, 3) and whether the
  // graph column is contained in `columns`.
  static std::pair<size_t, bool> prepareLocatedTriples(
      ColumnIndicesRef columns);

  // If `col1Id` is specified, `return {1, additionalColumns...}`, else return
  // `{0, 1, additionalColumns}`.
  // These are exactly the columns that are returned by a scan depending on
  // whether the `col1Id` is specified or not.
  static std::vector<ColumnIndex> prepareColumnIndices(
      const ScanSpecification& scanSpec, ColumnIndicesRef additionalColumns);

  static ScanImplConfig getScanConfig(
      const ScanSpecification& scanSpec, ColumnIndicesRef additionalColumns,
      const LocatedTriplesPerBlock& locatedTriples);
};

// TODO<joka921>
/*
 * 1. Also let the compressedRelationReader know about the number of columns
 * that the given permutation has.
 * 2. Then add assertions that we only get valid column indices specified.
 */

#endif  // QLEVER_SRC_INDEX_COMPRESSEDRELATIONREADER_H
