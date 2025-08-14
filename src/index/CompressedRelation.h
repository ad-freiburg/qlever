// Copyright 2021 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_COMPRESSEDRELATION_H
#define QLEVER_SRC_INDEX_COMPRESSEDRELATION_H

#include <type_traits>
#include <vector>

#include "backports/algorithm.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "index/KeyOrder.h"
#include "index/ScanSpecification.h"
#include "parser/data/LimitOffsetClause.h"
#include "util/CancellationHandle.h"
#include "util/File.h"
#include "util/Generator.h"
#include "util/MemorySize/MemorySize.h"
#include "util/Serializer/SerializeArrayOrTuple.h"
#include "util/Serializer/SerializeOptional.h"
#include "util/Serializer/SerializeVector.h"
#include "util/Serializer/Serializer.h"
#include "util/TaskQueue.h"

// Forward declarations
class IdTable;

class LocatedTriplesPerBlock;

// This type is used to buffer small relations that will be stored in the same
// block.
using SmallRelationsBuffer = IdTable;

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

// The metadata of a compressed block of ID triples in an index permutation.
struct CompressedBlockMetadataNoBlockIndex {
  // Since we have column-based indices, the two columns of each block are
  // stored separately (but adjacently).
  struct OffsetAndCompressedSize {
    off_t offsetInFile_;
    size_t compressedSize_;
    bool operator==(const OffsetAndCompressedSize&) const = default;
  };

  using GraphInfo = std::optional<std::vector<Id>>;

  std::vector<OffsetAndCompressedSize> offsetsAndCompressedSize_;
  size_t numRows_;

  // Store the first and the last triple of the block. First and last are meant
  // inclusively, that is, they are both part of the block. The order of the
  // triples depends on the stored permutation: For example, in the PSO
  // permutation, the first element of the triples is the P, the second one is
  // the S and the third one is the O. Note that the first key of the
  // permutation (for example the P in the PSO permutation) is not stored in
  // the blocks, but has to be retrieved via the corresponding
  // `CompressedRelationMetadata`.
  //
  // NOTE: Strictly speaking, storing one of `firstTriple_` or `lastTriple_`
  // would probably suffice. However, they make several functions much easier
  // to implement and don't really harm with respect to space efficiency. For
  // example, for Wikidata, we have only around 50K blocks with block size 8M
  // and around 5M blocks with block size 80K; even the latter takes only half
  // a GB in total.
  struct PermutedTriple {
    Id col0Id_;
    Id col1Id_;
    Id col2Id_;
    Id graphId_;
    auto operator<=>(const PermutedTriple&) const = default;

    // Formatted output for debugging.
    friend std::ostream& operator<<(std::ostream& str,
                                    const PermutedTriple& trip) {
      str << "Triple: " << trip.col0Id_ << ' ' << trip.col1Id_ << ' '
          << trip.col2Id_ << ' ' << trip.graphId_ << std::endl;
      return str;
    }

    template <typename T>
    friend std::true_type allowTrivialSerialization(PermutedTriple, T);
  };
  PermutedTriple firstTriple_;
  PermutedTriple lastTriple_;

  // If there are only few graphs contained at all in this block, then
  // the IDs of those graphs are stored here. If there are many different graphs
  // inside this block, `std::nullopt` is stored.
  std::optional<std::vector<Id>> graphInfo_;
  // True if and only if this block contains (adjacent) triples which only
  // differ in their Graph ID. Those have to be filtered out when scanning the
  // blocks.
  bool containsDuplicatesWithDifferentGraphs_;

  // Check for constant values in `firstTriple_` and `lastTriple` over all
  // columns `< columnIndex`.
  // Returns `true` if the respective column values of `firstTriple_` and
  // `lastTriple_` differ.
  bool containsInconsistentTriples(size_t columnIndex) const;

  // Check if `lastTriple_` of this block contains consistent `ValueId`s up to
  // `columnIndex` compared to `firstTriple_` of block `other`.
  bool isConsistentWith(const CompressedBlockMetadataNoBlockIndex& other,
                        size_t columnIndex) const;

  // Two of these are equal if all members are equal.
  bool operator==(const CompressedBlockMetadataNoBlockIndex&) const = default;

  // Format CompressedBlockMetadata contents for debugging.
  friend std::ostream& operator<<(
      std::ostream& str,
      const CompressedBlockMetadataNoBlockIndex& blockMetadata) {
    str << "#CompressedBlockMetadata\n(first) " << blockMetadata.firstTriple_
        << "(last) " << blockMetadata.lastTriple_
        << "num. rows: " << blockMetadata.numRows_ << ".\n";
    if (blockMetadata.graphInfo_.has_value()) {
      str << "Graphs: ";
      ad_utility::lazyStrJoin(&str, blockMetadata.graphInfo_.value(), ", ");
      str << '\n';
    }
    str << "[possibly] contains duplicates: "
        << blockMetadata.containsDuplicatesWithDifferentGraphs_ << '\n';
    return str;
  }
};

// The same as the above struct, but this block additionally knows its index.
struct CompressedBlockMetadata : CompressedBlockMetadataNoBlockIndex {
  // The index of this block in the permutation. This is required to find
  // the corresponding block from the `LocatedTriples` when only a subset of
  // blocks is being used.
  size_t blockIndex_;

  // Two of these are equal if all members are equal.
  bool operator==(const CompressedBlockMetadata&) const = default;

  // Format CompressedBlockMetadata contents for debugging.
  friend std::ostream& operator<<(
      std::ostream& str, const CompressedBlockMetadata& blockMetadata) {
    str << static_cast<const CompressedBlockMetadataNoBlockIndex&>(
        blockMetadata);
    str << "block index: " << blockMetadata.blockIndex_ << "\n";
    return str;
  }
};

// Serialization of the `OffsetAndcompressedSize` subclass.
AD_SERIALIZE_FUNCTION(CompressedBlockMetadata::OffsetAndCompressedSize) {
  serializer | arg.offsetInFile_;
  serializer | arg.compressedSize_;
}

// Serialization of the block metadata.
AD_SERIALIZE_FUNCTION(CompressedBlockMetadata) {
  serializer | arg.offsetsAndCompressedSize_;
  serializer | arg.numRows_;
  serializer | arg.firstTriple_;
  serializer | arg.lastTriple_;
  serializer | arg.graphInfo_;
  serializer | arg.containsDuplicatesWithDifferentGraphs_;
  serializer | arg.blockIndex_;
}

// `ql::span` containing `CompressedBlockMetadata` values.
using BlockMetadataSpan = ql::span<const CompressedBlockMetadata>;
// Iterator with respect to a `CompressedBlockMetadata` value of
// `std::span<const CompressedBlockMetadata>` (`BlockMetadataSpan`).
using BlockMetadataIt = BlockMetadataSpan::iterator;
// Section of relevant blocks as a subrange defined by `BlockMetadataIt`s.
using BlockMetadataRange = ql::ranges::subrange<BlockMetadataIt>;
// Vector containing `BlockMetadataRange`s.
using BlockMetadataRanges = std::vector<BlockMetadataRange>;

// The metadata of a whole compressed "relation", where relation refers to a
// maximal sequence of triples with equal first component (e.g., P for the PSO
// permutation).
struct CompressedRelationMetadata {
  Id col0Id_;
  // TODO: Is this still needed? Same for `offsetInBlock_`.
  size_t numRows_;
  float multiplicityCol1_;  // E.g., in PSO this is the multiplicity of "S".
  float multiplicityCol2_;  // E.g., in PSO this is the multiplicity of "O".
  // If this "relation" is contained in a block together with other "relations",
  // then all of these relations are contained only in this block and
  // `offsetInBlock_` stores the offset in this block (referring to the index in
  // the uncompressed sequence of triples).  Otherwise, this "relation" is
  // stored in one or several blocks of its own, and we set `offsetInBlock_` to
  // `Id(-1)`.
  uint64_t offsetInBlock_ = std::numeric_limits<uint64_t>::max();

  size_t getNofElements() const { return numRows_; }

  // Setters and getters for the multiplicities.
  float getCol1Multiplicity() const { return multiplicityCol1_; }
  float getCol2Multiplicity() const { return multiplicityCol2_; }
  void setCol1Multiplicity(float mult) { multiplicityCol1_ = mult; }
  void setCol2Multiplicity(float mult) { multiplicityCol2_ = mult; }

  bool isFunctional() const { return multiplicityCol1_ == 1.0f; }

  // Two of these are equal if all members are equal.
  bool operator==(const CompressedRelationMetadata&) const = default;
};

// Serialization of the compressed "relation" meta data.
AD_SERIALIZE_FUNCTION(CompressedRelationMetadata) {
  serializer | arg.col0Id_;
  serializer | arg.numRows_;
  serializer | arg.multiplicityCol1_;
  serializer | arg.multiplicityCol2_;
  serializer | arg.offsetInBlock_;
}

/// Manage the compression and serialization of relations during the index
/// build.
class CompressedRelationWriter {
 private:
  ad_utility::Synchronized<ad_utility::File> outfile_;
  ad_utility::Synchronized<std::vector<CompressedBlockMetadataNoBlockIndex>>
      blockBuffer_;
  // If multiple small relations are stored in the same block, keep track of the
  // first and last `col0Id`.
  Id currentBlockFirstCol0_ = Id::makeUndefined();
  Id currentBlockLastCol0_ = Id::makeUndefined();

  // The actual number of columns that is stored by this writer. Is 2 if there
  // are no additional special payloads.
  size_t numColumns_;

  ad_utility::AllocatorWithLimit<Id> allocator_ =
      ad_utility::makeUnlimitedAllocator<Id>();
  // A buffer for small relations that will be stored in the same block.
  SmallRelationsBuffer smallRelationsBuffer_{numColumns_, allocator_};
  ad_utility::MemorySize uncompressedBlocksizePerColumn_;

  // When we store a large relation with multiple blocks then we keep track of
  // its `col0Id`, mostly for sanity checks.
  Id currentCol0Id_ = Id::makeUndefined();
  size_t currentRelationPreviousSize_ = 0;

  ad_utility::TaskQueue<false> blockWriteQueue_{20, 10};
  ad_utility::timer::ThreadSafeTimer blockWriteQueueTimer_;

  // This callback is invoked for each block of small relations (which share the
  // same block), after this block has been completely handled by this writer.
  // The callback is used to efficiently pass the block from a permutation to
  // its twin permutation, which only has to re-sort and write the block.
  using SmallBlocksCallback = std::function<void(std::shared_ptr<IdTable>)>;
  SmallBlocksCallback smallBlocksCallback_;

  // A dummy value for multiplicities that can only later be determined.
  static constexpr float multiplicityDummy = 42.4242f;

 public:
  /// Create using a filename, to which the relation data will be written.
  explicit CompressedRelationWriter(
      size_t numColumns, ad_utility::File f,
      ad_utility::MemorySize uncompressedBlocksizePerColumn)
      : outfile_{std::move(f)},
        numColumns_{numColumns},
        uncompressedBlocksizePerColumn_{uncompressedBlocksizePerColumn} {}
  // Two helper types used to make the interface of the function
  // `createPermutationPair` below safer and more explicit.
  using MetadataCallback =
      std::function<void(ql::span<const CompressedRelationMetadata>)>;

  struct WriterAndCallback {
    CompressedRelationWriter& writer_;
    MetadataCallback callback_;
  };

  /**
   * @brief Write two permutations that only differ by the order of the col1 and
   * col2 (e.g. POS and PSO).
   * @param basename filename/path that will be used as a prefix for names of
   * temporary files.
   * @param writerAndCallback1 A writer for the first permutation together with
   * a callback that is called for each of the created metadata.
   * @param writerAndCallback2  The same as `writerAndCallback1`, but for the
   * other permutation.
   * @param sortedTriples The inputs as blocks of triples (plus possibly
   * additional columns). The first three columns must be sorted according to
   * the `permutation` (which corresponds to the `writerAndCallback1`.
   * @param permutation The permutation to be build (as a permutation of the
   * array `[0, 1, 2]`). The `sortedTriples` must be sorted by this permutation.
   */
  struct PermutationPairResult {
    size_t numDistinctCol0_;
    std::vector<CompressedBlockMetadata> blockMetadata_;
    std::vector<CompressedBlockMetadata> blockMetadataSwitched_;
  };
  static PermutationPairResult createPermutationPair(
      const std::string& basename, WriterAndCallback writerAndCallback1,
      WriterAndCallback writerAndCallback2,
      cppcoro::generator<IdTableStatic<0>> sortedTriples,
      qlever::KeyOrder permutation,
      const std::vector<std::function<void(const IdTableStatic<0>&)>>&
          perBlockCallbacks);

  /// Get all the CompressedBlockMetaData that were created by the calls to
  /// addRelation. This also closes the writer. The typical workflow is:
  /// add all relations and then call this method.
  std::vector<CompressedBlockMetadata> getFinishedBlocks() && {
    finish();
    auto blocks = std::move(*(blockBuffer_.wlock()));
    ql::ranges::sort(
        blocks, {}, [](const CompressedBlockMetadataNoBlockIndex& bl) {
          return std::tie(bl.firstTriple_.col0Id_, bl.firstTriple_.col1Id_,
                          bl.firstTriple_.col2Id_);
        });

    std::vector<CompressedBlockMetadata> result;
    result.reserve(blocks.size());
    // Write the correct block indices
    for (size_t i : ad_utility::integerRange(blocks.size())) {
      result.emplace_back(std::move(blocks.at(i)), i);
    }
    return result;
  }

  // Compute the multiplicity of given the number of elements and the number of
  // distinct elements. It is basically `numElements / numDistinctElements` with
  // the following addition: the result will only be exactly `1.0` if
  // `numElements == numDistinctElements`, s.t. `1.0` is equivalent to
  // `functional`. Note that this is not automatically ensured by the division
  // because of the numeric properties of `float`.
  static float computeMultiplicity(size_t numElements,
                                   size_t numDistinctElements);

  // Return the blocksize (in number of triples) of this writer. Note that the
  // actual sizes of blocks will slightly vary due to new relations starting in
  // new blocks etc.
  size_t blocksize() const {
    return uncompressedBlocksizePerColumn_.getBytes() / sizeof(Id);
  }

 private:
  /// Finish writing all relations which have previously been added, but might
  /// still be in some internal buffer.
  void finish() {
    AD_CORRECTNESS_CHECK(currentRelationPreviousSize_ == 0);
    writeBufferedRelationsToSingleBlock();
    auto timer = blockWriteQueueTimer_.startMeasurement();
    blockWriteQueue_.finish();
    timer.stop();
    outfile_.wlock()->close();
  }

  // Compress the contents of `smallRelationsBuffer_` into a single
  // block and write it to outfile_. Update `currentBlockData_` with the meta
  // data of the written block. Then clear `smallRelationsBuffer_`.
  void writeBufferedRelationsToSingleBlock();

  // Compress the `column` and write it to the `outfile_`. Return the offset and
  // size of the compressed column in the `outfile_`.
  CompressedBlockMetadata::OffsetAndCompressedSize compressAndWriteColumn(
      ql::span<const Id> column);

  // Return the number of columns that is stored inside the blocks.
  size_t numColumns() const { return numColumns_; }

  // Compress the given `block` and write it to the `outfile_`. The
  // `firstCol0Id` and `lastCol0Id` are needed to set up the block's metadata
  // which is appended to the internal buffer. If `invokeCallback` is true and
  // the `smallBlocksCallback_` is not empty, then
  // `smallBlocksCallback_(std::move(block))` is called AFTER the block has
  // completely been dealt with.
  void compressAndWriteBlock(Id firstCol0Id, Id lastCol0Id,
                             std::shared_ptr<IdTable> block,
                             bool invokeCallback);

  // Add a small relation that will be stored in a single block, possibly
  // together with other small relations.
  CompressedRelationMetadata addSmallRelation(Id col0Id, size_t numDistinctC1,
                                              IdTableView<0> relation);

  // Add a new block for a large relation that is to be stored in multiple
  // blocks. This function may only be called if one of the following holds:
  // * This is the first call to `addBlockForLargeRelation` or
  // `addSmallRelation`.
  // * The previously called function was `addSmallRelation` or
  // `finishLargeRelation`.
  // * The previously called function was `addBlockForLargeRelation` with the
  // same `col0Id`.
  void addBlockForLargeRelation(Id col0Id, std::shared_ptr<IdTable> relation);

  // This function must be called after all blocks of a large relation have been
  // added via `addBlockForLargeRelation` before any other function may be
  // called. In particular, it has to be called after the last block of the last
  // relation was added (in case this relation is large). Otherwise, an
  // assertion inside the `finish()` function (which is also called by the
  // destructor) will fail.
  CompressedRelationMetadata finishLargeRelation(size_t numDistinctC1);

  // Add a complete large relation by calling `addBlockForLargeRelation` for
  // each block in the `sortedBlocks` and then calling `finishLargeRelation`.
  // The number of distinct col1 entries will be computed from the blocks
  // directly.
  template <typename T>
  CompressedRelationMetadata addCompleteLargeRelation(Id col0Id,
                                                      T&& sortedBlocks);

  // This is a function in `CompressedRelationsTest.cpp` that tests the
  // internals of this class and therefore needs private access.
  template <typename T>
  friend std::pair<std::vector<CompressedBlockMetadata>,
                   std::vector<CompressedRelationMetadata>>
  compressedRelationTestWriteCompressedRelations(
      T inputs, std::string filename, ad_utility::MemorySize blocksize);
};

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

  // This struct stores a reference to the (optional) graphs by which a result
  // is filtered, the column in which the graph ID will reside in a result,
  // and the information whether this column is required as part of the output,
  // or whether it should be deleted after filtering. It can then filter a given
  // block according to those settings.
  struct FilterDuplicatesAndGraphs {
    ScanSpecification::Graphs desiredGraphs_;
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

   private:
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
        std::span<const CompressedBlockMetadata> blocks,
        size_t firstFreeColIndex);

    // Remove the first `numBlocksToRemove` from the `blockMetadata_`. This can
    // be used if it is known that those are not needed anymore, e.g. because
    // they have already been dealt with by a lazy `IndexScan` or `Join`.
    void removePrefix(size_t numBlocksToRemove) {
      auto it = blockMetadata_.begin();
      auto end = blockMetadata_.end();
      while (it != end) {
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

  using IdTableGenerator = cppcoro::generator<IdTable, LazyScanMetadata>;

 private:
  // The allocator used to allocate intermediate buffers.
  mutable Allocator allocator_;

  // The file that stores the actual permutations.
  ad_utility::File file_;

 public:
  explicit CompressedRelationReader(Allocator allocator, ad_utility::File file)
      : allocator_{std::move(allocator)}, file_{std::move(file)} {}

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
   * The same `CompressedRelationWriter` (see below).
   */
  IdTable scan(const ScanSpecAndBlocks& scanSpecAndBlocks,
               ColumnIndicesRef additionalColumns,
               const CancellationHandle& cancellationHandle,
               const LocatedTriplesPerBlock& locatedTriplesPerBlock,
               const LimitOffsetClause& limitOffset = {}) const;

  // Similar to `scan` (directly above), but the result of the scan is lazily
  // computed and returned as a generator of the single blocks that are scanned.
  // The blocks are guaranteed to be in order.
  CompressedRelationReader::IdTableGenerator lazyScan(
      ScanSpecification scanSpec,
      std::vector<CompressedBlockMetadata> relevantBlockMetadata,
      ColumnIndices additionalColumns, CancellationHandle cancellationHandle,
      const LocatedTriplesPerBlock& locatedTriplesPerBlock,
      LimitOffsetClause limitOffset = {}) const;

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
  // For a given relation, determine the `col1Id`s and their counts. This is
  // used for `computeGroupByObjectWithCount`.
  IdTable getDistinctCol1IdsAndCounts(
      const ScanSpecAndBlocks& scanSpecAndBlocks,
      const CancellationHandle& cancellationHandle,
      const LocatedTriplesPerBlock& locatedTriplesPerBlock) const;

  // For all `col0Ids` determine their counts. This is
  // used for `computeGroupByForFullScan`.
  IdTable getDistinctCol0IdsAndCounts(
      const ScanSpecAndBlocks& scanSpecAndBlocks,
      const CancellationHandle& cancellationHandle,
      const LocatedTriplesPerBlock& locatedTriplesPerBlock) const;

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
  // given arguments would lead to. Return `nullopt` if the scan result would
  // be empty. This function is used to more efficiently filter the blocks of
  // index scans between joining them to get better estimates for the beginning
  // and end of incomplete blocks.
  std::optional<ScanSpecAndBlocksAndBounds::FirstAndLastTriple>
  getFirstAndLastTriple(
      const ScanSpecAndBlocks& metadataAndBlocks,
      const LocatedTriplesPerBlock& locatedTriplesPerBlock) const;

  // Get access to the underlying allocator
  const Allocator& allocator() const { return allocator_; }

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
  IdTableGenerator asyncParallelBlockGenerator(
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

  // The common implementation for `getDistinctCol0IdsAndCounts` and
  // `getCol1IdsAndCounts`.
  CPP_template(typename IdGetter)(
      requires ad_utility::InvocableWithConvertibleReturnType<
          IdGetter, Id, const CompressedBlockMetadata::PermutedTriple&>) IdTable
      getDistinctColIdsAndCountsImpl(
          IdGetter idGetter, const ScanSpecAndBlocks& scanSpecAndBlocks,
          const CancellationHandle& cancellationHandle,
          const LocatedTriplesPerBlock& locatedTriplesPerBlock) const;
};

// TODO<joka921>
/*
 * 1. Also let the compressedRelationReader know about the number of columns
 * that the given permutation has.
 * 2. Then add assertions that we only get valid column indices specified.
 */

#endif  // QLEVER_SRC_INDEX_COMPRESSEDRELATION_H
