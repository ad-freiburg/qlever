// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#ifndef QLEVER_COMPRESSEDRELATION_H
#define QLEVER_COMPRESSEDRELATION_H

#include <algorithm>
#include <vector>

#include "engine/idTable/CompressedExternalIdTable.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "index/ConstantsIndexBuilding.h"
#include "util/Cache.h"
#include "util/CancellationHandle.h"
#include "util/ConcurrentCache.h"
#include "util/File.h"
#include "util/Generator.h"
#include "util/MemorySize/MemorySize.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/SerializeArray.h"
#include "util/Serializer/SerializeVector.h"
#include "util/Serializer/Serializer.h"
#include "util/TaskQueue.h"
#include "util/TypeTraits.h"

// Forward declaration of the `IdTable` class.
class IdTable;

// This type is used to buffer small relations that will be stored in the same
// block.
using SmallRelationsBuffer = IdTable;

// Sometimes we do not read/decompress  all the columns of a block, so we have
// to use a dynamic `IdTable`.
using DecompressedBlock = IdTable;

// To be able to use `DecompressedBlock` with `Caches`, we need a function for
// calculating the memory used for it.
struct DecompressedBlockSizeGetter {
  ad_utility::MemorySize operator()(const DecompressedBlock& block) const {
    return ad_utility::MemorySize::bytes(block.numColumns() * block.numRows() *
                                         sizeof(Id));
  }
};

// After compression the columns have different sizes, so we cannot use an
// `IdTable`.
using CompressedBlock = std::vector<std::vector<char>>;

// The metadata of a compressed block of ID triples in an index permutation.
struct CompressedBlockMetadata {
  // Since we have column-based indices, the two columns of each block are
  // stored separately (but adjacently).
  struct OffsetAndCompressedSize {
    off_t offsetInFile_;
    size_t compressedSize_;
    bool operator==(const OffsetAndCompressedSize&) const = default;
  };
  std::vector<OffsetAndCompressedSize> offsetsAndCompressedSize_;
  size_t numRows_;

  // Store the first and the last triple of the block. First and last are meant
  // inclusively, that is, they are both part of the block. The order of the
  // triples depends on the stored permutation: For example, in the PSO
  // permutation, the first element of the triples is the P, the second one is
  // the S and the third one is the O. Note that the first key of the
  // permutation (for example the P in the PSO permutation) is not stored in the
  // blocks, but has to be retrieved via the corresponding
  // `CompressedRelationMetadata`.
  // NOTE: Strictly speaking, storing one of `firstTriple_` or `lastTriple_`
  // would probably suffice. However, they make several functions much easier to
  // implement and don't really harm with respect to space efficiency. For
  // example, for Wikidata, we have only around 50K blocks with block size 8M
  // and around 5M blocks with block size 80K; even the latter takes only half a
  // GB in total.
  struct PermutedTriple {
    Id col0Id_;
    Id col1Id_;
    Id col2Id_;
    bool operator==(const PermutedTriple&) const = default;

    // Formatted output for debugging.
    friend std::ostream& operator<<(std::ostream& str,
                                    const PermutedTriple& trip) {
      str << "Triple: " << trip.col0Id_ << ' ' << trip.col1Id_ << ' '
          << trip.col2Id_ << std::endl;
      return str;
    }

    friend std::true_type allowTrivialSerialization(PermutedTriple, auto);
  };
  PermutedTriple firstTriple_;
  PermutedTriple lastTriple_;

  // Two of these are equal if all members are equal.
  bool operator==(const CompressedBlockMetadata&) const = default;
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
}

// The metadata of a whole compressed "relation", where relation refers to a
// maximal sequence of triples with equal first component (e.g., P for the PSO
// permutation).
struct CompressedRelationMetadata {
  Id col0Id_;
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
  ad_utility::Synchronized<std::vector<CompressedBlockMetadata>> blockBuffer_;
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
      std::function<void(std::span<const CompressedRelationMetadata>)>;

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
  static std::pair<std::vector<CompressedBlockMetadata>,
                   std::vector<CompressedBlockMetadata>>
  createPermutationPair(
      const std::string& basename, WriterAndCallback writerAndCallback1,
      WriterAndCallback writerAndCallback2,
      cppcoro::generator<IdTableStatic<0>> sortedTriples,
      std::array<size_t, 3> permutation,
      const std::vector<std::function<void(const IdTableStatic<0>&)>>&
          perBlockCallbacks);

  /// Get all the CompressedBlockMetaData that were created by the calls to
  /// addRelation. This also closes the writer. The typical workflow is:
  /// add all relations and then call this method.
  std::vector<CompressedBlockMetadata> getFinishedBlocks() && {
    finish();
    auto blocks = std::move(*(blockBuffer_.wlock()));
    std::ranges::sort(blocks, {}, [](const CompressedBlockMetadata& bl) {
      return std::tie(bl.firstTriple_.col0Id_, bl.firstTriple_.col1Id_,
                      bl.firstTriple_.col2Id_);
    });
    return blocks;
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
    blockWriteQueue_.finish();
    outfile_.wlock()->close();
  }

  // Compress the contents of `smallRelationsBuffer_` into a single
  // block and write it to outfile_. Update `currentBlockData_` with the meta
  // data of the written block. Then clear `smallRelationsBuffer_`.
  void writeBufferedRelationsToSingleBlock();

  // Compress the `column` and write it to the `outfile_`. Return the offset and
  // size of the compressed column in the `outfile_`.
  CompressedBlockMetadata::OffsetAndCompressedSize compressAndWriteColumn(
      std::span<const Id> column);

  // Return the number of columns that is stored inside the blocks.
  size_t numColumns() const { return numColumns_; }

  // Compress the given `block` and write it to the `outfile_`. The
  // `firstCol0Id` and `lastCol0Id` are needed to set up the block's metadata
  // which is appended to the internal buffer.
  void compressAndWriteBlock(Id firstCol0Id, Id lastCol0Id,
                             std::shared_ptr<IdTable> block);

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
  CompressedRelationMetadata addCompleteLargeRelation(Id col0Id,
                                                      auto&& sortedBlocks);

  // This is the function in `CompressedRelationsTest.cpp` that tests the
  // internals of this class and therefore needs private access.
  friend void testCompressedRelations(const auto& inputs,
                                      std::string testCaseName,
                                      ad_utility::MemorySize blocksize);
};

using namespace std::string_view_literals;

/// Manage the reading of relations from disk that have been previously written
/// using the `CompressedRelationWriter`.
class CompressedRelationReader {
  template <typename T>
  using vector = std::vector<T>;

 public:
  using Allocator = ad_utility::AllocatorWithLimit<Id>;
  using ColumnIndicesRef = std::span<const ColumnIndex>;
  using ColumnIndices = std::vector<ColumnIndex>;

  // The metadata of a single relation together with a subset of its
  // blocks and possibly a `col1Id` for additional filtering. This is used as
  // the input to several functions below that take such an input.
  struct MetadataAndBlocks {
    const CompressedRelationMetadata relationMetadata_;
    const std::span<const CompressedBlockMetadata> blockMetadata_;
    std::optional<Id> col1Id_;

    // If set, `firstAndLastTriple_` contains the first and the last triple
    // of the specified relation (and being filtered by the `col1Id` if
    // specified). This might be different from the first triple in the first
    // block (in the case of the `firstTriple_`, similarly for `lastTriple_`)
    // because the first and last block might also contain other relations, or
    // the same relation but with different `col1Id`s.
    struct FirstAndLastTriple {
      CompressedBlockMetadata::PermutedTriple firstTriple_;
      CompressedBlockMetadata::PermutedTriple lastTriple_;
    };
    std::optional<FirstAndLastTriple> firstAndLastTriple_;
  };

  struct LazyScanMetadata {
    size_t numBlocksRead_ = 0;
    size_t numBlocksAll_ = 0;
    size_t numElementsRead_ = 0;
    std::chrono::milliseconds blockingTime_ = std::chrono::milliseconds::zero();
  };

  using IdTableGenerator = cppcoro::generator<IdTable, LazyScanMetadata>;

 private:
  // This cache stores a small number of decompressed blocks. Its current
  // purpose is to make the e2e-tests run fast. They contain many SPARQL queries
  // with ?s ?p ?o triples in the body.
  // Note: The cache is thread-safe and using it does not change the semantics
  // of this class, so it is safe to mark it as `mutable` to make the `scan`
  // functions below `const`.
  mutable ad_utility::ConcurrentCache<ad_utility::HeapBasedLRUCache<
      off_t, DecompressedBlock, DecompressedBlockSizeGetter>>
      blockCache_{20ul};

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
  static std::vector<CompressedBlockMetadata> getBlocksForJoin(
      std::span<const Id> joinColumn,
      const MetadataAndBlocks& metadataAndBlocks);

  // For each of `metadataAndBlocks, metadataAndBlocks2` get the blocks (an
  // ordered subset of the blocks in the `scanMetadata` that might contain
  // matching elements in the following scenario: The result of
  // `metadataAndBlocks` is joined with the result of `metadataAndBlocks2`. For
  // each of the inputs the join column is the first column that is not fixed by
  // the metadata, so the middle column (col1) in case the `scanMetadata`
  // doesn't contain a `col1Id`, or the last column (col2) else.
  static std::array<std::vector<CompressedBlockMetadata>, 2> getBlocksForJoin(
      const MetadataAndBlocks& metadataAndBlocks,
      const MetadataAndBlocks& metadataAndBlocks2);

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
  IdTable scan(const CompressedRelationMetadata& metadata,
               std::optional<Id> col1Id,
               std::span<const CompressedBlockMetadata> blocks,
               ColumnIndicesRef additionalColumns,
               ad_utility::SharedCancellationHandle cancellationHandle) const;

  // Similar to `scan` (directly above), but the result of the scan is lazily
  // computed and returned as a generator of the single blocks that are scanned.
  // The blocks are guaranteed to be in order.
  IdTableGenerator lazyScan(
      CompressedRelationMetadata metadata, std::optional<Id> col1Id,
      std::vector<CompressedBlockMetadata> blockMetadata,
      ColumnIndices additionalColumns,
      ad_utility::SharedCancellationHandle cancellationHandle) const;

  // Only get the size of the result for a given permutation XYZ for a given X
  // and Y. This can be done by scanning one or two blocks. Note: The overload
  // of this function where only the X is given is not needed, as the size of
  // these scans can be retrieved from the `CompressedRelationMetadata`
  // directly.
  size_t getResultSizeOfScan(
      const CompressedRelationMetadata& metaData, Id col1Id,
      const vector<CompressedBlockMetadata>& blocks) const;

  // Get the contiguous subrange of the given `blockMetadata` for the blocks
  // that contain the triples that have the relationId/col0Id that was specified
  // by the `medata`. If the `col1Id` is specified (not `nullopt`), then the
  // blocks are additionally filtered by the given `col1Id`.
  static std::span<const CompressedBlockMetadata> getBlocksFromMetadata(
      const CompressedRelationMetadata& metadata, std::optional<Id> col1Id,
      std::span<const CompressedBlockMetadata> blockMetadata);

  // The same function, but specify the arguments as the `MetadataAndBlocks`
  // struct.
  static std::span<const CompressedBlockMetadata> getBlocksFromMetadata(
      const MetadataAndBlocks& metadataAndBlocks);

  // Get the first and the last triple that the result of a `scan` with the
  // given arguments would lead to. Throw an exception if the scan result would
  // be empty. This function is used to more efficiently filter the blocks of
  // index scans between joining them to get better estimates for the begginning
  // and end of incomplete blocks.
  MetadataAndBlocks::FirstAndLastTriple getFirstAndLastTriple(
      const MetadataAndBlocks& metadataAndBlocks) const;

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

  // Similar to `decompressBlock`, but the block is directly decompressed into
  // the `table`, starting at the `offsetInTable`-th row. The `table` and the
  // `compressedBlock` must have the same number of columns, and the `table`
  // must have at least `numRowsToRead + offsetInTable` rows.
  static void decompressBlockToExistingIdTable(
      const CompressedBlock& compressedBlock, size_t numRowsToRead,
      IdTable& table, size_t offsetInTable);

  // Helper function used by `decompressBlock` and
  // `decompressBlockToExistingIdTable`. Decompress the `compressedColumn` and
  // store the result at the `iterator`. For the `numRowsToRead` argument, see
  // the documentation of `decompressBlock`.
  template <typename Iterator>
  static void decompressColumn(const std::vector<char>& compressedColumn,
                               size_t numRowsToRead, Iterator iterator);

  // Read the block that is identified by the `blockMetaData` from the `file`,
  // decompress and return it. Only the columns specified by the `columnIndices`
  // are returned.
  DecompressedBlock readAndDecompressBlock(
      const CompressedBlockMetadata& blockMetaData,
      ColumnIndicesRef columnIndices) const;

  // Read the block that is identified by the `blockMetadata` from the `file`,
  // decompress and return it. Before returning, delete all rows where the col0
  // ID / relation ID does not correspond with the `relationMetadata`, or where
  // the `col1Id` doesn't match. For this to work, the block has to be one of
  // the blocks that actually store triples from the given `relationMetadata`'s
  // relation, else the behavior is undefined. Only return the columns specified
  // by the `columnIndices`.
  DecompressedBlock readPossiblyIncompleteBlock(
      const CompressedRelationMetadata& relationMetadata,
      std::optional<Id> col1Id, const CompressedBlockMetadata& blockMetadata,
      std::optional<std::reference_wrapper<LazyScanMetadata>> scanMetadata,
      ColumnIndicesRef columnIndices) const;

  // Yield all the blocks in the range `[beginBlock, endBlock)`. If the
  // `columnIndices` are set, only the specified columns from the blocks
  // are yielded, else all columns are yielded. The blocks are yielded
  // in the correct order, but asynchronously read and decompressed using
  // multiple worker threads.
  IdTableGenerator asyncParallelBlockGenerator(
      auto beginBlock, auto endBlock, ColumnIndices columnIndices,
      ad_utility::SharedCancellationHandle cancellationHandle) const;

  // A helper function to abstract away the timeout check:
  static void checkCancellation(
      const ad_utility::SharedCancellationHandle& cancellationHandle) {
    // Not really expensive but since this should be called
    // very often, try to avoid any extra checks.
    AD_EXPENSIVE_CHECK(cancellationHandle);
    cancellationHandle->throwIfCancelled("IndexScan"sv);
  }

  // Return a vector that consists of the concatenation of `baseColumns` and
  // `additionalColumns`
  static std::vector<ColumnIndex> prepareColumnIndices(
      std::initializer_list<ColumnIndex> baseColumns,
      ColumnIndicesRef additionalColumns);

  // If `col1Id` is specified, `return {1, additionalColumns...}`, else return
  // `{0, 1, additionalColumns}`.
  // These are exactly the columns that are returned by a scan depending on
  // whether the `col1Id` is specified or not.
  static std::vector<ColumnIndex> prepareColumnIndices(
      const std::optional<Id>& col1Id, ColumnIndicesRef additionalColumns);
};

// TODO<joka921>
/*
 * 1. Also let the compressedRelationReader know about the contained block data
 * and the number of columns etc. to make the permutation class a thinner
 * wrapper.
 * 2. Then add assertions that we only get valid column indices specified.
 */

#endif  // QLEVER_COMPRESSEDRELATION_H
