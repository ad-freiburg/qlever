// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#ifndef QLEVER_COMPRESSEDRELATION_H
#define QLEVER_COMPRESSEDRELATION_H

#include <algorithm>
#include <vector>

#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "index/ConstantsIndexBuilding.h"
#include "util/BufferedVector.h"
#include "util/Cache.h"
#include "util/ConcurrentCache.h"
#include "util/File.h"
#include "util/Generator.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/SerializeArray.h"
#include "util/Serializer/SerializeVector.h"
#include "util/Serializer/Serializer.h"
#include "util/Timer.h"
#include "util/TypeTraits.h"

// Forward declaration of the `IdTable` class.
class IdTable;

// Currently our indexes have two columns (the first column of a triple
// is stored in the respective metadata). This might change in the future when
// we add a column for patterns or functional relations like rdf:type.
static constexpr int NumColumns = 2;
// Two columns of IDs that are buffered in a file if they become too large.
// This is the format in which the raw two-column data for a single relation is
// passed around during the index building.
using BufferedIdTable =
    columnBasedIdTable::IdTable<Id, NumColumns, ad_utility::BufferedVector<Id>>;

// This type is used to buffer small relations that will be stored in the same
// block.
using SmallRelationsBuffer = columnBasedIdTable::IdTable<Id, NumColumns>;

// Sometimes we do not read/decompress  all the columns of a block, so we have
// to use a dynamic `IdTable`.
using DecompressedBlock = IdTable;

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
  ad_utility::File outfile_;
  std::vector<CompressedBlockMetadata> blockBuffer_;
  CompressedBlockMetadata currentBlockData_;
  SmallRelationsBuffer buffer_;
  size_t numBytesPerBlock_;

 public:
  /// Create using a filename, to which the relation data will be written.
  explicit CompressedRelationWriter(ad_utility::File f, size_t numBytesPerBlock)
      : outfile_{std::move(f)}, numBytesPerBlock_{numBytesPerBlock} {}

  /**
   * Add a complete (single) relation.
   *
   * \param col0Id The ID of the relation, that is, the value of X for a
   * permutation XYZ.
   *
   * \param col1And2Ids The sorted data of the relation, that is, the sequence
   * of all pairs of Y and Z for the given X.
   *
   * \param numDistinctCol1 The number of distinct values for X (from which we
   * can also calculate the average multiplicity and whether the relation is
   * functional, so we don't need to store that
   * explicitly).
   *
   * \return The Metadata of the relation that was added.
   */
  CompressedRelationMetadata addRelation(Id col0Id,
                                         const BufferedIdTable& col1And2Ids,
                                         size_t numDistinctCol1);

  /// Get all the CompressedBlockMetaData that were created by the calls to
  /// addRelation. This also closes the writer. The typical workflow is:
  /// add all relations and then call this method.
  auto getFinishedBlocks() && {
    finish();
    return std::move(blockBuffer_);
  }

  // Compute the multiplicity of given the number of elements and the number of
  // distinct elements. It is basically `numElements / numDistinctElements` with
  // the following addition: the result will only be exactly `1.0` if
  // `numElements == numDistinctElements`, s.t. `1.0` is equivalent to
  // `functional`. Note that this is not automatically ensured by the division
  // because of the numeric properties of `float`.
  static float computeMultiplicity(size_t numElements,
                                   size_t numDistinctElements);

 private:
  /// Finish writing all relations which have previously been added, but might
  /// still be in some internal buffer.
  void finish() {
    writeBufferedRelationsToSingleBlock();
    outfile_.close();
  }

  // Compress the contents of `buffer_` into a single block and write it to
  // outfile_. Update `currentBlockData_` with the meta data of the written
  // block. Then clear `buffer_`.
  void writeBufferedRelationsToSingleBlock();

  // Compress the relation from `data` into one or more blocks, depending on
  // its size. Write the blocks to `outfile_` and append all the created
  // block metadata to `blockBuffer_`.
  void writeRelationToExclusiveBlocks(Id col0Id, const BufferedIdTable& data);

  // Compress the `column` and write it to the `outfile_`. Return the offset and
  // size of the compressed column in the `outfile_`.
  CompressedBlockMetadata::OffsetAndCompressedSize compressAndWriteColumn(
      std::span<const Id> column);
};

/// Manage the reading of relations from disk that have been previously written
/// using the `CompressedRelationWriter`.
class CompressedRelationReader {
 public:
  using Allocator = ad_utility::AllocatorWithLimit<Id>;
  using TimeoutTimer = ad_utility::SharedConcurrentTimeoutTimer;

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
    size_t blockingTimeMs_ = 0;
  };

  using IdTableGenerator = cppcoro::generator<IdTable, LazyScanMetadata>;

 private:
  // This cache stores a small number of decompressed blocks. Its current
  // purpose is to make the e2e-tests run fast. They contain many SPARQL queries
  // with ?s ?p ?o triples in the body.
  // Note: The cache is thread-safe and using it does not change the semantics
  // of this class, so it is safe to mark it as `mutable` to make the `scan`
  // functions below `const`.
  mutable ad_utility::ConcurrentCache<
      ad_utility::HeapBasedLRUCache<off_t, DecompressedBlock>>
      blockCache_{20ul};

  // The allocator used to allocate intermediate buffers.
  mutable Allocator allocator_;

 public:
  explicit CompressedRelationReader(Allocator allocator)
      : allocator_{std::move(allocator)} {}
  /**
   * @brief For a permutation XYZ, retrieve all YZ for a given X.
   *
   * @param metadata The metadata of the given X.
   * @param blockMetadata The metadata of the on-disk blocks for the given
   * permutation.
   * @param file The file in which the permutation is stored.
   * @param timer If specified (!= nullptr) a `TimeoutException` will be thrown
   *          if the timer runs out during the exeuction of this function.
   *
   * The arguments `metadata`, `blocks`, and `file` must all be obtained from
   * The same `CompressedRelationWriter` (see below).
   */
  IdTable scan(const CompressedRelationMetadata& metadata,
               std::span<const CompressedBlockMetadata> blockMetadata,
               ad_utility::File& file, const TimeoutTimer& timer) const;

  // Similar to `scan` (directly above), but the result of the scan is lazily
  // computed and returned as a generator of the single blocks that are scanned.
  // The blocks are guaranteed to be in order.
  IdTableGenerator lazyScan(CompressedRelationMetadata metadata,
                            std::vector<CompressedBlockMetadata> blockMetadata,
                            ad_utility::File& file, TimeoutTimer timer) const;

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

  // For each of `metadataAndBlocks1, metadataAndBlocks2` get the blocks (an
  // ordered subset of the blocks in the `scanMetadata` that might contain
  // matching elements in the following scenario: The result of
  // `metadataAndBlocks1` is joined with the result of `metadataAndBlocks2`. For
  // each of the inputs the join column is the first column that is not fixed by
  // the metadata, so the middle column (col1) in case the `scanMetadata`
  // doesn't contain a `col1Id`, or the last column (col2) else.
  static std::array<std::vector<CompressedBlockMetadata>, 2> getBlocksForJoin(
      const MetadataAndBlocks& metadataAndBlocks1,
      const MetadataAndBlocks& metadataAndBlocks2);

  /**
   * @brief For a permutation XYZ, retrieve all Z for given X and Y.
   *
   * @param metadata The metadata of the given X.
   * @param col1Id The ID for Y.
   * @param blocks The metadata of the on-disk blocks for the given
   * permutation.
   * @param file The file in which the permutation is stored.
   * @param result The ID table to which we write the result. It must have
   * exactly one column.
   * @param timer If specified (!= nullptr) a `TimeoutException` will be
   * thrown if the timer runs out during the exeuction of this function.
   *
   * The arguments `metadata`, `blocks`, and `file` must all be obtained from
   * The same `CompressedRelationWriter` (see below).
   */
  IdTable scan(const CompressedRelationMetadata& metadata, Id col1Id,
               std::span<const CompressedBlockMetadata> blocks,
               ad_utility::File& file,
               const TimeoutTimer& timer = nullptr) const;

  // Similar to `scan` (directly above), but the result of the scan is lazily
  // computed and returned as a generator of the single blocks that are scanned.
  // The blocks are guaranteed to be in order.
  IdTableGenerator lazyScan(CompressedRelationMetadata metadata, Id col1Id,
                            std::vector<CompressedBlockMetadata> blockMetadata,
                            ad_utility::File& file, TimeoutTimer timer) const;

  // Only get the size of the result for a given permutation XYZ for a given X
  // and Y. This can be done by scanning one or two blocks. Note: The overload
  // of this function where only the X is given is not needed, as the size of
  // these scans can be retrieved from the `CompressedRelationMetadata`
  // directly.
  size_t getResultSizeOfScan(const CompressedRelationMetadata& metaData,
                             Id col1Id,
                             const vector<CompressedBlockMetadata>& blocks,
                             ad_utility::File& file) const;

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
      const MetadataAndBlocks& metadataAndBlocks, ad_utility::File& file) const;

  // Get access to the underlying allocator
  const Allocator& allocator() const { return allocator_; }

 private:
  // Read the block that is identified by the `blockMetaData` from the `file`.
  // If `columnIndices` is `nullopt`, then all columns of the block are read,
  // else only the specified columns are read.
  static CompressedBlock readCompressedBlockFromFile(
      const CompressedBlockMetadata& blockMetaData, ad_utility::File& file,
      std::optional<std::vector<size_t>> columnIndices);

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
  // decompress and return it.
  // If `columnIndices` is `nullopt`, then all columns of the block are read,
  // else only the specified columns are read.
  DecompressedBlock readAndDecompressBlock(
      const CompressedBlockMetadata& blockMetadata, ad_utility::File& file,
      std::optional<std::vector<size_t>> columnIndices) const;

  // Read the block that is identified by the `blockMetadata` from the `file`,
  // decompress and return it. Before returning, delete all rows where the col0
  // ID / relation ID does not correspond with the `relationMetadata`, or where
  // the `col1Id` doesn't match. For this to work, the block has to be one of
  // the blocks that actually store triples from the given `relationMetadata`'s
  // relation, else the behavior is undefined.
  DecompressedBlock readPossiblyIncompleteBlock(
      const CompressedRelationMetadata& relationMetadata,
      std::optional<Id> col1Id, ad_utility::File& file,
      const CompressedBlockMetadata& blockMetadata,
      std::optional<std::reference_wrapper<LazyScanMetadata>> scanMetadata)
      const;

  // Yield all the blocks in the range `[beginBlock, endBlock)`. If the
  // `columnIndices` are set, that only the specified columns from the blocks
  // are yielded, else the complete blocks are yielded. The blocks are yielded
  // in the correct order, but asynchronously read and decompressed using
  // multiple worker threads.
  IdTableGenerator asyncParallelBlockGenerator(
      auto beginBlock, auto endBlock, ad_utility::File& file,
      std::optional<std::vector<size_t>> columnIndices,
      TimeoutTimer timer) const;

  // A helper function to abstract away the timeout check:
  static void checkTimeout(
      const ad_utility::SharedConcurrentTimeoutTimer& timer) {
    if (timer) {
      timer->wlock()->checkTimeoutAndThrow("IndexScan :");
    }
  }
};

#endif  // QLEVER_COMPRESSEDRELATION_H
