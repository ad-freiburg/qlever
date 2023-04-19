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
#include "util/Serializer/ByteBufferSerializer.h"
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
using DecompressedBlock = columnBasedIdTable::IdTable<Id, 0>;

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
  // For example, in the PSO permutation, col0 is the P and col1 is the S. The
  // col0 ID is not stored in the block. First and last are meant inclusively,
  // that is, they are both part of the block.
  //
  // NOTE: Strictly speaking, we don't need `col0FirstId_` and `col1FirstId_`.
  // However, they are convenient to have and don't really harm with respect to
  // space efficiency. For example, for Wikidata, we have only around 50K blocks
  // with block size 8M and around 5M blocks with block size 80K; even the
  // latter takes only half a GB in total.
  Id col0FirstId_;
  Id col0LastId_;
  Id col1FirstId_;
  Id col1LastId_;

  // For our `DeltaTriples` (https://github.com/ad-freiburg/qlever/pull/916), we
  // need to know the least significant `Id` of the last triple as well.
  Id col2LastId_;

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
  serializer | arg.col0FirstId_;
  serializer | arg.col0LastId_;
  serializer | arg.col1FirstId_;
  serializer | arg.col1LastId_;
  serializer | arg.col2LastId_;
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

  // We currently always store two columns (the second and third column of a
  // triple). This might change in the future when we might also store
  // patterns and functional relations. Factor out this magic constant already
  // now to make the code more readable.
  static constexpr size_t numColumns() { return NumColumns; }

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
  std::vector<CompressedRelationMetadata> metaDataBuffer_;
  std::vector<CompressedBlockMetadata> blockBuffer_;
  CompressedBlockMetadata currentBlockData_;
  SmallRelationsBuffer buffer_;
  size_t numBytesPerBlock_;

 public:
  /// Create using a filename, to which the relation data will be written.
  explicit CompressedRelationWriter(
      ad_utility::File f,
      size_t numBytesPerBlock = BLOCKSIZE_COMPRESSED_METADATA)
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
   */
  void addRelation(Id col0Id, const BufferedIdTable& col1And2Ids,
                   size_t numDistinctCol1);

  /// Finish writing all relations which have previously been added, but might
  /// still be in some internal buffer.
  void finish() {
    writeBufferedRelationsToSingleBlock();
    outfile_.close();
  }

  /// Get the complete CompressedRelationMetaData created by the calls to
  /// addRelation. This meta data is then deleted from the
  /// CompressedRelationWriter. The typical workflow is: add all relations,
  /// then call `finish()` and then call this method.
  auto getFinishedMetaData() {
    auto result = std::move(metaDataBuffer_);
    metaDataBuffer_.clear();
    return result;
  }

  /// Get all the CompressedBlockMetaData that were created by the calls to
  /// addRelation. This meta data is then deleted from the
  /// CompressedRelationWriter. The typical workflow is: add all relations,
  /// then call `finish()` and then call this method.
  auto getFinishedBlocks() {
    auto result = std::move(blockBuffer_);
    blockBuffer_.clear();
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

 private:
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

 public:
  /**
   * @brief For a permutation XYZ, retrieve all YZ for a given X.
   *
   * @param metadata The metadata of the given X.
   * @param blockMetadata The metadata of the on-disk blocks for the given
   * permutation.
   * @param file The file in which the permutation is stored.
   * @param result The ID table to which we write the result. It must have
   * exactly two columns.
   * @param timer If specified (!= nullptr) a `TimeoutException` will be thrown
   *          if the timer runs out during the exeuction of this function.
   *
   * The arguments `metaData`, `blocks`, and `file` must all be obtained from
   * The same `CompressedRelationWriter` (see below).
   */
  void scan(const CompressedRelationMetadata& metadata,
            const vector<CompressedBlockMetadata>& blockMetadata,
            ad_utility::File& file, IdTable* result,
            ad_utility::SharedConcurrentTimeoutTimer timer) const;

  /**
   * @brief For a permutation XYZ, retrieve all Z for given X and Y.
   *
   * @param metaData The metadata of the given X.
   * @param col1Id The ID for Y.
   * @param blocks The metadata of the on-disk blocks for the given permutation.
   * @param file The file in which the permutation is stored.
   * @param result The ID table to which we write the result. It must have
   * exactly one column.
   * @param timer If specified (!= nullptr) a `TimeoutException` will be thrown
   *              if the timer runs out during the exeuction of this function.
   *
   * The arguments `metaData`, `blocks`, and `file` must all be obtained from
   * The same `CompressedRelationWriter` (see below).
   */
  void scan(const CompressedRelationMetadata& metaData, Id col1Id,
            const vector<CompressedBlockMetadata>& blocks,
            ad_utility::File& file, IdTable* result,
            ad_utility::SharedConcurrentTimeoutTimer timer = nullptr) const;

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
  static DecompressedBlock decompressBlock(
      const CompressedBlock& compressedBlock, size_t numRowsToRead);

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
  static DecompressedBlock readAndDecompressBlock(
      const CompressedBlockMetadata& blockMetaData, ad_utility::File& file,
      std::optional<std::vector<size_t>> columnIndices);
};

#endif  // QLEVER_COMPRESSEDRELATION_H
