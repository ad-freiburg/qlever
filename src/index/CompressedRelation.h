// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#ifndef QLEVER_COMPRESSEDRELATION_H
#define QLEVER_COMPRESSEDRELATION_H

#include <algorithm>
#include <vector>

#include "../global/Id.h"
#include "../util/BufferedVector.h"
#include "../util/File.h"
#include "../util/Serializer/SerializeVector.h"
#include "../util/Serializer/Serializer.h"
#include "../util/Timer.h"
#include "../util/CoroToStateMachine.h"

// The meta data of a compressed block of ID triples in an index permutation.
struct CompressedBlockMetaData {
  off_t _offsetInFile;
  size_t _compressedSize;  // In bytes.
  size_t _numRows;
  // For example, in the PSO permutation, col0 is the P and col1 is the S. The
  // col0 ID is not stored in the block. First and last are meant inclusively,
  // that is, they are both part of the block.
  Id _col0FirstId;
  Id _col0LastId;
  Id _col1FirstId;
  Id _col1LastId;

  // Two of these are equal if all members are equal.
  bool operator==(const CompressedBlockMetaData&) const = default;
};

// Serialization of the block meta data.
template <typename Serializer>
void serialize(Serializer& s, CompressedBlockMetaData& b) {
  s | b._offsetInFile;
  s | b._compressedSize;
  s | b._numRows;
  s | b._col0FirstId;
  s | b._col0LastId;
  s | b._col1FirstId;
  s | b._col1LastId;
}

// The meta data of a whole compressed "relation", where relation refers to a
// maximal sequence of triples with equal first component (e.g., P for the PSO
// permutation).
struct CompressedRelationMetaData {
  Id _col0Id;
  size_t _numRows;
  float _multiplicityCol1;  // E.g., in PSO this is the multiplicity of "S".
  float _multiplicityCol2;  // E.g., in PSO this is the multiplicity of "O".
  // If this "relation" is contained in a block together with other "relations",
  // then all of these relations are contained only in this block and
  // `_offsetInBlock` stores the offset in this block (referring to the index in
  // the uncompressed sequence of triples).  Otherwise, this "relation" is
  // stored in one or several blocks of its own, and we set `_offsetInBlock` to
  // `Id(-1)`.
  Id _offsetInBlock = Id(-1);

  size_t getNofElements() const { return _numRows; }

  // Setters and getters for the multiplicities.
  float getCol1Multiplicity() const { return _multiplicityCol1; }
  float getCol2Multiplicity() const { return _multiplicityCol2; }
  void setCol1Multiplicity(float mult) { _multiplicityCol1 = mult; }
  void setCol2Multiplicity(float mult) { _multiplicityCol2 = mult; }

  bool isFunctional() const { return _multiplicityCol1 == 1.0; }

  // A special value for an "empty" or "nonexisting" meta data. This is needed
  // for the mmap-based meta data.
  static CompressedRelationMetaData emptyMetaData() {
    size_t m = size_t(-1);
    return CompressedRelationMetaData{m, m, 0, 0, m};
  }

  // Two of these are equal if all members are equal.
  bool operator==(const CompressedRelationMetaData&) const = default;

  /**
   * @brief For a permutation XYZ, retrieve all YZ for a given X.
   *
   * @tparam Permutation The permutations Index::POS()... have different types
   *
   * @param col0Id The ID of the "relation". That is, for permutation XYZ, the
   * ID of an X.
   *
   * @param result The ID table to which we write the result, which must have
   * exactly two columns.
   *
   * @param permutation The permutation from which to scan, which is one of:
   * PSO, POS, SPO, SOP, OSO, OPS.
   */
  // The IdTable is a rather expensive type, so we don't include it here.
  // but we can also not forward declare it because it is actually an alias.
  template <class Permutation, typename IdTableImpl>
  static void scan(Id col0Id, IdTableImpl* result,
                   const Permutation& permutation,
                   ad_utility::SharedConcurrentTimeoutTimer timer = nullptr);

  /**
   * @brief For a permutation XYZ, retrieve all Z for given X and Y.
   *
   * @tparam Permutation The permutations Index::POS()... have different types
   *
   * @param col0Id The ID for X.
   *
   * @param col1Id The ID for Y.
   *
   * @param result The ID table to which we write the result.
   *
   * @param permutation The permutation from which to scan, which is one of:
   * PSO, POS, SPO, SOP, OSO, OPS.
   */
  template <class PermutationInfo, typename IdTableImpl>
  static void scan(const Id count, const Id& col1Id, IdTableImpl* result,
                   const PermutationInfo& permutation,
                   ad_utility::SharedConcurrentTimeoutTimer timer = nullptr);

 private:
  // Some helper functions for reading and decompressing blocks.

  template <class Permutation>
  static std::vector<char> readCompressedBlockFromFile(
      const CompressedBlockMetaData& block, const Permutation& permutation);

  static std::vector<std::array<Id, 2>> decompressBlock(
      const std::vector<char>& compressedBlock, size_t numRowsToRead);

  template <typename Iterator>
  static void decompressBlock(const std::vector<char>& compressedBlock,
                              size_t numRowsToRead, Iterator iterator);

  template <class Permutation>
  static std::vector<std::array<Id, 2>> readAndDecompressBlock(
      const CompressedBlockMetaData& block, const Permutation& permutation);
};

// Serialization of the compressed "relation" meta data.
template <class Serializer>
void serialize(Serializer& s, CompressedRelationMetaData& c) {
  s | c._col0Id;
  s | c._numRows;
  s | c._multiplicityCol1;
  s | c._multiplicityCol2;
  s | c._offsetInBlock;
}

/// Manage the compression and serialization of relations during the index
/// build.
class CompressedRelationWriter {
 private:
  ad_utility::File _outfile;
  std::vector<CompressedRelationMetaData> _metaDataBuffer;
  std::vector<CompressedBlockMetaData> _blockBuffer;

 public:
  /// Create using a filename, to which the relation data will be written.
  CompressedRelationWriter(ad_utility::File f) : _outfile{std::move(f)} {}

  /// Finish writing all relations which have previously been added, but might
  /// still be in some internal buffer.
  //void finish() { writeBufferedRelationsToSingleBlock(); }

  /// Get the complete CompressedRelationMetaData created by the calls to
  /// addRelation. This meta data is then deleted from the
  /// CompressedRelationWriter. The typical workflow is: add all relations,
  /// then call `finish()` and then call this method.
  auto getFinishedMetaData() {
    return std::move(_metaDataBuffer);
    _metaDataBuffer.clear();
  }

  /// Get all the CompressedBlockMetaData that were created by the calls to
  /// addRelation. This meta data is then deleted from the
  /// CompressedRelationWriter. The typical workflow is: add all relations,
  /// then call `finish()` and then call this method.
  auto getFinishedBlocks() {
    return std::move(_blockBuffer);
    _blockBuffer.clear();
  }

 public:
  ad_utility::CoroToStateMachine<std::array<Id, 3>> triplePusher();
  ad_utility::CoroToStateMachine<std::array<Id, 3>> switchedTriplePusher();
 private:

  // A block of triples with the same first column.
  struct Block {
    // Will the triples from this col0Id be written to multiple exclusive blocks.
    // If false, then this is the only block for this col0Id that exists.
    bool _toExclusiveBlocks;
    Id _col0Id;
    std::vector<std::array<Id, 2>> _col1And2Ids;
  };
  using BlockPusher =
  ad_utility::CoroToStateMachine<Block> ;

  // Write `Block`s of triples of Ids with the same col0Id. On each call to `push()` the correct `offsetInBlock` for the corresponding `col0Id` will be written
  CompressedRelationWriter::BlockPusher blockPusher(uint64_t& offsetInBlock);

  // Takes tuples of `(col1Id, col2Id)`, aggregates them to `Block`s and call `blockPusher.push()` for each of those blocks.
  ad_utility::CoroToStateMachine<std::array<Id, 2>> internalTriplePusher(Id col0Id, BlockPusher& blockPusher);

  // Compress and write a block of `[col1Id, col2Id]` pairs to disk, and append the corresponding block meta data to `_blockBuffer`
  void writeBlock(Id firstCol0Id, Id lastCol0Id,
                  const std::vector<std::array<Id, 2>>& data);
};

#endif  // QLEVER_COMPRESSEDRELATION_H
