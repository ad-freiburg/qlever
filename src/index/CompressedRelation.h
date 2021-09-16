//
// Created by johannes on 01.07.21.
//

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

// The meta data of a compressed block of Id triples in a certain permutation.
struct CompressedBlockMetaData {
  off_t _offsetInFile;
  size_t _compressedSize;  // in Bytes
  size_t _numRows;
  // e.g. in the POS permutation, "P" (not stored in the block) is col0),
  // "O" is the col1.
  // first and last are inclusive, the lastXXX is still contained in the block.
  Id _col0FirstId;
  Id _col0LastId;
  Id _col1FirstId;
  Id _col1LastId;

  bool operator==(const CompressedBlockMetaData&) const = default;
};

// How to serialize block meta data
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

// The meta data of a compressed relation. (e.g. in the PSO permutation,
// all triples with the same P form a relation.
struct CompressedRelationMetaData {
  Id _col0Id;
  size_t _numRows;
  float _multiplicityCol1;  // e.g. in PSO the multiplicity of "S"
  float _multiplicityCol2;  // e.g. in PSO the multiplicity of "O"
  // If there are multiple Ids in the same block (e.g. for SPO, OSP index), then
  // all of these IDs/relations span exactly one block and it suffices to store
  // their offset in the (uncompressed!) block. The special value Id(-1)
  // signals, that this relation owns all its blocks exclusively;
  Id _offsetInBlock = Id(-1);

  size_t getNofElements() const { return _numRows; }

  // Setters and getters for the multiplicities
  float getCol1Multiplicity() const { return _multiplicityCol1; }
  float getCol2Multiplicity() const { return _multiplicityCol2; }
  void setCol1Multiplicity(float mult) { _multiplicityCol1 = mult; }
  void setCol2Multiplicity(float mult) { _multiplicityCol2 = mult; }

  bool isFunctional() const { return _multiplicityCol1 == 1.0; }

  // A special value for an "empty" or "nonexisting" meta data, needed for
  // the MmapBased metaData.
  static CompressedRelationMetaData emptyMetaData() {
    size_t m = size_t(-1);
    return CompressedRelationMetaData{m, m, 0, 0, m};
  }

  // Equal if all members are equal.
  bool operator==(const CompressedRelationMetaData&) const = default;

  /**
   * @brief Perform a scan for one col0Id i.e. retrieve all YZ from the XYZ
   * permutation for a specific col0Id value of X
   * @tparam Permutation The permutations Index::POS()... have different types
   * @param col0Id The col0Id (in Id space) for which to search, e.g. fixed
   * value for O in OSP permutation.
   * @param result The Id table to which we will write. Must have 2 columns.
   * @param permutation The Permutation to use (in particularly POS(), SOP(),...
   * members of Index class).
   */
  // The IdTable is a rather expensive type, so we don't include it here.
  // but we can also not forward declare it because it is actually an alias.
  template <class Permutation, typename IdTableImpl>
  static void scan(Id col0Id, IdTableImpl* result,
                   const Permutation& permutation,
                   ad_utility::SharedConcurrentTimeoutTimer timer = nullptr);

  /**
   * @brief Perform a scan for two keys i.e. retrieve all Z from the XYZ
   * permutation for specific key values of X and Y.
   * @tparam Permutation The permutations Index::POS()... have different types
   * @param keyFirst The first key (as a raw string that is yet to be
   * transformed to index space) for which to search, e.g. fixed value for O in
   * OSP permutation.
   * @param keySecond The second key (as a raw string that is yet to be
   * transformed to index space) for which to search, e.g. fixed value for S in
   * OSP permutation.
   * @param result The Id table to which we will write. Must have 2 columns.
   * @param permutation The Permutation to use (in particularly POS(), SOP,...
   * members of Index class).
   */
  template <class PermutationInfo, typename IdTableImpl>
  static void scan(const Id count, const Id& col1Id, IdTableImpl* result,
                   const PermutationInfo& permutation,
                   ad_utility::SharedConcurrentTimeoutTimer timer = nullptr);

 private:
  // some helper functions for reading and decompressing of blocks.

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

// How to serialize a CompressedRelationMetaData
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
  CompressedBlockMetaData _currentBlockData;
  ad_utility::serialization::ByteBufferWriteSerializer _buffer;

 public:
  /// Create using a filename, to which the relation data will be written.
  CompressedRelationWriter(ad_utility::File f) : _outfile{std::move(f)} {}

  /// Add a complete (single) relation
  /// \param col0Id the Id of the relation (e.g. the P in PSO permutation)
  /// \param data the sorted data of the relation (e.g. vector of (S, O) in PSO)
  /// \param numDistinctCol1 the number of distinct elements in the 1st colun (S
  /// in PSO), needed for calculating the multiplicity \param functional is this
  /// relation functional
  void addRelation(Id col0Id,
                   const ad_utility::BufferedVector<array<Id, 2>>& data,
                   size_t numDistinctCol1, bool functional);

  /// Finish writing all relations which have previously been added, but might
  /// still be in some internal buffer.
  void finish() { writeBufferedRelationsToSingleBlock(); }

  /// Get all the CompressedRelationMetaData that was created by the calls to
  /// addRelation. This meta data is then deleted from the
  /// CompressedRelationWriter. Typical workflow: add all relations, then call
  /// finish() and then call this method
  auto getFinishedMetaData() {
    return std::move(_metaDataBuffer);
    _metaDataBuffer.clear();
  }

  /// Get all the CompressedBlockMetaData that was created by the calls to
  /// addRelation. This meta data is then deleted from the
  /// CompressedRelationWriter. Typical workflow: add all relations, then call
  /// finish() and then call this method
  auto getFinishedBlocks() {
    return std::move(_blockBuffer);
    _blockBuffer.clear();
  }

 private:
  // Compress the _buffer into a single block and write it to _outfile.
  // Update _currentBlockData with the metaData of the written Block.
  // Clear the _buffer.
  void writeBufferedRelationsToSingleBlock();

  // Compress the relation from arg data into one or more blocks, depending on
  // its size. Write the _blocks to _outfile and append all the created
  // block meta data to the _blockBuffer.
  void writeRelationToExclusiveBlocks(
      Id col0Id, const ad_utility::BufferedVector<array<Id, 2>>& data);
};

#endif  // QLEVER_COMPRESSEDRELATION_H
