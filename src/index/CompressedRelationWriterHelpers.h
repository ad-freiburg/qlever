//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_COMPRESSEDRELATIONWRITERHELPERS_H
#define QLEVER_COMPRESSEDRELATIONWRITERHELPERS_H

#include "./ConstantsIndexBuilding.h"
#include "../util/BackgroundStxxlSorter.h"

struct Block {
  // If true then the triples from this col0ID be written to multiple exclusive
  // blocks. If false then this is the only block for this col0Id that
  // exists.
  bool _writeToExclusiveBlocks;
  Id _col0Id;
  std::vector<std::array<Id, 2>> _col1And2Ids;
};

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
template<typename WriteBlock>
struct BlockPusherT {
  bool _isFinished = false;
  uint64_t offsetInBlock;
  Id col0FirstId;
  Id col0LastId;
  std::vector<std::array<Id, 2>> secondAndThirdColumn;
  WriteBlock _writeBlock;
  explicit BlockPusherT(WriteBlock writeBlock): _writeBlock(std::move(writeBlock)) {}

  void push(Block nextBlock) {
    if (nextBlock._writeToExclusiveBlocks) {
      _writeBlock(col0FirstId, col0LastId, secondAndThirdColumn);
      secondAndThirdColumn.clear();
      _writeBlock(nextBlock._col0Id, nextBlock._col0Id,
                  std::move(nextBlock._col1And2Ids));
      offsetInBlock = uint64_t(-1);
    } else {
      if (secondAndThirdColumn.empty()) {
        col0FirstId = nextBlock._col0Id;
      }
      col0LastId = nextBlock._col0Id;
      offsetInBlock = secondAndThirdColumn.size();
      secondAndThirdColumn.insert(secondAndThirdColumn.end(),
                                  nextBlock._col1And2Ids.begin(),
                                  nextBlock._col1And2Ids.end());
      auto blocksize = BLOCKSIZE_COMPRESSED_METADATA / (2 * sizeof(Id));
      if (secondAndThirdColumn.size() >= blocksize) {
        _writeBlock(col0FirstId, col0LastId, secondAndThirdColumn);
        secondAndThirdColumn.clear();
      }
    }
  }

  void finish() {
    if (_isFinished) {
      return;
    }
    _isFinished = true;
    // There might be a leftover block
    _writeBlock(col0FirstId, col0LastId, secondAndThirdColumn);
  }

  ~BlockPusherT() {
    finish();
  }
};

template<typename BlockPusher>
struct InternalTriplePusher{
  Id _col0Id;
  BlockPusher* _blockPusher;
  std::vector<std::array<Id, 2>> secondAndThirdColumn;
  bool hasExclusiveBlocks = false;
  bool _isFinished = false;
  static constexpr auto blocksize = BLOCKSIZE_COMPRESSED_METADATA / (2 * sizeof(Id));
  InternalTriplePusher(Id col0Id, BlockPusher& blockPusher) : _col0Id{col0Id}, _blockPusher(&blockPusher) {
    secondAndThirdColumn.reserve(blocksize);
  }

  void push(std::array<Id, 2> nextTuple) {
    secondAndThirdColumn.push_back(nextTuple);
    if (secondAndThirdColumn.size() >= blocksize) {
      hasExclusiveBlocks = true;
      _blockPusher->push({true, _col0Id, std::move(secondAndThirdColumn)});
      secondAndThirdColumn.clear();
      secondAndThirdColumn.reserve(blocksize);
    }
  }


  void finish() {
    if (_isFinished) {
      return;
    }
    _isFinished = true;
    _blockPusher->push(
        {hasExclusiveBlocks, _col0Id, std::move(secondAndThirdColumn)});
  }

  /*
  InternalTriplePusher(InternalTriplePusher&&) noexcept = default;
  InternalTriplePusher& operator=(InternalTriplePusher other) {
    std::swap(*this, other);
    return *this;
  }
   */

  // TODO: shall we perform this via a Mixin?
  ~InternalTriplePusher() {
    //finish();
  }
};

template<typename WriteBlock>
struct TriplePusher {
  bool _isFinished = false;
  std::optional<Id> currentC0;
  Id previousC1;
  size_t distinctC1;
  size_t sizeOfRelation = 0;
  BlockPusherT<WriteBlock> blockStore;
  InternalTriplePusher<decltype(blockStore)> tripleStore{0, blockStore};
  std::vector<CompressedRelationMetaData>& _metaDataBuffer;
  explicit TriplePusher(WriteBlock writeBlock, std::vector<CompressedRelationMetaData>& metaDataBuffer) : blockStore{std::move(writeBlock)}, _metaDataBuffer{metaDataBuffer} {}


  void pushMetadata (Id col0Id, uint64_t sizeOfRelation,
                             uint64_t numDistinctCol1,
                             uint64_t offsetInBlock) {
    bool functional = sizeOfRelation == numDistinctCol1;
    float multC1 = functional ? 1.0 : sizeOfRelation / float(numDistinctCol1);
    // Dummy value that will be overwritten later
    float multC2 = 42.42;
    LOG(TRACE) << "Done calculating multiplicities.\n";
    // This sets everything except the _offsetInBlock, which will be set
    // explicitly below.
    CompressedRelationMetaData metaData{col0Id, sizeOfRelation, multC1,
                                        multC2, offsetInBlock};
    _metaDataBuffer.push_back(metaData);
  }

  void push(std::array<Id, 3> triple) {
    if (!currentC0.has_value()) {
      currentC0 = triple[0];
      previousC1 = triple[1];
      distinctC1 = 1;
      sizeOfRelation = 0;
      tripleStore = InternalTriplePusher{currentC0.value(), blockStore};
    }
    if (triple[0] != currentC0) {
      auto previousC0 = currentC0.value();
      currentC0 = triple[0];
      tripleStore.finish();
      tripleStore = InternalTriplePusher{currentC0.value(), blockStore};
      pushMetadata(previousC0, sizeOfRelation, distinctC1,
                   blockStore.offsetInBlock);
      sizeOfRelation = 0;
      distinctC1 = 1;
    } else {
      distinctC1 += triple[1] != previousC1;
    }
    tripleStore.push({triple[1], triple[2]});
    ++sizeOfRelation;
  }

  void finish() {
    if (_isFinished) {
      return;
    }
    if (!currentC0.has_value()) {
      return;
    }
    _isFinished = true;
    tripleStore.finish();
    blockStore.finish();
    pushMetadata(currentC0.value(), sizeOfRelation, distinctC1,
                 blockStore.offsetInBlock);
  }
  ~TriplePusher() {
    finish();
  }

};

template <typename TriplePusher>
struct PermutingTriplePusher {

  using T = std::array<Id, 2>;
  struct Compare : public std::less<> {
    static T max_value() {
      static constexpr auto max = std::numeric_limits<Id>::max();
      return {max, max};
    }
    static T min_value() {
      static constexpr auto min = std::numeric_limits<Id>::min();
      return {min, min};
    }
  };

  ad_utility::BackgroundStxxlSorter<T, Compare> sorter{1 << 30};
  bool _isFinished = false;
  std::optional<Id> _currentC0;
  TriplePusher _permutedTriplePusher;

  explicit PermutingTriplePusher(TriplePusher triplePusher) : _permutedTriplePusher{std::move(triplePusher)} {}

  void push(std::array<Id, 3> triple) {
    if (!_currentC0.has_value()) {
      _currentC0 = triple[0];
    }
    if (triple[0] != _currentC0) {
      for (const auto [a, b] : sorter.sortedView()) {
        _permutedTriplePusher.push({_currentC0.value(), a, b});
      }
      sorter.clear();
      _currentC0 = triple[0];
    }
    sorter.push({triple[1], triple[2]});
  }

  void finish() {
    if (_isFinished) {
      return;
    }
    _isFinished = true;
    for (const auto [a, b] : sorter.sortedView()) {
      _permutedTriplePusher.push({_currentC0.value(), a, b});
    }
  }
  ~PermutingTriplePusher() {
    finish();
  }
};

#endif  // QLEVER_COMPRESSEDRELATIONWRITERHELPERS_H
