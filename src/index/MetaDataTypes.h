// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <array>
#include <utility>
#include <vector>

#include "../global/Id.h"
#include "../util/File.h"
#include "../util/HashMap.h"
#include "../util/Serializer/SerializeVector.h"

using std::array;
using std::pair;
using std::vector;

// Check index_layout.md for explanations (expected comments).
// Removed comments here so that not two places had to be kept up-to-date.
//
// (joka921): After restructuring this file only contains the basic single
// building blocks of the metaData for one relation.

static const uint64_t IS_FUNCTIONAL_MASK = 0x0100000000000000;
static const uint64_t HAS_BLOCKS_MASK = 0x0200000000000000;
static const uint64_t NOF_ELEMENTS_MASK = 0x000000FFFFFFFFFF;
static const uint64_t MAX_NOF_ELEMENTS = NOF_ELEMENTS_MASK;

class BlockMetaData {
 public:
  BlockMetaData() : _firstLhs(0), _startOffset(0) {}

  BlockMetaData(Id lhs, off_t start) : _firstLhs(lhs), _startOffset(start) {}

  Id _firstLhs;
  off_t _startOffset;
};

template <typename Serializer>
void serialize(Serializer& serializer, BlockMetaData& meta) {
  serializer | meta._firstLhs;
  serializer | meta._startOffset;
}

class FullRelationMetaData {
 public:
  FullRelationMetaData();

  FullRelationMetaData(Id relId, off_t startFullIndex, size_t nofElements,
                       double col1Mult, double col2Mult, bool isFunctional,
                       bool hasBlocks);

  static const FullRelationMetaData empty;

  size_t getNofBytesForFulltextIndex() const;

  // Returns true if there is exactly one RHS for each LHS in the relation
  bool isFunctional() const;

  bool hasBlocks() const;

  // Handle the fact that those properties are encoded in the same
  // size_t as the number of elements.
  void setIsFunctional(bool isFunctional);

  void setHasBlocks(bool hasBlocks);

  void setCol1LogMultiplicity(uint8_t mult);
  void setCol2LogMultiplicity(uint8_t mult);
  uint8_t getCol1LogMultiplicity() const;
  uint8_t getCol2LogMultiplicity() const;

  size_t getNofElements() const;

  // The size this object will require when serialized to file.
  // size_t bytesRequired() const;

  off_t getStartOfLhs() const;

  Id _relId;
  off_t _startFullIndex;

  // operators needed for checking of emptyness
  // inequality is the common case, so we implement this
  bool operator!=(const FullRelationMetaData& other) const {
    return _relId != other._relId ||
           _typeMultAndNofElements != other._typeMultAndNofElements ||
           _startFullIndex != other._startFullIndex;
  }

  // __________________________________________________________________
  bool operator==(const FullRelationMetaData& other) const {
    return !(*this != other);
  }

  // __________________________________________________________________________
  template <typename Serializer>
  friend void serialize(Serializer& serializer, FullRelationMetaData& meta) {
    serializer | meta._relId;
    serializer | meta._startFullIndex;
    serializer | meta._typeMultAndNofElements;
  }

 private:
  // first byte: type
  // second byte: log(col1Multiplicity)
  // third byte: log(col2Multiplicity)
  // other 5 bytes: the nof elements.
  uint64_t _typeMultAndNofElements;
};

class BlockBasedRelationMetaData {
 public:
  BlockBasedRelationMetaData();

  BlockBasedRelationMetaData(off_t startRhs, off_t offsetAfter,
                             const vector<BlockMetaData>& blocks);

  // The size this object will require when serialized to file.
  // size_t bytesRequired() const;

  // Takes a LHS and returns the offset into the file at which the
  // corresponding block can be read as well as the nof bytes to read.
  // If the relation is functional, this offset will be located in the
  // range of the FullIndex, otherwise it will be reference into the lhs list.
  // Reading nofBytes from the offset will yield a block which contains
  // the desired lhs if such a block exists at all.
  // If the lhs does not exists at all, this will only be clear after reading
  // said block.
  pair<off_t, size_t> getBlockStartAndNofBytesForLhs(Id lhs) const;

  // Gets the block after the one returned by getBlockStartAndNofBytesForLhs.
  // This is necessary for finding rhs upper bounds for the last item in a
  // block.
  // If this is equal to the block returned by getBlockStartAndNofBytesForLhs,
  // it means it is the last block and the offsetAfter can be used.
  pair<off_t, size_t> getFollowBlockForLhs(Id lhs) const;

  off_t _startRhs;
  off_t _offsetAfter;
  vector<BlockMetaData> _blocks;
};

template <typename Serializer>
void serialize(Serializer& serializer, BlockBasedRelationMetaData& metaData) {
  serializer | metaData._startRhs;
  serializer | metaData._offsetAfter;
  serializer | metaData._blocks;
}

class RelationMetaData {
 public:
  explicit RelationMetaData(const FullRelationMetaData& rmdPairs)
      : _rmdPairs(rmdPairs), _rmdBlocks(nullptr) {}

  off_t getStartOfLhs() const { return _rmdPairs.getStartOfLhs(); }

  size_t getNofBytesForFulltextIndex() const {
    return _rmdPairs.getNofBytesForFulltextIndex();
  }

  // Returns true if there is exactly one RHS for each LHS in the relation
  bool isFunctional() const { return _rmdPairs.isFunctional(); }

  bool hasBlocks() const { return _rmdPairs.hasBlocks(); }

  size_t getNofElements() const { return _rmdPairs.getNofElements(); }

  uint8_t getCol1LogMultiplicity() const {
    return _rmdPairs.getCol1LogMultiplicity();
  }

  uint8_t getCol2LogMultiplicity() const {
    return _rmdPairs.getCol2LogMultiplicity();
  }

  const FullRelationMetaData& _rmdPairs;
  const BlockBasedRelationMetaData* _rmdBlocks;
};
