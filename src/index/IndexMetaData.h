// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <array>
#include <vector>
#include <utility>
#include <unordered_map>

#include "../global/Id.h"
#include "../util/File.h"


using std::array;
using std::vector;
using std::pair;
using std::unordered_map;

// Copy & Paste from IndexLayout.txt:
//
//
// -----
// 3. Meta Data
// -----
//
// --
// a) Relation Meta Data
// --
// * rel Id
// * offset: start of FullIndex
// * offset: end of BlockIndex RHS (needed for upper bound on last block)
// * # elements
// * # blocks
// * vector of BlockMetaData (pairs)
//
//
//
// + isFunctional (# block == 0)
// + getSizeOfFullIndex()
// + pair<offset, size_t> getBlockStartAndNofBytesForLhs()
//      Get info from two BMS. Only available if not functional.
// + offset getRhsForLhs
//
// --
// b) Block Meta Data
// --
// - minLHS
// - offset: start of RHS Data

class BlockMetaData {
public:
  BlockMetaData() :_firstLhs(0), _startOffset(0) { }
  BlockMetaData(Id lhs, off_t start) :_firstLhs(lhs), _startOffset(start) { }
  Id _firstLhs;
  off_t _startOffset;
};

class RelationMetaData {
public:
  RelationMetaData();

  RelationMetaData(Id relId, off_t startFullIndex, off_t startRhs,
      off_t offsetAfter, size_t nofElements, size_t nofBlocks,
      const vector<BlockMetaData>& blocks);

  size_t getNofBytesForFulltextIndex() const;

  off_t getStartOfLhs() const;

  // Returns true if there is exactly one RHS for each LHS in the realtion
  bool isFunctional() const;

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

  // Restores meta data from raw memory.
  // Needed when registering an index on startup.
  RelationMetaData& createFromByteBuffer(unsigned char* buffer);

  // The size this object will require when serialized to file.
  size_t bytesRequired() const;

  Id _relId;
  off_t _startFullIndex;
  off_t _startRhs;
  off_t _offsetAfter;
  size_t _nofElements;
  size_t _nofBlocks;
  vector<BlockMetaData> _blocks;
};

inline ad_utility::File& operator<<(ad_utility::File& f,
    const RelationMetaData& rmd) {
  f.write(&rmd._relId, sizeof(rmd._relId));
  f.write(&rmd._startFullIndex, sizeof(rmd._startFullIndex));
  f.write(&rmd._startRhs, sizeof(rmd._startRhs));
  f.write(&rmd._offsetAfter, sizeof(rmd._offsetAfter));
  f.write(&rmd._nofElements, sizeof(rmd._nofElements));
  f.write(&rmd._nofBlocks, sizeof(rmd._nofBlocks));
  f.write(rmd._blocks.data(), rmd._nofBlocks * sizeof(BlockMetaData));
  return f;
}

class IndexMetaData {
public:
  IndexMetaData();

  void add(const RelationMetaData& rmd);
  off_t getOffsetAfter() const;
  const RelationMetaData& getRmd(Id relId) const;

  void createFromByteBuffer(unsigned char* buf);

  bool relationExists(Id relId) const;

  string statistics() const;

private:
  unordered_map<Id, RelationMetaData> _data;
  off_t _offsetAfter;

  friend ad_utility::File& operator<<(ad_utility::File& f,
      const IndexMetaData& rmd);
};

ad_utility::File& operator<<(ad_utility::File& f, const IndexMetaData& imd);