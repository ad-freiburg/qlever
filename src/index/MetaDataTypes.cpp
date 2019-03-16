// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./MetaDataTypes.h"
#include <stdio.h>
#include <algorithm>
#include <cmath>
#include "../util/ReadableNumberFact.h"
#include "./MetaDataHandler.h"

const FullRelationMetaData FullRelationMetaData::empty(-1, -1, -1, 1.0, 1.0,
                                                       false, false);
// _____________________________________________________________________________
FullRelationMetaData::FullRelationMetaData()
    : _relId(0), _startFullIndex(0), _typeMultAndNofElements(0) {}

// _____________________________________________________________________________
FullRelationMetaData::FullRelationMetaData(Id relId, off_t startFullIndex,
                                           size_t nofElements, double col1Mult,
                                           double col2Mult, bool isFunctional,
                                           bool hasBlocks)
    : _relId(relId),
      _startFullIndex(startFullIndex),
      _typeMultAndNofElements(nofElements) {
  assert(col1Mult >= 1);
  assert(col2Mult >= 1);
  double c1ml = log2(col1Mult);
  double c2ml = log2(col2Mult);
  uint8_t c1 = c1ml > 255 ? uint8_t(255) : uint8_t(c1ml);
  uint8_t c2 = c2ml > 255 ? uint8_t(255) : uint8_t(c2ml);
  setIsFunctional(isFunctional);
  setHasBlocks(hasBlocks);
  setCol1LogMultiplicity(c1);
  setCol2LogMultiplicity(c2);
}

// _____________________________________________________________________________
size_t FullRelationMetaData::getNofBytesForFulltextIndex() const {
  return getNofElements() * 2 * sizeof(Id);
}

// _____________________________________________________________________________
bool FullRelationMetaData::isFunctional() const {
  return (_typeMultAndNofElements & IS_FUNCTIONAL_MASK) != 0;
}

// _____________________________________________________________________________
bool FullRelationMetaData::hasBlocks() const {
  return (_typeMultAndNofElements & HAS_BLOCKS_MASK) != 0;
}

// _____________________________________________________________________________
size_t FullRelationMetaData::getNofElements() const {
  return size_t(_typeMultAndNofElements & NOF_ELEMENTS_MASK);
}

// _____________________________________________________________________________
void FullRelationMetaData::setIsFunctional(bool isFunctional) {
  if (isFunctional) {
    _typeMultAndNofElements |= IS_FUNCTIONAL_MASK;
  } else {
    _typeMultAndNofElements &= ~IS_FUNCTIONAL_MASK;
  }
}

// _____________________________________________________________________________
void FullRelationMetaData::setHasBlocks(bool hasBlocks) {
  if (hasBlocks) {
    _typeMultAndNofElements |= HAS_BLOCKS_MASK;
  } else {
    _typeMultAndNofElements &= ~HAS_BLOCKS_MASK;
  }
}

// _____________________________________________________________________________
void FullRelationMetaData::setCol1LogMultiplicity(uint8_t mult) {
  // Reset a current value
  _typeMultAndNofElements &= 0xFF00FFFFFFFFFFFF;
  // Set the new one
  _typeMultAndNofElements |= (uint64_t(mult) << 48);
}

// _____________________________________________________________________________
void FullRelationMetaData::setCol2LogMultiplicity(uint8_t mult) {
  // Reset a current value
  _typeMultAndNofElements &= 0xFFFF00FFFFFFFFFF;
  // Set the new one
  _typeMultAndNofElements |= (uint64_t(mult) << 40);
}

// _____________________________________________________________________________
uint8_t FullRelationMetaData::getCol1LogMultiplicity() const {
  return uint8_t((_typeMultAndNofElements & 0x00FF000000000000) >> 48);
}

// _____________________________________________________________________________
uint8_t FullRelationMetaData::getCol2LogMultiplicity() const {
  return uint8_t((_typeMultAndNofElements & 0x0000FF0000000000) >> 40);
}

// _____________________________________________________________________________
pair<off_t, size_t> BlockBasedRelationMetaData::getBlockStartAndNofBytesForLhs(
    Id lhs) const {
  // get the first block where the first Id is greater or equal to what we are
  // looking for.
  auto it = std::lower_bound(
      _blocks.begin(), _blocks.end(), lhs,
      [](const BlockMetaData& a, Id lhs) { return a._firstLhs < lhs; });

  // Go back one block unless perfect lhs match.
  if (it == _blocks.end() || it->_firstLhs > lhs) {
    // if we already are at the first block this means that we
    // will have an empty result since the first
    // entry is already too big. In this case our result will
    // be empty and we can perform a short cut.
    if (it == _blocks.begin()) {
      // Empty result: scan 0 bytes from a valid start index.
      return {it->_startOffset, 0};
    }

    it--;
  }

  off_t after;
  if ((it + 1) != _blocks.end()) {
    after = (it + 1)->_startOffset;
  } else {
    after = _startRhs;
  }

  return {it->_startOffset, after - it->_startOffset};
}

// _____________________________________________________________________________
pair<off_t, size_t> BlockBasedRelationMetaData::getFollowBlockForLhs(
    Id lhs) const {
  auto it = std::lower_bound(
      _blocks.begin(), _blocks.end(), lhs,
      [](const BlockMetaData& a, Id lhs) { return a._firstLhs < lhs; });

  // Go back one block unless perfect lhs match.
  if (it == _blocks.end() || it->_firstLhs > lhs) {
    AD_CHECK(it != _blocks.begin());
    it--;
  }

  // Advance one block again is possible
  if ((it + 1) != _blocks.end()) {
    ++it;
  }

  off_t after;
  if ((it + 1) != _blocks.end()) {
    after = (it + 1)->_startOffset;
  } else {
    // In this case after is the beginning of the rhs list,
    after = _startRhs;
  }

  return pair<off_t, size_t>(it->_startOffset, after - it->_startOffset);
}

// _____________________________________________________________________________
FullRelationMetaData& FullRelationMetaData::createFromByteBuffer(
    unsigned char* buffer) {
  _relId = *reinterpret_cast<Id*>(buffer);
  _startFullIndex = *reinterpret_cast<off_t*>(buffer + sizeof(_relId));
  _typeMultAndNofElements =
      *reinterpret_cast<uint64_t*>(buffer + sizeof(Id) + sizeof(off_t));
  return *this;
}

// _____________________________________________________________________________
BlockBasedRelationMetaData& BlockBasedRelationMetaData::createFromByteBuffer(
    unsigned char* buffer) {
  _startRhs = *reinterpret_cast<off_t*>(buffer);
  _offsetAfter = *reinterpret_cast<off_t*>(buffer + sizeof(_startRhs));
  size_t nofBlocks = *reinterpret_cast<size_t*>(buffer + sizeof(_startRhs) +
                                                sizeof(_offsetAfter));
  _blocks.resize(nofBlocks);
  memcpy(_blocks.data(),
         buffer + sizeof(_startRhs) + sizeof(_offsetAfter) + sizeof(nofBlocks),
         nofBlocks * sizeof(BlockMetaData));

  return *this;
}

// _____________________________________________________________________________
size_t FullRelationMetaData::bytesRequired() const {
  return sizeof(_relId) + sizeof(_startFullIndex) +
         sizeof(_typeMultAndNofElements);
}

// _____________________________________________________________________________
off_t FullRelationMetaData::getStartOfLhs() const {
  AD_CHECK(hasBlocks());
  return _startFullIndex + 2 * sizeof(Id) * getNofElements();
}

// _____________________________________________________________________________
size_t BlockBasedRelationMetaData::bytesRequired() const {
  return sizeof(_startRhs) + sizeof(_offsetAfter) + sizeof(size_t) +
         _blocks.size() * sizeof(BlockMetaData);
}

// _____________________________________________________________________________
BlockBasedRelationMetaData::BlockBasedRelationMetaData()
    : _startRhs(0), _offsetAfter(0), _blocks() {}

// _____________________________________________________________________________
BlockBasedRelationMetaData::BlockBasedRelationMetaData(
    off_t startRhs, off_t offsetAfter, const vector<BlockMetaData>& blocks)
    : _startRhs(startRhs), _offsetAfter(offsetAfter), _blocks(blocks) {}
