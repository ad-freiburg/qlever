// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./IndexMetaData.h"
#include <stdio.h>
#include <algorithm>
#include <cmath>
#include "../util/ReadableNumberFact.h"

// _____________________________________________________________________________
IndexMetaData::IndexMetaData() : _offsetAfter(0), _nofTriples(0), _name() {}

// _____________________________________________________________________________
void IndexMetaData::add(const FullRelationMetaData& rmd,
                        const BlockBasedRelationMetaData& bRmd) {
  _data[rmd._relId] = rmd;
  off_t afterExpected =
      rmd.hasBlocks() ? bRmd._offsetAfter
                      : static_cast<off_t>(rmd._startFullIndex +
                                           rmd.getNofBytesForFulltextIndex());
  if (rmd.hasBlocks()) {
    _blockData[rmd._relId] = bRmd;
  }
  if (afterExpected > _offsetAfter) {
    _offsetAfter = afterExpected;
  }
}

// _____________________________________________________________________________
off_t IndexMetaData::getOffsetAfter() const { return _offsetAfter; }

// _____________________________________________________________________________
void IndexMetaData::createFromByteBuffer(unsigned char* buf) {
  size_t nameLength = *reinterpret_cast<size_t*>(buf);
  size_t nofBytesDone = sizeof(size_t);
  _name.assign(reinterpret_cast<char*>(buf + nofBytesDone), nameLength);
  nofBytesDone += nameLength;
  size_t nofRelations = *reinterpret_cast<size_t*>(buf + nofBytesDone);
  nofBytesDone += sizeof(size_t);
  _offsetAfter = *reinterpret_cast<off_t*>(buf + nofBytesDone);
  nofBytesDone += sizeof(off_t);
  _nofTriples = 0;
  for (size_t i = 0; i < nofRelations; ++i) {
    FullRelationMetaData rmd;
    rmd.createFromByteBuffer(buf + nofBytesDone);
    _nofTriples += rmd.getNofElements();
    nofBytesDone += rmd.bytesRequired();
    if (rmd.hasBlocks()) {
      BlockBasedRelationMetaData bRmd;
      bRmd.createFromByteBuffer(buf + nofBytesDone);
      nofBytesDone += bRmd.bytesRequired();
      add(rmd, bRmd);
    } else {
      add(rmd, BlockBasedRelationMetaData());
    }
  }
}

// _____________________________________________________________________________
const RelationMetaData IndexMetaData::getRmd(Id relId) const {
  auto it = _data.find(relId);
  AD_CHECK(it != _data.end());
  RelationMetaData ret(it->second);
  if (it->second.hasBlocks()) {
    ret._rmdBlocks = &_blockData.find(it->first)->second;
  }
  return ret;
}

// _____________________________________________________________________________
bool IndexMetaData::relationExists(Id relId) const {
  return _data.count(relId) > 0;
}

// _____________________________________________________________________________
ad_utility::File& operator<<(ad_utility::File& f, const IndexMetaData& imd) {
  size_t nameLength = imd._name.size();
  f.write(&nameLength, sizeof(nameLength));
  f.write(imd._name.data(), nameLength);
  size_t nofElements = imd._data.size();
  f.write(&nofElements, sizeof(nofElements));
  f.write(&imd._offsetAfter, sizeof(imd._offsetAfter));
  for (auto it = imd._data.begin(); it != imd._data.end(); ++it) {
    f << it->second;
    if (it->second.hasBlocks()) {
      auto itt = imd._blockData.find(it->second._relId);
      AD_CHECK(itt != imd._blockData.end());
      f << itt->second;
    }
  }
  return f;
}

// _____________________________________________________________________________
string IndexMetaData::statistics() const {
  std::ostringstream os;
  std::locale loc;
  ad_utility::ReadableNumberFacet facet(1);
  std::locale locWithNumberGrouping(loc, &facet);
  os.imbue(locWithNumberGrouping);
  os << '\n';
  os << "-------------------------------------------------------------------\n";
  os << "----------------------------------\n";
  os << "Index Statistics:\n";
  os << "----------------------------------\n\n";
  os << "# Relations: " << _data.size() << '\n';
  size_t totalElements = 0;
  size_t totalBytes = 0;
  size_t totalBlocks = 0;
  for (auto it = _data.begin(); it != _data.end(); ++it) {
    totalElements += it->second.getNofElements();
    totalBytes += getTotalBytesForRelation(it->second);
    totalBlocks += getNofBlocksForRelation(it->first);
  }
  size_t totalPairIndexBytes = totalElements * 2 * sizeof(Id);
  os << "# Elements:  " << totalElements << '\n';
  os << "# Blocks:    " << totalBlocks << "\n\n";
  os << "Theoretical size of Id triples: " << totalElements * 3 * sizeof(Id)
     << " bytes \n";
  os << "Size of pair index:             " << totalPairIndexBytes
     << " bytes \n";
  os << "Total Size:                     " << totalBytes << " bytes \n";
  os << "-------------------------------------------------------------------\n";
  return os.str();
}

// _____________________________________________________________________________
size_t IndexMetaData::getNofBlocksForRelation(const Id id) const {
  auto it = _blockData.find(id);
  if (it != _blockData.end()) {
    return it->second._blocks.size();
  } else {
    return 0;
  }
}

// _____________________________________________________________________________
size_t IndexMetaData::getTotalBytesForRelation(
    const FullRelationMetaData& frmd) const {
  auto it = _blockData.find(frmd._relId);
  if (it != _blockData.end()) {
    return static_cast<size_t>(it->second._offsetAfter - frmd._startFullIndex);
  } else {
    return frmd.getNofBytesForFulltextIndex();
  }
}

// _____________________________________________________________________________
size_t IndexMetaData::getNofDistinctC1() const { return _data.size(); }

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
  auto it = std::lower_bound(
      _blocks.begin(), _blocks.end(), lhs,
      [](const BlockMetaData& a, Id lhs) { return a._firstLhs < lhs; });

  // Go back one block unless perfect lhs match.
  if (it == _blocks.end() || it->_firstLhs > lhs) {
    AD_CHECK(it != _blocks.begin());
    it--;
  }

  off_t after;
  if ((it + 1) != _blocks.end()) {
    after = (it + 1)->_startOffset;
  } else {
    after = _startRhs;
  }

  return pair<off_t, size_t>(it->_startOffset, after - it->_startOffset);
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
