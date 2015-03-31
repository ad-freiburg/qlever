// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <stdio.h>
#include <algorithm>
#include "./IndexMetaData.h"
#include "../util/ReadableNumberFact.h"

// _____________________________________________________________________________
IndexMetaData::IndexMetaData() : _offsetAfter(0) {
}

// _____________________________________________________________________________
void IndexMetaData::add(const RelationMetaData& rmd) {
  _data[rmd._relId] = rmd;
  if (rmd._offsetAfter > _offsetAfter) { _offsetAfter = rmd._offsetAfter; }
}


// _____________________________________________________________________________
off_t IndexMetaData::getOffsetAfter() const {
  return _offsetAfter;
}

// _____________________________________________________________________________
void IndexMetaData::createFromByteBuffer(unsigned char* buf) {
  size_t nofRelations = *reinterpret_cast<size_t*>(buf);
  size_t nofBytesDone = sizeof(size_t);

  for (size_t i = 0; i < nofRelations; ++i) {
    RelationMetaData rmd;
    rmd.createFromByteBuffer(buf + nofBytesDone);
    nofBytesDone += rmd.bytesRequired();
    add(rmd);
  }
}

// _____________________________________________________________________________
const RelationMetaData& IndexMetaData::getRmd(Id relId) const {
  auto it = _data.find(relId);
  AD_CHECK(it != _data.end());
  return it->second;
}

// _____________________________________________________________________________
bool IndexMetaData::relationExists(Id relId) const {
  return _data.count(relId) > 0;
}

// _____________________________________________________________________________
ad_utility::File& operator<<(ad_utility::File& f, const IndexMetaData& imd) {
  size_t nofElements = imd._data.size();
  f.write(&nofElements, sizeof(nofElements));
  for (auto it = imd._data.begin(); it != imd._data.end(); ++it) {
    f << it->second;
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
  size_t totalLhsBytes = 0;
  size_t totalRhsBytes = 0;
  for (unordered_map<Id, RelationMetaData>::const_iterator it = _data.begin();
       it != _data.end(); ++it) {
    totalElements += it->second._nofElements;
    totalBytes += it->second._offsetAfter - it->second._startFullIndex;
    totalBlocks += it->second._nofBlocks;
    totalLhsBytes += it->second._startRhs -
        (it->second._startFullIndex + 2 * sizeof(Id) * it->second._nofElements);
    totalRhsBytes += it->second._offsetAfter - it->second._startRhs;
  }
  size_t totalPairIndexBytes = totalElements * 2 * sizeof(Id);
  os << "# Elements:  " << totalElements << '\n';
  os << "# Blocks:    " << totalBlocks << "\n\n";
  os << "Theoretical size of Id triples: "
      << totalElements * 3 * sizeof(Id) << " bytes \n";
  os << "Size of pair index:             "
      << totalPairIndexBytes << " bytes \n";
  os << "Size of LHS lists:              " << totalLhsBytes << " bytes \n";
  os << "Size of RHS lists:              " << totalRhsBytes << " bytes \n";
  os << "Total Size:                     " << totalBytes << " bytes \n";
  os << "-------------------------------------------------------------------\n";
  return os.str();
}


// _____________________________________________________________________________
RelationMetaData::RelationMetaData() :
    _relId(0), _startFullIndex(0), _startRhs(0), _offsetAfter(0),
    _nofElements(0), _nofBlocks(0), _blocks() {
}

// _____________________________________________________________________________
RelationMetaData::RelationMetaData(Id relId, off_t startFullIndex,
    off_t startRhs, off_t offsetAfter, size_t nofElements, size_t nofBlocks,
    const vector<BlockMetaData>& blocks) :
    _relId(relId),
    _startFullIndex(startFullIndex),
    _startRhs(startRhs),
    _offsetAfter(offsetAfter),
    _nofElements(nofElements),
    _nofBlocks(nofBlocks),
    _blocks(blocks) {
}

// _____________________________________________________________________________
size_t RelationMetaData::getNofBytesForFulltextIndex() const {
  return _nofElements * 2 * sizeof(Id);
}

// _____________________________________________________________________________
bool RelationMetaData::isFunctional() const {
  return _startRhs == _offsetAfter;
}

// _____________________________________________________________________________
pair<off_t, size_t> RelationMetaData::getBlockStartAndNofBytesForLhs(
    Id lhs) const {

  auto it = std::lower_bound(_blocks.begin(), _blocks.end(), lhs,
      [](const BlockMetaData& a, Id lhs) {
        return a._firstLhs < lhs;
      });

  // Go back one block unless perfect lhs match.
  if (it == _blocks.end() || it->_firstLhs > lhs) {
    AD_CHECK(it != _blocks.begin());
    it--;
  }

  off_t after;
  if ((it + 1) != _blocks.end()) {
    after = (it + 1)->_startOffset;
  }  else {
    if (isFunctional()) {
      after = _startFullIndex + getNofBytesForFulltextIndex();
      AD_CHECK_EQ(after, _offsetAfter);
    } else {
      // In this case after is the beginning of the rhs list,
      after = _startRhs;
    }
  }

  return pair<off_t, size_t>(it->_startOffset, after - it->_startOffset);
}

// _____________________________________________________________________________
pair<off_t, size_t> RelationMetaData::getFollowBlockForLhs(
    Id lhs) const {

  auto it = std::lower_bound(_blocks.begin(), _blocks.end(), lhs,
      [](const BlockMetaData& a, Id lhs) {
        return a._firstLhs < lhs;
      });

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
  }  else {
    if (isFunctional()) {
      after = _startFullIndex + getNofBytesForFulltextIndex();
      AD_CHECK_EQ(after, _offsetAfter);
    } else {
      // In this case after is the beginning of the rhs list,
      after = _startRhs;
    }
  }

  return pair<off_t, size_t>(it->_startOffset, after - it->_startOffset);
}

// _____________________________________________________________________________
RelationMetaData& RelationMetaData::createFromByteBuffer(
    unsigned char* buffer) {

  _relId = *reinterpret_cast<Id*>(buffer);
  _startFullIndex = *reinterpret_cast<off_t*>(buffer + sizeof(Id));
  _startRhs = *reinterpret_cast<off_t*>(buffer + sizeof(Id) + sizeof(off_t));
  _offsetAfter = *reinterpret_cast<off_t*>(
      buffer + sizeof(Id) + 2 * sizeof(off_t));
  _nofElements = *reinterpret_cast<size_t*>(
      buffer + sizeof(Id) + 3 * sizeof(off_t));
  _nofBlocks = *reinterpret_cast<size_t*>(
      buffer + sizeof(Id) + 3 * sizeof(off_t) + sizeof(size_t));
  _blocks.resize(_nofBlocks);
  memcpy(_blocks.data(),
      buffer + sizeof(Id) + 3 * sizeof(off_t) + 2 * sizeof(size_t),
      _nofBlocks * sizeof(BlockMetaData));
  return *this;
}

// _____________________________________________________________________________
size_t RelationMetaData::bytesRequired() const {
  return sizeof(_relId)
      + sizeof(_startFullIndex)
      + sizeof(_startRhs)
      + sizeof(_offsetAfter)
      + sizeof(_nofElements)
      + sizeof(_nofBlocks)
      + _nofBlocks * sizeof(BlockMetaData);
}

// _____________________________________________________________________________
off_t RelationMetaData::getStartOfLhs() const {
  return _startFullIndex + _nofElements * 2 * sizeof(Id);
}
