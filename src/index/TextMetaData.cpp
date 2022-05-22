// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./TextMetaData.h"

#include "../global/Constants.h"
#include "../util/ReadableNumberFact.h"

// _____________________________________________________________________________
const TextBlockMetaData& TextMetaData::getBlockInfoByWordRange(
    const uint64_t lower, const uint64_t upper) const {
  AD_CHECK_GE(upper, lower);
  assert(_blocks.size() > 0);
  assert(_blocks.size() ==
         _blockUpperBoundWordIds.size() + _blockUpperBoundEntityIds.size());

  // Binary search in the sorted _blockUpperBoundWordIds vector.
  auto it = std::lower_bound(_blockUpperBoundWordIds.begin(),
                             _blockUpperBoundWordIds.end(), lower);

  // If the word would be behind all that, return the last block
  if (it == _blockUpperBoundWordIds.end()) {
    --it;
  }

  // Binary search in the sorted _blockUpperBoundWordIds vector.
  auto upperIt = std::lower_bound(_blockUpperBoundWordIds.begin(),
                                  _blockUpperBoundWordIds.end(), upper);

  if (upper > *it) {
    AD_THROW(ad_semsearch::Exception::BAD_QUERY,
             "No words found for the given prefix. This usually means that the "
             "prefix "
             "is smaller than the configured minimum prefix size. This range "
             "spans over " +
                 std::to_string(upperIt - it) + " blocks");
  }

  // Use the info to retrieve an index.
  size_t index = static_cast<size_t>(it - _blockUpperBoundWordIds.begin());
  assert(lower <= _blocks[index]._lastWordId);
  assert(lower >= _blocks[index]._firstWordId);
  return _blocks[index];
}

// _____________________________________________________________________________
bool TextMetaData::existsTextBlockForEntityId(const uint64_t eid) const {
  assert(_blocks.size() > 0);
  assert(_blocks.size() ==
         _blockUpperBoundWordIds.size() + _blockUpperBoundEntityIds.size());

  // Binary search in the sorted _blockUpperBoundWordIds vector.
  auto it = std::lower_bound(_blockUpperBoundEntityIds.begin(),
                             _blockUpperBoundEntityIds.end(), eid);

  return (*it == eid);
}

// _____________________________________________________________________________
const TextBlockMetaData& TextMetaData::getBlockInfoByEntityId(
    const uint64_t eid) const {
  assert(_blocks.size() > 0);
  assert(_blocks.size() ==
         _blockUpperBoundWordIds.size() + _blockUpperBoundEntityIds.size());

  // Binary search in the sorted _blockUpperBoundWordIds vector.
  auto it = std::lower_bound(_blockUpperBoundEntityIds.begin(),
                             _blockUpperBoundEntityIds.end(), eid);

  assert(*it == eid);

  // Use the info to retrieve an index.
  size_t index = static_cast<size_t>(it - _blockUpperBoundEntityIds.begin()) +
                 _blockUpperBoundWordIds.size();
  assert(eid == _blocks[index]._lastWordId);
  assert(eid == _blocks[index]._firstWordId);
  return _blocks[index];
}

// _____________________________________________________________________________
size_t TextMetaData::getBlockCount() const { return _blocks.size(); }

// _____________________________________________________________________________
string TextMetaData::statistics() const {
  // TODO: Not sure which of these we still want and why they are computed here
  // (each time the statistics is shown) and not when the index build is done.
  size_t totalElementsClassicLists = 0;
  size_t totalElementsEntityLists = 0;
  size_t totalBytesClassicLists = 0;
  size_t totalBytesEntityLists = 0;
  size_t totalBytesCls = 0;
  size_t totalBytesWls = 0;
  size_t totalBytesSls = 0;
  for (size_t i = 0; i < _blocks.size(); ++i) {
    const ContextListMetaData& wcl = _blocks[i]._cl;
    const ContextListMetaData& ecl = _blocks[i]._entityCl;

    totalElementsClassicLists += wcl._nofElements;
    totalElementsEntityLists += ecl._nofElements;

    totalBytesClassicLists += 1 + wcl._lastByte - wcl._startContextlist;
    totalBytesEntityLists += 1 + ecl._lastByte - ecl._startContextlist;

    totalBytesCls += wcl._startWordlist - wcl._startContextlist;
    totalBytesCls += ecl._startWordlist - ecl._startContextlist;
    totalBytesWls += wcl._startScorelist - wcl._startWordlist;
    totalBytesWls += ecl._startScorelist - ecl._startWordlist;
    totalBytesSls += 1 + wcl._lastByte - wcl._startScorelist;
    totalBytesSls += 1 + ecl._lastByte - ecl._startScorelist;
  }
  // Show abbreviated statistics (like for the permutations, which used to have
  // very verbose statistics, too).
  std::ostringstream os;
  std::locale loc;
  ad_utility::ReadableNumberFacet facet(1);
  std::locale locWithNumberGrouping(loc, &facet);
  os.imbue(locWithNumberGrouping);
  os << "#records = " << _nofEntityContexts
     << ", #words = " << totalElementsClassicLists
     << ", #entities = " << _nofEntities << ", #blocks = " << _blocks.size();
  return std::move(os).str();
}

// _____________________________________________________________________________
void TextMetaData::addBlock(const TextBlockMetaData& md, bool isEntityBlock) {
  _blocks.push_back(md);
  if (isEntityBlock) {
    _blockUpperBoundEntityIds.push_back(md._lastWordId);
  } else {
    _blockUpperBoundWordIds.push_back(md._lastWordId);
  }
}

// _____________________________________________________________________________
off_t TextMetaData::getOffsetAfter() {
  return _blocks.back()._entityCl._lastByte + 1;
}
