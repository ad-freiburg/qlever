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
ad_utility::File& operator<<(ad_utility::File& f, const TextMetaData& md) {
  auto buf = md.getBlockCount();
  f.write(&buf, sizeof(md.getBlockCount()));
  for (const auto& b : md._blocks) {
    f << b;
  }
  f.write(&md._nofEntities, sizeof(md._nofEntities));
  f.write(&md._nofEntityContexts, sizeof(md._nofEntityContexts));
  f.write(&md._nofTextRecords, sizeof(md._nofTextRecords));
  f.write(&md._nofWordPostings, sizeof(md._nofWordPostings));
  f.write(&md._nofEntityPostings, sizeof(md._nofEntityPostings));
  size_t nameLength = md._name.size();
  f.write(&nameLength, sizeof(nameLength));
  f.write(md._name.data(), nameLength);
  return f;
}

// _____________________________________________________________________________
TextMetaData& TextMetaData::createFromByteBuffer(unsigned char* buffer) {
  size_t nofBlocks = *reinterpret_cast<size_t*>(buffer);
  off_t offset = sizeof(size_t);
  bool stepEntity = false;
  for (size_t i = 0; i < nofBlocks; ++i) {
    TextBlockMetaData tbmd;
    tbmd.createFromByteBuffer(buffer + offset);
    offset += TextBlockMetaData::sizeOnDisk();
    if (!stepEntity) {
      if (_blocks.size() == 0 ||
          _blocks.back()._lastWordId + 1 == tbmd._firstWordId) {
        _blockUpperBoundWordIds.push_back(tbmd._lastWordId);
      } else {
        stepEntity = true;
        _blockUpperBoundEntityIds.push_back(tbmd._lastWordId);
      }
    } else {
      _blockUpperBoundEntityIds.push_back(tbmd._lastWordId);
    }
    _blocks.emplace_back(tbmd);
  }
  _nofEntities = *reinterpret_cast<size_t*>(buffer + offset);
  offset += sizeof(_nofEntities);
  _nofEntityContexts = *reinterpret_cast<size_t*>(buffer + offset);
  offset += sizeof(_nofEntityContexts);
  _nofTextRecords = *reinterpret_cast<size_t*>(buffer + offset);
  offset += sizeof(_nofTextRecords);
  _nofWordPostings = *reinterpret_cast<size_t*>(buffer + offset);
  offset += sizeof(_nofWordPostings);
  _nofEntityPostings = *reinterpret_cast<size_t*>(buffer + offset);
  offset += sizeof(_nofEntityPostings);
  size_t nameLength;
  nameLength = *reinterpret_cast<size_t*>(buffer + offset);
  offset += sizeof(nameLength);
  _name.assign(reinterpret_cast<char*>(buffer + offset), nameLength);
  return *this;
}

// _____________________________________________________________________________
ad_utility::File& operator<<(ad_utility::File& f, const TextBlockMetaData& md) {
  f.write(&md._firstWordId, sizeof(md._firstWordId));
  f.write(&md._lastWordId, sizeof(md._lastWordId));
  f << md._cl << md._entityCl;
  return f;
}

// _____________________________________________________________________________
TextBlockMetaData& TextBlockMetaData::createFromByteBuffer(
    unsigned char* buffer) {
  off_t offset = 0;
  _firstWordId = *reinterpret_cast<uint64_t*>(buffer + offset);
  offset += sizeof(_firstWordId);
  _lastWordId = *reinterpret_cast<uint64_t*>(buffer + offset);
  offset += sizeof(_lastWordId);
  _cl.createFromByteBuffer(buffer + offset);
  offset += ContextListMetaData::sizeOnDisk();
  _entityCl.createFromByteBuffer(buffer + offset);
  return *this;
}

// _____________________________________________________________________________
ad_utility::File& operator<<(ad_utility::File& f,
                             const ContextListMetaData& md) {
  f.write(&md._nofElements, sizeof(md._nofElements));
  f.write(&md._startContextlist, sizeof(md._startContextlist));
  f.write(&md._startWordlist, sizeof(md._startWordlist));
  f.write(&md._startScorelist, sizeof(md._startScorelist));
  f.write(&md._lastByte, sizeof(md._lastByte));
  return f;
}

// _____________________________________________________________________________
ContextListMetaData& ContextListMetaData::createFromByteBuffer(
    unsigned char* buffer) {
  off_t offset = 0;
  _nofElements = *reinterpret_cast<size_t*>(buffer + offset);
  offset += sizeof(_nofElements);
  _startContextlist = *reinterpret_cast<off_t*>(buffer + offset);
  offset += sizeof(_startContextlist);
  _startWordlist = *reinterpret_cast<off_t*>(buffer + offset);
  offset += sizeof(_startWordlist);
  _startScorelist = *reinterpret_cast<off_t*>(buffer + offset);
  offset += sizeof(_startScorelist);
  _lastByte = *reinterpret_cast<off_t*>(buffer + offset);
  return *this;
}

// _____________________________________________________________________________
string TextMetaData::statistics() const {
  std::ostringstream os;
  std::locale loc;
  ad_utility::ReadableNumberFacet facet(1);
  std::locale locWithNumberGrouping(loc, &facet);
  os.imbue(locWithNumberGrouping);
  os << '\n';
  os << "-------------------------------------------------------------------\n";
  os << "----------------------------------\n";
  os << "Text Index Statistics:\n";
  os << "----------------------------------\n\n";
  os << "# Blocks: " << _blocks.size() << '\n';
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
  os << "-------------------------------------------------------------------\n";
  os << "# Elements: " << totalElementsClassicLists + totalElementsEntityLists
     << '\n';
  os << "  thereof:\n";
  os << "    Elements in classic lists: " << totalElementsClassicLists << '\n';
  os << "    Elements in entity lists:  " << totalElementsEntityLists << '\n';
  os << "-------------------------------------------------------------------\n";
  os << "# Entities with text: " << _nofEntities << '\n';
  os << "# Contexts for those entities: " << _nofEntityContexts << '\n';
  os << "    Average contexts / entity: " << getAverageNofEntityContexts()
     << '\n';
  os << "-------------------------------------------------------------------\n";
  os << "# Bytes: " << totalBytesClassicLists + totalBytesEntityLists << '\n';
  os << "  thereof:\n";
  os << "    Bytes in classic lists: " << totalBytesClassicLists << '\n';
  os << "    Bytes in entity lists:  " << totalBytesEntityLists << '\n';
  os << "-------------------------------------------------------------------\n";
  os << "Breakdown:\n";
  os << "    Bytes in context / doc lists: " << totalBytesCls << '\n';
  os << "    Bytes in word lists:          " << totalBytesWls << '\n';
  os << "    Bytes in score lists:         " << totalBytesSls << '\n';
  os << "-------------------------------------------------------------------\n";
  os << "\n";
  os << "-------------------------------------------------------------------\n";
  os << "Theoretical (naiive) size: "
     << (totalElementsClassicLists + totalElementsEntityLists) *
            (2 * sizeof(Id) + sizeof(Score))
     << '\n';
  os << "-------------------------------------------------------------------\n";
  return std::move(os).str();
}

// _____________________________________________________________________________
void TextMetaData::addBlock(const TextBlockMetaData& md) {
  _blocks.push_back(md);
  _blockUpperBoundWordIds.push_back(md._lastWordId);
}

// _____________________________________________________________________________
off_t TextMetaData::getOffsetAfter() {
  return _blocks.back()._entityCl._lastByte + 1;
}
