// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <cstdio>
#include <vector>

#include "../global/Id.h"
#include "../util/Exception.h"
#include "../util/File.h"
#include "../util/Serializer/Serializer.h"
#include "../util/TypeTraits.h"

using std::vector;

class ContextListMetaData {
 public:
  ContextListMetaData()
      : _nofElements(),
        _startContextlist(0),
        _startWordlist(0),
        _startScorelist(0),
        _lastByte(0) {}

  ContextListMetaData(size_t nofElements, off_t startCl, off_t startWl,
                      off_t startSl, off_t lastByte)
      : _nofElements(nofElements),
        _startContextlist(startCl),
        _startWordlist(startWl),
        _startScorelist(startSl),
        _lastByte(lastByte) {}

  size_t _nofElements;
  off_t _startContextlist;
  off_t _startWordlist;
  off_t _startScorelist;
  off_t _lastByte;

  bool hasMultipleWords() const { return _startScorelist > _startWordlist; }

  static constexpr size_t sizeOnDisk() {
    return sizeof(size_t) + 4 * sizeof(off_t);
  }
};

class TextBlockMetaData {
 public:
  TextBlockMetaData() : _firstWordId(), _lastWordId(), _cl(), _entityCl() {}

  TextBlockMetaData(WordIndex firstWordId, WordIndex lastWordId,
                    const ContextListMetaData& cl,
                    const ContextListMetaData& entityCl)
      : _firstWordId(firstWordId),
        _lastWordId(lastWordId),
        _cl(cl),
        _entityCl(entityCl) {}

  uint64_t _firstWordId;
  uint64_t _lastWordId;
  ContextListMetaData _cl;
  ContextListMetaData _entityCl;

  static constexpr size_t sizeOnDisk() {
    return 2 * sizeof(Id) + 2 * ContextListMetaData::sizeOnDisk();
  }
  friend std::true_type allowTrivialSerialization(TextBlockMetaData);
};

ad_utility::File& operator<<(ad_utility::File& f, const TextBlockMetaData& md);

class TextMetaData {
 public:
  //! Get the corresponding block meta data for some word or entity Id range.
  //! Currently assumes that the range lies in a single block.
  const TextBlockMetaData& getBlockInfoByWordRange(const uint64_t lower,
                                                   const uint64_t upper) const;

  const TextBlockMetaData& getBlockInfoByEntityId(const uint64_t eid) const;

  bool existsTextBlockForEntityId(const uint64_t eid) const;

  size_t getBlockCount() const;

  string statistics() const;

  void addBlock(const TextBlockMetaData& md, bool isEntityBlock);

  off_t getOffsetAfter();

  const TextBlockMetaData& getBlockById(size_t id) const { return _blocks[id]; }

  size_t getNofEntities() const { return _nofEntities; }

  void setNofEntities(size_t nofEntities) { _nofEntities = nofEntities; }

  size_t getNofEntityContexts() const { return _nofEntityContexts; }

  void setNofEntityContexts(size_t n) { _nofEntityContexts = n; }

  size_t getNofTextRecords() const { return _nofTextRecords; }

  void setNofTextRecords(size_t n) { _nofTextRecords = n; }

  size_t getNofWordPostings() const { return _nofWordPostings; }

  void setNofWordPostings(size_t n) { _nofWordPostings = n; }

  size_t getNofEntityPostings() const { return _nofEntityPostings; }

  void setNofEntityPostings(size_t n) { _nofEntityPostings = n; }

  const string& getName() const { return _name; }

  void setName(const string& name) { _name = name; }

  float getAverageNofEntityContexts() const {
    return float(_nofEntityContexts) / _nofEntities;
  };

 private:
  vector<uint64_t> _blockUpperBoundWordIds;
  vector<uint64_t> _blockUpperBoundEntityIds;
  size_t _nofEntities = 0;
  size_t _nofEntityContexts = 0;
  size_t _nofTextRecords = 0;
  size_t _nofWordPostings = 0;
  size_t _nofEntityPostings = 0;
  string _name;
  vector<TextBlockMetaData> _blocks;

  // ___________________________________________________________________________
  AD_SERIALIZE_FRIEND_FUNCTION(TextMetaData) {
    serializer | arg._blockUpperBoundWordIds;
    serializer | arg._blockUpperBoundEntityIds;
    serializer | arg._nofEntities;
    serializer | arg._nofEntityContexts;
    serializer | arg._nofTextRecords;
    serializer | arg._nofWordPostings;
    serializer | arg._nofEntityPostings;
    serializer | arg._name;
    serializer | arg._blocks;
  }
};
