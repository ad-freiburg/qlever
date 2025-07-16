// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_INDEX_TEXTMETADATA_H
#define QLEVER_SRC_INDEX_TEXTMETADATA_H

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

  size_t getByteLengthContextList() const {
    return static_cast<size_t>(_startWordlist - _startContextlist);
  }

  size_t getByteLengthWordlist() const {
    return static_cast<size_t>(_startScorelist - _startWordlist);
  }

  size_t getByteLengthScorelist() const {
    return static_cast<size_t>(_lastByte + 1 - _startScorelist);
  }

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

  template <typename T>
  friend std::true_type allowTrivialSerialization(TextBlockMetaData, T);
};

ad_utility::File& operator<<(ad_utility::File& f, const TextBlockMetaData& md);

class TextMetaData {
 public:
  // Get the corresponding block meta data for some word or entity Id range.
  // Can be multiple blocks. Note: the range is [lower, upper], NOT [lower,
  // upper)
  vector<std::reference_wrapper<const TextBlockMetaData>>
  getBlockInfoByWordRange(const uint64_t lower, const uint64_t upper) const;

  size_t getBlockCount() const;

  std::string statistics() const;

  void addBlock(const TextBlockMetaData& md);

  off_t getOffsetAfter();

  const TextBlockMetaData& getBlockById(size_t id) const { return _blocks[id]; }

  size_t getNofTextRecords() const { return _nofTextRecords; }

  void setNofTextRecords(size_t n) { _nofTextRecords = n; }

  size_t getNofWordPostings() const { return _nofWordPostings; }

  void setNofWordPostings(size_t n) { _nofWordPostings = n; }

  size_t getNofEntityPostings() const { return _nofEntityPostings; }

  void setNofEntityPostings(size_t n) { _nofEntityPostings = n; }

  const std::string& getName() const { return _name; }

  void setName(const std::string& name) { _name = name; }

  float getAverageNofEntityContexts() const { return 1.0f; };

 private:
  vector<uint64_t> _blockUpperBoundWordIds;
  size_t _nofTextRecords = 0;
  size_t _nofWordPostings = 0;
  size_t _nofEntityPostings = 0;
  std::string _name;
  vector<TextBlockMetaData> _blocks;

  // ___________________________________________________________________________
  AD_SERIALIZE_FRIEND_FUNCTION(TextMetaData) {
    serializer | arg._blockUpperBoundWordIds;
    serializer | arg._nofTextRecords;
    serializer | arg._nofWordPostings;
    serializer | arg._nofEntityPostings;
    serializer | arg._name;
    serializer | arg._blocks;
  }
};

#endif  // QLEVER_SRC_INDEX_TEXTMETADATA_H
