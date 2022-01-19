// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#pragma once

#include <string>
#include <vector>

#include "../global/Id.h"
#include "../util/File.h"
#include "../util/MmapVector.h"
#include "StringSortComparator.h"

using std::string;
using std::vector;

struct IdAndOffset {
  uint64_t _id;
  uint64_t _offset;

  // Compare only by the ids, since they are unique
  auto operator<=>(const IdAndOffset& rhs) const {
    return _id <=> rhs._id;
  }
};

struct OffsetAndSize {
  uint64_t _offset;
  uint64_t _size;
};

//! On-disk vocabulary. Very small (O(1)) memory consumption.
//! Layout: <term1><term2>..<termn><offsets><startOfOffsets>
//! <offsets> are n off_t where the i's off_t specifies the position
//! of term i in the file.
//! To obtain item i, read two offsets from
//! startofOffsets + i * sizeof(off_t) and then read the strings in between.
//! To obtain an ID for a term, do a binary search where each random access
//! uses the steps described above.
template <class StringComparator>
class ExternalVocabulary {
 public:
  void buildFromVector(const vector<string>& v, const string& fileName);
  void buildFromTextFile(const string& textFileName, const string& outFileName);

  void initFromFile(const string& file);

  // close the underlying file and uninitialize this vocabulary for further use
  void clear() { _file.close(); }

  //! Get the word with the given id
  //! (as non-reference, returning a cost ref is not possible, because the
  //! string does not necessarily already exist in memory - unlike for an
  //! internal vocabulary)
  std::optional<string> operator[](Id id) const;

  //! Get the number of words in the vocabulary.
  size_t size() const { return idsAndOffsets().size() - 1; }

  //! Get an Id from the vocabulary for some "normal" word.
  //! Return value signals if something was found at all.
  bool getId(const string& word, Id* id) const {
    *id = binarySearchInVocab(word);
    return *id < size() && (*this)[*id] == word;
  }

  StringComparator& getCaseComparator() { return _caseComparator; }

  ExternalVocabulary() = default;
  ExternalVocabulary(ExternalVocabulary&&) noexcept = default;
  ExternalVocabulary& operator=(ExternalVocabulary&&) noexcept = default;

 private:
  mutable ad_utility::File _file;
  ad_utility::MmapVectorView<IdAndOffset> _idsAndOffsets;

  const auto& idsAndOffsets() const {return _idsAndOffsets;}

  StringComparator _caseComparator;

  Id binarySearchInVocab(const string& word) const;

  std::optional<OffsetAndSize> getOffsetAndSize(Id id) const;

  template<class Iterable>
  void buildFromIterable(Iterable&& iterable, const string& filename);

  inline static const std::string _offsetSuffix = ".idsAndOffsets.mmap";
};
