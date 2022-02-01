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
#include "../util/Iterators.h"

using std::string;
using std::vector;

struct IdAndOffset {
  uint64_t _id;
  uint64_t _offset;

  // Compare only by the ids, since they are unique
  auto operator<=>(const IdAndOffset& rhs) const { return _id <=> rhs._id; }
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
  struct IdAndString {
    Id _id;
    std::string _string;
  };
  void buildFromVector(const vector<string>& v, const string& fileName);
  void buildFromTextFile(const string& textFileName, const string& outFileName);

  void initFromFile(const string& file);

  // close the underlying file and uninitialize this vocabulary for further use
  void clear() { _file.close(); }

  // If an entry with the Id set to `id` exists, the return
  // the corresponding string.
  std::optional<string> idToOptionalString(Id id) const;

  IdAndString getNthElement(size_t n) const;

  //! Get the number of words in the vocabulary.
  size_t size() const { return idsAndOffsets().size() - 1; }

  //! Get an Id from the vocabulary for some "normal" word.
  //! Return value signals if something was found at all.
  bool getId(const string& word, Id* id) const {
    *id = binarySearchInVocab(word);
    return *id < size() && idToOptionalString(*id) == word;
  }

  StringComparator& getCaseComparator() { return _caseComparator; }

  ExternalVocabulary() = default;
  ExternalVocabulary(ExternalVocabulary&&) noexcept = default;
  ExternalVocabulary& operator=(ExternalVocabulary&&) noexcept = default;

  using Accessor = decltype([](auto&& vocabulary, auto index) {
    return vocabulary.getNthElement(index);
  });

  using const_iterator = ad_utility::IteratorForAccessOperator<ExternalVocabulary, Accessor>;

  const_iterator begin() const {
    return {this, 0};
  }

  const_iterator end() const {
    return {this, size()};
  }

 private:
  mutable ad_utility::File _file;
  ad_utility::MmapVectorView<IdAndOffset> _idsAndOffsets;

  const auto& idsAndOffsets() const { return _idsAndOffsets; }

  StringComparator _caseComparator;

  Id binarySearchInVocab(const string& word) const;

  std::optional<OffsetAndSize> getOffsetAndSize(Id id) const;

  template <class Iterable>
  void buildFromIterable(Iterable&& iterable, const string& filename);

  inline static const std::string _offsetSuffix = ".idsAndOffsets.mmap";
};
