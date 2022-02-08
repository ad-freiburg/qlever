// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Björn Buchhold <buchholb>
//          Johannes Kalmbach <johannes.kalmbach@gmail.com>

#pragma once

#include <string>
#include <vector>

#include "../global/Id.h"
#include "../util/File.h"
#include "../util/Iterators.h"
#include "../util/MmapVector.h"
#include "StringSortComparator.h"

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

// On-disk vocabulary of strings.
// Each entry is a pair of <Id, String>. The Ids are ascending, but not (necessarily) contiguous. If the strings are also sorted, then binary search for strings can be performed.
// Currently this class is coupled with a `StringSortComparator` that performs comparisons according to the Unicode standard.
// TODO<joka921> As soon as we have merged the modular vocabulary, the comparator can be moved out of this class.
template <class StringComparator>
class ExternalVocabulary {
 public:
  struct WordAndId {
    std::optional<std::string> _word;
    Id _id;
  };

  // Build from a vector of strings, or from a textFile with one word per line.
  // In both cases the strings have to be sorted wrt the `StringComparator`.
  // These functions will assign the contiguous Ids [0 .. #numWords).
  void buildFromVector(const vector<string>& v, const string& fileName);
  void buildFromTextFile(const string& textFileName, const string& outFileName);

  // Initialize from a file. The vocabulary must have been previously written to this file, for example via `buildFromVector` or `buildFromTextFile`
  void initFromFile(const string& file);

  // Close the underlying file and uninitialize this vocabulary for further use
  void close() { _file.close(); }

  // If an entry with the Id set to `id` exists, the return
  // the corresponding string, else `std::nullopt`
  std::optional<string> idToOptionalString(Id id) const;

  // Return the `n-th` element from this vocabulary. Note that this is (in general) NOT the element with the ID `n`, because the ID space is not contiguous.
  WordAndId getNthElement(size_t n) const;

  //! Get the number of words in the vocabulary.
  size_t size() const { return _size; }

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

  using const_iterator =
      ad_utility::IteratorForAccessOperator<ExternalVocabulary, Accessor>;

  const_iterator begin() const { return {this, 0}; }

  const_iterator end() const { return {this, size()}; }

  // Get the id that is the largest id contained in this external vocabulary + 1
  Id getUpperBoundForIds() const { return _highestId + 1; }

  using SortLevel = typename StringComparator::Level;

  // __________________________________________________________________________
  WordAndId upper_bound(const auto& word, const SortLevel level) const {
    auto it = std::upper_bound(begin(), end(), word,
                               getComparatorForSortLevel(level));
    if (it == end()) {
      return {getUpperBoundForIds(), std::nullopt};
    } else {
      return *it;
    }
  }

  // _________________________________________________________________________
  WordAndId lower_bound(const auto& word, const SortLevel level) const {
    auto it = std::lower_bound(begin(), end(), word,
                               getComparatorForSortLevel(level));
    if (it == end()) {
      return {getUpperBoundForIds(), std::nullopt};
    } else {
      return *it;
    }
  }

 private:
  mutable ad_utility::File _file;
  ad_utility::MmapVectorView<IdAndOffset> _idsAndOffsets;

  const auto& idsAndOffsets() const { return _idsAndOffsets; }

  StringComparator _caseComparator;
  Id _highestId = 0;
  size_t _size = 0;

  auto getComparatorForSortLevel(const SortLevel level) const {
    auto getString = [&](const auto& input) {
      if constexpr (ad_utility::isSimilar<decltype(input), WordAndId>) {
        AD_CHECK(input._word.has_value());
        return input._word.value();
      } else {
        return input;
      }
    };
    return [this, level, getString](auto&& a, auto&& b) {
      return this->_caseComparator(getString(a), getString(b), level);
    };
  }

  Id binarySearchInVocab(const string& word) const;

  std::optional<OffsetAndSize> getOffsetAndSize(Id id) const;
  OffsetAndSize getOffsetAndSizeForNthElement(Id id) const;

  template <class Iterable>
  void buildFromIterable(Iterable&& iterable, const string& filename);

  inline static const std::string _offsetSuffix = ".idsAndOffsets.mmap";
};