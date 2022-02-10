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
#include "./vocabulary/VocabularyTypes.h"
#include "StringSortComparator.h"

using std::string;
using std::vector;

// On-disk vocabulary of strings.
// Each entry is a pair of <Id, String>. The Ids are ascending, but not
// (necessarily) contiguous. If the strings are also sorted, then binary search
// for strings can be performed.
class VocabularyOnDisk {
 private:
  // An Id and the offset of the corresponding word in the underlying file.
  struct IdAndOffset {
    uint64_t _id;
    uint64_t _offset;
    // Compare only by the IDs, since they are unique
    auto operator<=>(const IdAndOffset& rhs) const { return _id <=> rhs._id; }
  };

  // The file in which the words are stored.
  mutable ad_utility::File _file;

  // The IdsAndOffsets
  ad_utility::MmapVectorView<IdAndOffset> _idsAndOffsets;

  // The highest ID that occurs in the vocabulary. Only valid if _size > 0;
  Id _highestId = 0;
  // The number of words stored in the vocabulary
  size_t _size = 0;

  // This suffix is appended to the "actual" filename to get the name for the
  // file where the ids and offsets are stored.
  static constexpr std::string_view _offsetSuffix = ".idsAndOffsets.mmap";

 public:
  /// Build from a vector of strings, or from a textFile with one word per line.
  /// In both cases the strings have to be sorted wrt the `StringComparator`.
  /// These functions will assign the contiguous Ids [0 .. #numWords).
  void buildFromVector(const vector<string>& v, const string& fileName);
  void buildFromTextFile(const string& textFileName, const string& outFileName);

  /// Initialize from a file. The vocabulary must have been previously written
  /// to this file, for example via `buildFromVector` or `buildFromTextFile`
  void readFromFile(const string& file);

  /// Close the underlying file and uninitialize this vocabulary for further
  /// use. NOTE: The name "clear" is a little misleading, but it is required to
  /// make the interface consistent with the other vocabulary types.
  void clear() { _file.close(); }

  /// If an entry with this `id` exists, return the corresponding string, else
  /// `std::nullopt`
  std::optional<string> operator[](Id id) const;

  /// Get the number of words in the vocabulary.
  size_t size() const { return _size; }

  /// Default constructor for an empty vocabulary.
  VocabularyOnDisk() = default;

  /// `VocabularyOnDisk` is movable, but not copyable.
  VocabularyOnDisk(VocabularyOnDisk&&) noexcept = default;
  VocabularyOnDisk& operator=(VocabularyOnDisk&&) noexcept = default;

  // Get the largest ID contained in this vocabulary. May only be called if
  // size() > 0;
  Id getHighestId() const {
    AD_CHECK(size() > 0);
    return _highestId;
  }

  // __________________________________________________________________________
  template <typename Comparator>
  WordAndIndex upper_bound(const auto& word, Comparator comparator) const {
    auto it =
        std::upper_bound(begin(), end(), word, transformComparator(comparator));
    return iteratorToWordAndIndex(it);
  }

  // __________________________________________________________________________
  template <typename Comparator>
  WordAndIndex lower_bound(const auto& word, Comparator comparator) const {
    auto it =
        std::lower_bound(begin(), end(), word, transformComparator(comparator));
    return iteratorToWordAndIndex(it);
  }

  // The offset of a word in the _file, as well as its size in number of bytes.
  struct OffsetAndSize {
    uint64_t _offset;
    uint64_t _size;
  };

 private:
  // Return the `n-th` element from this vocabulary. Note that this is (in
  // general) NOT the element with the ID `n`, because the ID space is not
  // contiguous.
  WordAndIndex getNthElement(size_t n) const;

  // Helper function for implementing a random access iterator iterator.
  [[maybe_unused]] const static inline auto accessor = [](auto&& vocabulary,
                                                          auto index) {
    return vocabulary.getNthElement(index);
  };

  // Const random access iterators, implemented via the
  // `IteratorForAccessOperator` template
  using const_iterator =
      ad_utility::IteratorForAccessOperator<VocabularyOnDisk,
                                            decltype(accessor)>;
  const_iterator begin() const { return {this, 0}; }
  const_iterator end() const { return {this, size()}; }

  // Takes a lambda that compares to string-like types and returns a lambda that
  // compares string-like types with `WordAndIndex`.
  auto transformComparator(auto comparator) const {
    // For a `WordAndIndex`, return the word, else (for string types) just
    // return the input.
    auto getString = [&](const auto& input) {
      if constexpr (ad_utility::isSimilar<decltype(input), WordAndIndex>) {
        AD_CHECK(input._word.has_value());
        return input._word.value();
      } else {
        return input;
      }
    };

    return [comparator, getString](auto&& a, auto&& b) {
      return comparator(getString(a), getString(b));
    };
  }

  // Convert and iterator to the corresponding `WordAndIndex` by also correctly
  // handling the `end()` iterator which cannot directly be dereferenced.
  [[nodiscard]] WordAndIndex iteratorToWordAndIndex(const_iterator it) const {
    if (it == end()) {
      return {std::nullopt, getHighestId() + 1};
    } else {
      return *it;
    }
  }

  // Get the `OffsetAndSize` for the element with the `id`. Return
  // `std::nullopt` if `id` is not contained in the vocabulary.
  std::optional<OffsetAndSize> getOffsetAndSize(Id id) const;

  // Return the `OffsetAndSize` for the element with the n-th smalles ID.
  // Requires that `n < size()`
  OffsetAndSize getOffsetAndSizeForNthElement(uint64_t n) const;

  // Build a vocabulary from any type that is forward-iterable and yields
  // strings or string_views. Used as the common implementation for
  // `buildFromVector` and `buildFromTextFile`.
  template <class Iterable>
  void buildFromIterable(Iterable&& iterable, const string& filename);
};