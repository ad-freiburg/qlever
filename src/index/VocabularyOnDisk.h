// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Johannes Kalmbach <johannes.kalmbach@gmail.com>

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

// On-disk vocabulary of strings. Each entry is a pair of <ID, String>. The IDs
// are ascending, but not (necessarily) contiguous. If the strings are sorted,
// then binary search for a string can be performed.
class VocabularyOnDisk {
 private:
  // An ID and the offset of the corresponding word in the underlying file.
  struct IdAndOffset {
    uint64_t _id;
    uint64_t _offset;
    // Compare only by the IDs.
    auto operator<=>(const IdAndOffset& rhs) const { return _id <=> rhs._id; }
  };

  // The file in which the words are stored.
  mutable ad_utility::File _file;

  // The IDs and offsets of the words.
  ad_utility::MmapVectorView<IdAndOffset> _idsAndOffsets;

  // The highest ID that occurs in the vocabulary. If the vocabulary is empty,
  // this will be Id(-1), s.t. _highestId + 1 will overflow to 0.
  Id _highestId = std::numeric_limits<Id>::max();
  // The number of words stored in the vocabulary.
  size_t _size = 0;

  // This suffix is appended to the filename of the main file, in order to get
  // the name for the file in which IDs and offsets are stored.
  static constexpr std::string_view _offsetSuffix = ".idsAndOffsets.mmap";

 public:
  /// Build from a vector of strings, or from a textFile with one word per line.
  /// These functions will assign the contiguous Ids [0 .. #numWords).
  void buildFromVector(const vector<string>& words, const string& fileName);
  void buildFromTextFile(const string& textFileName, const string& outFileName);

  /// Build from a vector of pairs of `(string, id)`. This requires the IDs to
  /// be contiguous.
  void buildFromStringsAndIds(
      const vector<std::pair<std::string, uint64_t>>& wordsAndIds,
      const string& fileName);

  /// Open the vocabulary from file. It must have been previously written to
  /// this file, for example via `buildFromVector` or `buildFromTextFile`.
  void open(const string& filename);

  /// Close the underlying file and uninitialize this vocabulary for further
  /// use.
  void close() { _file.close(); }

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

  // Get the largest ID contained in this vocabulary. If the vocabulary is
  // empty, this is the highest possible ID (s.t. `getHighestId() + 1` overflows
  // to 0). This makes the behavior of `lower_bound` and `upper_bound` for empty
  // vocabularies consistent with other vocabulary types like
  // `VocabularyInMemory`.
  Id getHighestId() const { return _highestId; }

  /// Return a `WordAndIndex` that points to the first entry that is equal or
  /// greater than `word` wrt the `comparator`. Only works correctly if the
  /// vocabulary is sorted according to the comparator (exactly like in
  /// `std::lower_bound`, which is used internally).
  template <typename Comparator>
  WordAndIndex lower_bound(const auto& word, Comparator comparator) const {
    auto it =
        std::lower_bound(begin(), end(), word, transformComparator(comparator));
    return iteratorToWordAndIndex(it);
  }

  /// Return a `WordAndIndex` that points to the first entry that is greater
  /// than `word` wrt the `comparator`. Only works correctly if the
  /// vocabulary is sorted according to the comparator (exactly like in
  /// `std::upper_bound`, which is used internally).
  template <typename Comparator>
  WordAndIndex upper_bound(const auto& word, Comparator comparator) const {
    auto it =
        std::upper_bound(begin(), end(), word, transformComparator(comparator));
    return iteratorToWordAndIndex(it);
  }

  // The offset of a word in `_file`, its size in number of bytes and its ID
  struct OffsetSizeId {
    uint64_t _offset;
    uint64_t _size;
    uint64_t _id;
  };

  // The offset of a word in `_file` and its size in number of bytes.
  struct OffsetAndSize {
    uint64_t _offset;
    uint64_t _size;
    OffsetAndSize(OffsetSizeId osi) : _offset{osi._offset}, _size{osi._size} {}
    OffsetAndSize() = default;
  };

 private:
  // Return the `i`-th element from this vocabulary. Note that this is (in
  // general) NOT the element with the ID `i`, because the ID space is not
  // contiguous.
  WordAndIndex getIthElement(size_t n) const;

  // Helper function for implementing a random access iterator.
  using Accessor = decltype([](const auto& vocabulary, uint64_t index) {
    return vocabulary.getIthElement(index);
  });

  // Const random access iterators, implemented via the
  // `IteratorForAccessOperator` template.
  using const_iterator =
      ad_utility::IteratorForAccessOperator<VocabularyOnDisk, Accessor>;
  const_iterator begin() const { return {this, 0}; }
  const_iterator end() const { return {this, size()}; }

  // Takes a lambda that compares two string-like objects and returns a lambda
  // that compares two objects, either of which can be string-like or
  // `WordAndIndex`.
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

    return [comparator, getString](const auto& a, const auto& b) {
      return comparator(getString(a), getString(b));
    };
  }

  // Convert an iterator to the corresponding `WordAndIndex`. For the `end()`
  // iterator return `{nullopt, highestID + 1}`.
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

  // Return the `OffsetSizeId` for the element with the i-th smallest ID.
  // Requires that i < size().
  OffsetSizeId getOffsetSizeIdForIthElement(uint64_t i) const;

  // Return the `OffsetAndSize` for the element with the i-th smallest ID.
  // Requires that i < size().
  OffsetAndSize getOffsetAndSizeForIthElement(uint64_t i) const {
    return getOffsetSizeIdForIthElement(i);
  }

  // Build a vocabulary from any type that is forward-iterable and yields
  // pairs of (string-like, ID). Used as the common implementation for
  // the `buildFrom...` methods.
  template <class Iterable>
  void buildFromIterable(Iterable&& iterable, const string& filename);
};
