// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#pragma once

#include <string>
#include <vector>

#include "global/Id.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "util/File.h"
#include "util/Iterators.h"
#include "util/MmapVector.h"

// On-disk vocabulary of strings. Each entry is a pair of <ID, String>. The IDs
// are ascending, but not (necessarily) contiguous. If the strings are sorted,
// then binary search for a string can be performed.
class VocabularyOnDisk {
 private:
  // An ID and the offset of the corresponding word in the underlying file.
  struct IndexAndOffset {
    uint64_t idx_;
    uint64_t offset_;
    // Compare only by the IDs.
    auto operator<=>(const IndexAndOffset& rhs) const {
      return idx_ <=> rhs.idx_;
    }
    // Equality comparison is currently unused, but it must be declared to be
    // able to use `std::ranges::lower_bound` etc.
    bool operator==(const IndexAndOffset& rhs) const;
  };

  // The file in which the words are stored.
  mutable ad_utility::File file_;

  // The IDs and offsets of the words.
  ad_utility::MmapVectorView<IndexAndOffset> idsAndOffsets_;

  // The highest ID that occurs in the vocabulary. If the vocabulary is empty,
  // this will be Id(-1), s.t. highestIdx_ + 1 will overflow to 0.
  static constexpr uint64_t highestIndexEmpty_ =
      std::numeric_limits<uint64_t>::max();
  uint64_t highestIdx_ = highestIndexEmpty_;
  // The number of words stored in the vocabulary.
  size_t size_ = 0;

  // This suffix is appended to the filename of the main file, in order to get
  // the name for the file in which IDs and offsets are stored.
  static constexpr std::string_view offsetSuffix_ = ".idsAndOffsets.mmap";

 public:
  // A helper class that is used to build a vocabulary word by word.
  // Each call to `operator()` adds the next word to the vocabulary.
  // At the end, the `finish()` method can be called. Note that `finish`
  // is also implicitly called by the destructor, but doing so implicitly
  // releases resources earlier and is cleaner in case of exceptions.
  class WordWriter {
   private:
    ad_utility::File file_;
    ad_utility::MmapVector<IndexAndOffset> idsAndOffsets_;
    uint64_t currentOffset_ = 0;
    std::optional<uint64_t> previousId_ = std::nullopt;
    uint64_t currentIndex_ = 0;
    bool isFinished_ = false;
    ad_utility::ThrowInDestructorIfSafe throwInDestructorIfSafe_;

   public:
    // Constructor, used by `VocabularyOnDisk::wordWriter`.
    explicit WordWriter(const std::string& filename);
    // Add the next word to the vocabulary.
    void operator()(std::string_view word);
    // Finish the writing. After this no more calls to `operator()` are allowed.
    void finish();
    // Destructor. Implicitly calls `finish` if it hasn't been called before.
    ~WordWriter();
  };

  /// Build from a vector of pairs of `(string, id)`. This requires the IDs to
  /// be contiguous.
  void buildFromStringsAndIds(
      const std::vector<std::pair<std::string, uint64_t>>& wordsAndIds,
      const std::string& fileName);

  /// Open the vocabulary from file. It must have been previously written to
  /// this file, for example via `buildFromVector` or `buildFromTextFile`.
  void open(const std::string& filename);

  /// If an entry with this `idx` exists, return the corresponding string, else
  /// `std::nullopt`
  std::optional<std::string> operator[](uint64_t idx) const;

  /// Get the number of words in the vocabulary.
  size_t size() const { return size_; }

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
  uint64_t getHighestId() const { return highestIdx_; }

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

  // The offset of a word in `file_`, its size in number of bytes and its ID
  struct OffsetSizeId {
    uint64_t _offset;
    uint64_t _size;
    uint64_t _id;
  };

  // The offset of a word in `file_` and its size in number of bytes.
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
        AD_CONTRACT_CHECK(input._word.has_value());
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

  // Get the `OffsetAndSize` for the element with the `idx`. Return
  // `std::nullopt` if `idx` is not contained in the vocabulary.
  std::optional<OffsetAndSize> getOffsetAndSize(uint64_t idx) const;

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
