// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#pragma once

#include <string>
#include <string_view>

#include "index/VocabularyOnDisk.h"
#include "index/vocabulary/VocabularyInMemoryBinSearch.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "util/Exception.h"

//! A vocabulary. Wraps a `CompactVectorOfStrings<char>`
//! and provides additional methods for reading and writing to/from file,
//! and retrieval via binary search.
class VocabularyInternalExternal {
 private:
  // The actual storage.
  VocabularyInMemoryBinSearch internalVocab_;
  VocabularyOnDisk externalVocab_;

 public:
  /// Construct an empty vocabulary
  VocabularyInternalExternal() = default;

  // Vocabularies are movable
  VocabularyInternalExternal& operator=(VocabularyInternalExternal&&) noexcept =
      default;
  VocabularyInternalExternal(VocabularyInternalExternal&&) noexcept = default;

  /// Read the vocabulary from a file. The file must have been created by a call
  /// to `writeToFile` or using a `WordWriter`.
  void open(const string& filename) {
    internalVocab_.open(filename + ".internal");
    externalVocab_.open(filename + ".external");
  }

  /// Write the vocabulary to a file.
  void writeToFile([[maybe_unused]] const string& fileName) {
    // TODO<joka921> Do we need this?
    AD_FAIL();
  }

  /// Return the total number of words
  [[nodiscard]] size_t size() const { return externalVocab_.size(); }

  /// Return the highest ID (= index) that occurs in this vocabulary. May only
  /// becalled if size() > 0.
  [[nodiscard]] uint64_t getHighestId() const {
    return externalVocab_.getHighestId();
  }

  /// Return the `i-th` word. The behavior is undefined if `i >= size()`
  std::string operator[](uint64_t i) const {
    auto fromInternal = internalVocab_[i];
    if (fromInternal.has_value()) {
      return std::string{fromInternal.value()};
    }
    return externalVocab_[i];
  }

  /// Return a `WordAndIndex` that points to the first entry that is equal or
  /// greater than `word` wrt. to the `comparator`. Only works correctly if the
  /// `_words` are sorted according to the comparator (exactly like in
  /// `std::lower_bound`, which is used internally).
  template <typename InternalStringType, typename Comparator>
  WordAndIndex boundImpl(const InternalStringType& word, Comparator comparator,
                         auto boundFunction) const {
    WordAndIndex lowerBoundInternal =
        boundFunction(internalVocab_, word, comparator);

    //  The external vocabulary might have slightly different bounds.
    return boundFunction(externalVocab_, word, comparator,
                         lowerBoundInternal._previousIndex,
                         lowerBoundInternal._nextIndex);
  }

  /// Return a `WordAndIndex` that points to the first entry that is equal or
  /// greater than `word` wrt. to the `comparator`. Only works correctly if the
  /// `_words` are sorted according to the comparator (exactly like in
  /// `std::lower_bound`, which is used internally).
  template <typename InternalStringType, typename Comparator>
  WordAndIndex lower_bound(const InternalStringType& word,
                           Comparator comparator) const {
    return boundImpl(word, comparator, [](const auto& vocab, auto&&... args) {
      return vocab.lower_bound(AD_FWD(args)...);
    });
  }

  // Same as `lower_bound`, but compares an `iterator` and a `value` instead of
  // two values. Required by the `CompressedVocabulary`.
  template <typename InternalStringType, typename Comparator>
  WordAndIndex lower_bound_iterator(const InternalStringType& word,
                                    Comparator comparator) const {
    return boundImpl(word, comparator, [](const auto& vocab, auto&&... args) {
      return vocab.lower_bound_iterator(AD_FWD(args)...);
    });
  }

  /// Return a `WordAndIndex` that points to the first entry that is greater
  /// than `word` wrt. to the `comparator`. Only works correctly if the `_words`
  /// are sorted according to the comparator (exactly like in
  /// `std::upper_bound`, which is used internally).
  template <typename InternalStringType, typename Comparator>
  WordAndIndex upper_bound(const InternalStringType& word,
                           Comparator comparator) const {
    return boundImpl(word, comparator, [](const auto& vocab, auto&&... args) {
      return vocab.upper_bound(AD_FWD(args)...);
    });
  }

  // Same as `upper_bound`, but compares a `value` and an `iterator` instead of
  // two values. Required by the `CompressedVocabulary`.
  template <typename InternalStringType, typename Comparator>
  WordAndIndex upper_bound_iterator(const InternalStringType& word,
                                    Comparator comparator) const {
    return boundImpl(word, comparator, [](const auto& vocab, auto&&... args) {
      return vocab.upper_bound_iterator(AD_FWD(args)...);
    });
  }

  /// A helper type that can be used to directly write a vocabulary to disk
  /// word-by-word, without having to materialize it in RAM first. See the
  /// documentation of `CompactVectorOfStrings` for details.
  struct WordWriter {
    VocabularyInMemoryBinSearch::WordWriter internalWriter_;
    VocabularyOnDisk::WordWriter externalWriter_;
    uint64_t idx_ = 0;
    size_t milestoneDistance_;
    size_t sinceMilestone = 0;
    explicit WordWriter(const std::string& filename,
                        size_t milestoneDistance = 1'000)
        : internalWriter_{filename + ".internal"},
          externalWriter_{filename + ".external"},
          milestoneDistance_{milestoneDistance} {}
    // TODO<joka921> Get rid of the default argument...
    void operator()(std::string_view str, bool isExternal = true) {
      externalWriter_(str);
      if (!isExternal || sinceMilestone >= milestoneDistance_) {
        internalWriter_(str, idx_);
        sinceMilestone = 0;
      }
      ++idx_;
      ++sinceMilestone;
    }

    void finish() {
      internalWriter_.finish();
      externalWriter_.finish();
    }
  };

  // Return a `WordWriter` that directly writes the words to the given
  // `filename`. The words are not materialized in RAM, but the vocabulary later
  // has to be explicitly initizlied via `open(filename)`.
  WordWriter makeDiskWriter(const std::string& filename) const {
    return WordWriter{filename};
  }

  /// Clear the vocabulary.
  void close() { internalVocab_.close(); }

  /// Initialize the vocabulary from the given `words`.
  void build(const std::vector<std::string>& words,
             const std::string& filename) {
    WordWriter writer = makeDiskWriter(filename);
    for (const auto& word : words) {
      writer(word, false);
    }
    writer.finish();
    open(filename);
  }

  // Const access to the underlying words.
  auto begin() const { return externalVocab_.begin(); }
  auto end() const { return externalVocab_.end(); }

  auto iteratorToIndex(auto it) const {
    if constexpr (requires() { it - begin(); }) {
      return it - begin();
    } else {
      return internalVocab_.offsets().at(it - internalVocab_.begin());
    }
  }
};
