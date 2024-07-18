// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (kalmbach@cs.uni-freiburg.de)

#pragma once

#include <ranges>
#include <string>
#include <string_view>

#include "index/VocabularyOnDisk.h"
#include "index/vocabulary/VocabularyInMemoryBinSearch.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "util/Exception.h"

// A vocabulary that stores all the words on disk. Additionally, some of the
// words can be stored in RAM. The words that are stored in RAM can be accessed
// much faster, and additionally serve to make binary searches on the words that
// are stored on disk faster. When storing the vocabulary, the user can manually
// specify for each word whether it shall be cached in RAM. Additionally, every
// k-th word (default 1000) is stored in RAM.
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

  // Direct const access to the underlying vocabularies.
  const auto& internalVocab() const { return internalVocab_; }
  const auto& externalVocab() const { return externalVocab_; }

  // Read the vocabulary from a file. The file must have been created using a
  // `WordWriter`.
  void open(const string& filename) {
    internalVocab_.open(filename + ".internal");
    externalVocab_.open(filename + ".external");
  }

  // Return the total number of words
  [[nodiscard]] size_t size() const { return externalVocab_.size(); }

  /// Return the highest ID (= index) that occurs in this vocabulary. May only
  /// becalled if size() > 0.
  [[nodiscard]] uint64_t getHighestId() const {
    return externalVocab_.getHighestId();
  }

  /// Return the `i-th` word. The behavior is undefined if `i >= size()`
  std::string operator[](uint64_t i) const;

  /// Return a `WordAndIndex` that points to the first entry that is equal or
  /// greater than `word` wrt. to the `comparator`. Only works correctly if the
  /// `words_` are sorted according to the comparator (exactly like in
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
  /// than `word` wrt. to the `comparator`. Only works correctly if the `words_`
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
  /// word-by-word, without having to materialize it in RAM first.
  struct WordWriter {
    VocabularyInMemoryBinSearch::WordWriter internalWriter_;
    VocabularyOnDisk::WordWriter externalWriter_;
    uint64_t idx_ = 0;
    size_t milestoneDistance_;
    size_t sinceMilestone = 0;

    // Construct from the `filename` to which the vocabulary will be serialized.
    // At least every `milestoneDistance`-th word will be cached in RAM.
    explicit WordWriter(const std::string& filename,
                        size_t milestoneDistance = 1'000);

    // Add the next word. If `isExternal` is true, then the word will only be
    // stored on disk, and not be cached in RAM.
    void operator()(std::string_view word, bool isExternal = true);

    // Finish writing.
    void finish();
  };

  // Return a `WordWriter` that directly writes the words to the given
  // `filename`. The words are not materialized in RAM, but the vocabulary later
  // has to be explicitly initialized via `open(filename)`.
  WordWriter makeDiskWriter(const std::string& filename) const {
    return WordWriter{filename};
  }

  /// Clear the vocabulary.
  void close() { internalVocab_.close(); }

  /// Initialize the vocabulary from the given `words`.
  void build(const std::vector<std::string>& words,
             const std::string& filename);

  // Const access to the underlying words.
  auto begin() const { return externalVocab_.begin(); }
  auto end() const { return externalVocab_.end(); }

  // Convert an iterator (which can be an iterator to the external or internal
  // vocabulary) into the corresponding index by (logically) subtracting
  // `begin()`.
  uint64_t iteratorToIndex(std::ranges::iterator_t<VocabularyOnDisk> it) const {
    return it - begin();
  }
  uint64_t iteratorToIndex(
      std::ranges::iterator_t<VocabularyInMemoryBinSearch> it) const {
    return internalVocab_.indices().at(it - internalVocab_.begin());
  }

 private:
  // The common implementation of `lower_bound`, `upper_bound`,
  // `lower_bound_iterator`, and `upper_bound_iterator`.
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
};
