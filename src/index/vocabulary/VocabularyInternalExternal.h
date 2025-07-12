// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (kalmbach@cs.uni-freiburg.de)

#ifndef QLEVER_SRC_INDEX_VOCABULARY_VOCABULARYINTERNALEXTERNAL_H
#define QLEVER_SRC_INDEX_VOCABULARY_VOCABULARYINTERNALEXTERNAL_H

#include <ranges>
#include <string>
#include <string_view>

#include "index/vocabulary/VocabularyInMemoryBinSearch.h"
#include "index/vocabulary/VocabularyOnDisk.h"
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
  void open(const std::string& filename);

  // Return the total number of words
  [[nodiscard]] size_t size() const { return externalVocab_.size(); }

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
  struct WordWriter : public WordWriterBase {
    VocabularyInMemoryBinSearch::WordWriter internalWriter_;
    VocabularyOnDisk::WordWriter externalWriter_;
    uint64_t idx_ = 0;
    size_t milestoneDistance_;
    size_t sinceMilestone_ = 0;

    // Construct from the `filename` to which the vocabulary will be serialized.
    // At least every `milestoneDistance`-th word will be cached in RAM.
    explicit WordWriter(const std::string& filename,
                        size_t milestoneDistance = 1'000);

    // Add the next word. If `isExternal` is true, then the word will only be
    // stored on disk, and not be cached in RAM.
    uint64_t operator()(std::string_view word, bool isExternal) override;

    ~WordWriter() override;

    // Finish writing.
    void finishImpl() override;
  };

  // Return a `unique_ptr<WordWriter>` that writes to the given `filename`.
  static auto makeDiskWriterPtr(const std::string& filename) {
    return std::make_unique<WordWriter>(filename);
  }

  /// Clear the vocabulary.
  void close() { internalVocab_.close(); }

  // Convert an iterator (which can be an iterator to the external or internal
  // vocabulary) into the corresponding index by (logically) subtracting
  // `begin()`.
  uint64_t iteratorToIndex(ql::ranges::iterator_t<VocabularyOnDisk> it) const {
    return it - externalVocab_.begin();
  }
  uint64_t iteratorToIndex(
      ql::ranges::iterator_t<VocabularyInMemoryBinSearch> it) const {
    return internalVocab_.indices().at(it - internalVocab_.begin());
  }

 private:
  // The common implementation of `lower_bound`, `upper_bound`,
  // `lower_bound_iterator`, and `upper_bound_iterator`. The `boundFunction`
  // must be a lambda, that calls the corresponding function (e.g.
  // `lower_bound`) on its first argument (see above for usages).
  template <typename InternalStringType, typename Comparator,
            typename BoundFunction>
  WordAndIndex boundImpl(const InternalStringType& word, Comparator comparator,
                         BoundFunction boundFunction) const {
    // First do a binary search in the internal vocab.
    WordAndIndex boundFromInternalVocab =
        boundFunction(internalVocab_, word, comparator);

    // Then refine it in the external vocab.
    std::optional<uint64_t> upperBound = std::nullopt;
    if (!boundFromInternalVocab.isEnd()) {
      upperBound = boundFromInternalVocab.index() + 1;
    }
    return boundFunction(externalVocab_, word, comparator,
                         boundFromInternalVocab.previousIndex(), upperBound);
  }
};

#endif  // QLEVER_SRC_INDEX_VOCABULARY_VOCABULARYINTERNALEXTERNAL_H
