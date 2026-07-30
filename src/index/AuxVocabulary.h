// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_AUXVOCABULARY_H
#define QLEVER_SRC_INDEX_AUXVOCABULARY_H

#include <array>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "global/Id.h"
#include "global/ValueIdComparators.h"
#include "global/VocabIndexMarker.h"
#include "index/Vocabulary.h"
#include "index/vocabulary/VocabularyType.h"

// The vocabulary of an auxiliary index (see `index/AuxIndex.h`). It stores
// exactly those words that are used by the triples of that auxiliary index and
// that are not part of the vocabulary of the main index. The words are stored
// on disk, and the vocabulary uses the same `VocabularyType` as the main index,
// so in particular it splits its words over the same sub-vocabularies (see
// `SplitVocabulary` and `VocabIndexMarker`) as the main vocabulary and stores
// the same precomputed data (for example the `GeometryInfo` of WKT literals).
//
// The `Id`s of the words of an `AuxVocabulary` have their own `Datatype`
// (`Datatype::AuxVocabIndex`). Because the datatype bits are the most
// significant bits of an `Id`, all of these `Id`s are greater than all `Id`s of
// the main vocabulary when compared bitwise. This is what makes the auxiliary
// index mergeable into a scan of the main index (both are sorted bitwise), but
// it means that the bitwise order is not the order of the string values.
//
// For the semantically correct comparison, an `AuxVocabulary` additionally
// holds an in-RAM array that stores, for each of its words, the position at
// which that word would be sorted into the vocabulary of the main index (see
// `positionsInMainVocab()`). A semantic comparison of a word from the auxiliary
// vocabulary with a word from the main vocabulary therefore is a single random
// access into RAM, see `valueIdComparators::AuxVocabOrdering`.
//
// NOTE: That array is indexed by the *offset* of a word, not by its `Id`. The
// two differ when the vocabulary is split, because then the `Id`s of a
// sub-vocabulary with a marker greater than zero start at a large value (the
// marker sits in the highest bits of the payload). The offset is the position
// of a word in the concatenation of all sub-vocabularies in the order of their
// markers, so it is dense, and it is ascending in the (bitwise, and hence also
// semantic) order of the `Id`s. That is exactly what makes the array of
// positions ascending as well, which is required for the reverse lookup in
// `numWordsSmallerThanMainVocabPosition`.
class AuxVocabulary {
 public:
  // The vocabulary implementation. This is the same as `RdfsVocabulary` (in
  // particular it is a `PolymorphicVocabulary`, so the concrete implementation
  // can be chosen at runtime), only the index type differs.
  using Vocab = Vocabulary<detail::UnderlyingVocabRdfsVocabulary,
                           TripleComponentComparator, AuxVocabIndex>;
  using AccessReturnType = typename Vocab::AccessReturnType;

 private:
  Vocab vocabulary_;
  VocabIndexMarker marker_;
  // For each word of `vocabulary_`, indexed by the word's offset (see the class
  // comment), the raw bits of the `Id` of the first word of the main vocabulary
  // that is greater than that word. Note that all words of an `AuxVocabulary`
  // are absent from the main vocabulary, so there is no need to store an upper
  // bound as well. The entries of this array are ascending.
  std::vector<uint64_t> positionsInMainVocab_;
  // For each marker, the offset at which the words of the corresponding
  // sub-vocabulary start, that is, the exclusive prefix sums of the numbers of
  // words of the sub-vocabularies.
  std::array<uint64_t, VocabIndexMarker::maxNumMarkers> markerOffsets_{};

 public:
  // Return the file names of the auxiliary vocabulary with the given base name.
  // Note that which of them exist depends on the `VocabularyType`, see
  // `PolymorphicVocabulary`.
  static std::vector<std::string> fileNames(const std::string& basename);

  AuxVocabulary() = default;
  AuxVocabulary(const AuxVocabulary&) = delete;
  AuxVocabulary& operator=(const AuxVocabulary&) = delete;
  AuxVocabulary(AuxVocabulary&&) noexcept = default;
  AuxVocabulary& operator=(AuxVocabulary&&) noexcept = default;

  // Read the auxiliary vocabulary that was written to `basename` by a
  // `WordWriter` (see below). The `type` must be the same as the one that was
  // used for writing, and the locale must be the one of the main vocabulary,
  // else the order of the words is inconsistent with the main vocabulary.
  void open(const std::string& basename, ad_utility::VocabularyType type,
            const std::string& language, const std::string& country,
            bool ignorePunctuation);

  // The total number of words, summed over all sub-vocabularies. NOTE: This is
  // *not* an upper bound for the `Id`s of this vocabulary if it is split, see
  // the class comment.
  size_t numWords() const { return positionsInMainVocab_.size(); }

  // The word with the given index.
  AccessReturnType operator[](AuxVocabIndex index) const {
    return vocabulary_[index];
  }

  // The offset of the word with the given index, see the class comment.
  uint64_t offsetOf(AuxVocabIndex index) const {
    auto rawIndex = index.get();
    uint64_t offset = markerOffsets_.at(marker_.getMarker(rawIndex)) +
                      marker_.getIndexWithoutMarker(rawIndex);
    AD_CORRECTNESS_CHECK(offset < positionsInMainVocab_.size());
    return offset;
  }

  // The raw bits of the `Id` of the first word of the main vocabulary that is
  // greater than the word with the given `index`.
  uint64_t positionInMainVocab(AuxVocabIndex index) const {
    return positionsInMainVocab_[offsetOf(index)];
  }

  // The whole array of positions, see the class comment.
  ql::span<const uint64_t> positionsInMainVocab() const {
    return positionsInMainVocab_;
  }

  // The information that is required to compare `Id`s of type
  // `Datatype::AuxVocabIndex` semantically, that is, by the string values that
  // they represent.
  valueIdComparators::AuxVocabOrdering ordering() const {
    return valueIdComparators::AuxVocabOrdering{positionsInMainVocab_,
                                                markerOffsets_, marker_};
  }

  // Return the offset (see the class comment) of the first word of this
  // vocabulary that is not smaller than the word of the main vocabulary with
  // the given `Id`. This is the reverse direction of `positionInMainVocab` and
  // is possible in logarithmic time because the positions are ascending. It is
  // required to translate a bound in the main vocabulary into a bound in the
  // auxiliary vocabulary.
  uint64_t numWordsSmallerThanMainVocabPosition(Id mainVocabId) const {
    return ordering().numWordsSmallerThan(mainVocabId);
  }

  // Look up `word`. Return its index if it is contained in this vocabulary, and
  // `std::nullopt` otherwise.
  std::optional<AuxVocabIndex> getId(std::string_view word) const;

  // The position of `word` in this vocabulary. The `first` element is the index
  // of the first word that is greater than or equal to `word`; the `second`
  // element is `first + 1` if the word is contained, and equal to `first`
  // otherwise. Note that the lookup happens in the sub-vocabulary that `word`
  // belongs to, so both are marker-encoded. This mirrors
  // `Vocabulary::getPositionOfWord`.
  std::pair<AuxVocabIndex, AuxVocabIndex> getPositionOfWord(
      std::string_view word) const;

  // The offset (see the class comment) of the first word of this vocabulary
  // that is not smaller than `word`. This is the value that the semantic
  // comparison of a local vocab entry against a word of this vocabulary needs,
  // see `LocalVocabEntry::numSmallerAuxVocabWords`.
  uint64_t numWordsSmallerThan(std::string_view word) const;

  // Direct access to the underlying vocabulary, for example for the
  // `GeometryInfo` of a WKT literal.
  const Vocab& vocab() const { return vocabulary_; }

  // Writer for an `AuxVocabulary`. The words have to be pushed grouped by the
  // sub-vocabulary that they belong to, in ascending order of the markers of
  // those sub-vocabularies, and within each group in ascending order with
  // respect to the comparator of the main vocabulary at the `TOTAL` level. None
  // of the words may be contained in the main vocabulary. All of this is
  // checked.
  //
  // The writer computes the positions in the main vocabulary itself, via one
  // binary search per word.
  class WordWriter {
   private:
    const RdfsVocabulary& mainVocab_;
    std::string basename_;
    VocabIndexMarker marker_;
    std::unique_ptr<WordWriterBase> wordWriter_;
    std::vector<uint64_t> positionsInMainVocab_;
    std::array<uint64_t, VocabIndexMarker::maxNumMarkers> markerOffsets_{};
    uint8_t currentMarker_ = 0;
    std::optional<std::string> previousWord_;
    bool finishWasCalled_ = false;

   public:
    // Create a writer that writes the vocabulary of the given `type` to files
    // starting with `basename`. The `type` has to be the type of the main
    // vocabulary, such that the two split their words in the same way. The
    // `mainVocab` is required to compute the positions and to check the
    // precondition that no word of the auxiliary vocabulary is contained in the
    // main vocabulary.
    WordWriter(const RdfsVocabulary& mainVocab, const std::string& basename,
               ad_utility::VocabularyType type);
    ~WordWriter();
    WordWriter(const WordWriter&) = delete;
    WordWriter& operator=(const WordWriter&) = delete;

    // Add `word` and return the `Id` that was assigned to it.
    AuxVocabIndex operator()(std::string_view word);

    // Signal that the last word has been pushed and write the positions to
    // disk. Must be called exactly once.
    void finish();

    // The number of words that have been pushed so far.
    size_t numWords() const { return positionsInMainVocab_.size(); }
  };
};

#endif  // QLEVER_SRC_INDEX_AUXVOCABULARY_H
