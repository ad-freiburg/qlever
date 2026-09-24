// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_SECONDARYVOCABULARY_H
#define QLEVER_SRC_INDEX_VOCABULARY_SECONDARYVOCABULARY_H

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "global/IndexTypes.h"
#include "util/CompactStringVector.h"

// The secondary vocabulary of an index. It stores words that were added after
// the main index was built and that are not part of the vocabulary of that
// main index, so that data containing such words can be persisted and reloaded
// (in particular delta triples and materialized views, whose new words
// otherwise only exist as `LocalVocabEntry`s, that is, as pointers into the
// memory of one process). The `Id`s of its words have their own `Datatype`
// (`Datatype::SecondaryVocabIndex`), so all of them are greater than all `Id`s
// of the main vocabulary when compared bitwise, which is what makes those
// words mergeable into a scan of the main index.
//
// TERMINOLOGY. The words are kept in RAM as an append-only sequence of
// *segments* (see `appendSegment`), each of which holds its own words in
// sorted order (the concatenation of the segments, however, is *not* sorted).
// A word of this vocabulary is addressed in two different ways, which must not
// be conflated:
//
// * By its *global index*: the position of the word in the concatenation of
//   all segments, in the order in which those segments were appended. This is
//   the number that a `SecondaryVocabIndex` (and hence an `Id`) stores, so it
//   is what "the index of a word" means in this class unless stated otherwise.
//   It is not a position in any single segment: `operator[]` first has to map
//   it to the `[segmentIndex, indexWithinSegment]` pair that actually locates
//   the word, which it does via `segmentOffsets_`. For example, if the
//   segments `["<b>", "<d>"]` and `["<a>"]` were appended in that order, then
//   the global index of `<a>` is 2, and it is mapped to the pair `[1, 0]`.
// * By its *lexicographic rank*: the position of the word in the sorted
//   sequence of all words of all segments. This is a different number than the
//   global index; `sortedIndices_` maps a lexicographic rank to the
//   corresponding global index, and `getId` binary-searches in that array. In
//   the example above, `<a>` has lexicographic rank 0 but global index 2.
//
// The global index of a word never changes when further segments are appended,
// which is what allows persisted data (in particular the blobs of
// `NamedCachedQueryBlobManager`, in a follow-up change) to add words
// incrementally, one segment at a time, without invalidating the `Id`s of the
// earlier segments. Lexicographic ranks, in contrast, do change when a segment
// is appended.
//
// There is no *semantic* (that is, by string value) order among the words of
// this vocabulary; they compare by their `Id`s alone, which is why all `Id`s
// of this vocabulary compare greater than all `Id`s of the main vocabulary.
//
// NOTE: The lexicographic order used above is the cheap bytewise order of
// `std::string_view`, and this class deliberately takes no comparator. Looking
// a word up by its string does require *some* total order on the words, but
// this vocabulary currently supports (and needs) only exact lookup via
// `getId`, and no range queries, so any total order will do and the cheapest
// one is the right choice.
//
// TODO<joka921> The eventual implementation additionally stores, for each
// word, the position at which that word would be sorted into the main
// vocabulary, which a *semantic* comparison of an `Id` of this vocabulary with
// an `Id` of the main vocabulary needs; it follows in the changes that build
// on this one. That semantic order is a separate thing that does not have to
// replace the bytewise order used here; only once this class has to support
// range queries as well will a comparator have to be passed in.
class SecondaryVocabulary {
 private:
  // The words, stored as an append-only sequence of segments, each of which
  // holds its words in sorted order (see `appendSegment`).
  std::vector<CompactVectorOfStrings<char>> segments_;

  // For each segment, the global index of its first word, that is, the sum of
  // the sizes of all the segments that precede it. Has the same number of
  // elements as `segments_` and is sorted, so that `operator[]` can map a
  // global index to the corresponding `[segmentIndex, indexWithinSegment]`
  // pair by binary search.
  std::vector<uint64_t> segmentOffsets_;

  // The global indices of all words, ordered by the word that each of them
  // refers to; that is, `sortedIndices_[rank]` is the global index of the word
  // with lexicographic rank `rank`. For the example from the comment on this
  // class (the segments `["<b>", "<d>"]` and `["<a>"]`, appended in that
  // order), the global indices are `<b>` -> 0, `<d>` -> 1 and `<a>` -> 2, and
  // `sortedIndices_` is `[2, 0, 1]`: `sortedIndices_[0] == 2`, because the
  // lexicographically smallest word `<a>` has global index 2. `getId` binary-
  // searches in this array, and `appendSegment` merges the global indices of
  // the newly added words into it.
  std::vector<uint64_t> sortedIndices_;

 public:
  SecondaryVocabulary() = default;

  // Create a vocabulary that holds the given `words` as a single segment. The
  // `words` have to be sorted and pairwise distinct (which is checked, see
  // `appendSegment`), and none of them may be contained in the vocabulary of
  // the main index.
  explicit SecondaryVocabulary(std::vector<std::string> words);

  // Append `segment` as a new segment of this vocabulary. The global indices
  // of all the words that were already contained stay unchanged; `segment`'s
  // words are assigned the global indices that directly follow the ones of the
  // previously last segment. This is the operation that will let persisted
  // data (in particular the blobs of `NamedCachedQueryBlobManager`, in a
  // follow-up change) load its segments into the secondary vocabulary of the
  // corresponding index, one segment at a time.
  //
  // The words of `segment` have to be sorted and pairwise distinct, and none
  // of them may already be contained in one of the previous segments. Both of
  // these are checked, and both checks happen before anything is modified, so
  // that a rejected segment leaves this vocabulary unchanged. Establishing the
  // sorted order is the job of whoever builds the segment (for a segment that
  // was deserialized from a previously written one it trivially holds), and
  // not of this function, because `segment` may be a zero-copy view that must
  // not be reordered here.
  //
  // NOTE: If `segment` is a zero-copy view (see
  // `CompactVectorOfStrings::fromZeroCopyDeserializer`), the buffer that it
  // points into has to outlive this `SecondaryVocabulary`.
  void appendSegment(CompactVectorOfStrings<char> segment);

  // Return the number of words across all segments.
  size_t numWords() const;

  // Return the number of segments.
  size_t numSegments() const;

  // Return the word with the given global index.
  std::string_view operator[](SecondaryVocabIndex index) const;

  // Look up `word`. Return its global index if it is contained in this
  // vocabulary, and `std::nullopt` otherwise.
  std::optional<SecondaryVocabIndex> getId(std::string_view word) const;

 private:
  // Return, for each word of `segment` (in the order of `segment`), the
  // position in `sortedIndices_` at which the global index of that word has to
  // be inserted, that is, the lexicographic rank that the word has among the
  // words that are currently contained. Check while doing so that none of the
  // words is already contained. Since `segment` is sorted, the returned
  // positions are non-decreasing, and each of the binary searches can start
  // where the previous one ended.
  std::vector<size_t> insertPositionsInSortedIndices(
      const CompactVectorOfStrings<char>& segment) const;

  // Insert the global indices `firstGlobalIndex, firstGlobalIndex + 1, ...`
  // into `sortedIndices_` at the given `insertPositions`, which have to be
  // non-decreasing and to refer to the state of `sortedIndices_` from before
  // this insertion (see `insertPositionsInSortedIndices`). Fill the array from
  // the back, so that each of the previously contained global indices is moved
  // at most once.
  void mergeIntoSortedIndices(const std::vector<size_t>& insertPositions,
                              uint64_t firstGlobalIndex);

  // Return the word with the given global index. Used as the projection that
  // orders (and looks up) global indices by the word that each of them refers
  // to, in `getId` and `insertPositionsInSortedIndices`.
  std::string_view wordAt(uint64_t globalIndex) const;
};

#endif  // QLEVER_SRC_INDEX_VOCABULARY_SECONDARYVOCABULARY_H
