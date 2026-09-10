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
// The words are kept in RAM, in insertion order, as an append-only sequence of
// *segments* (see `appendSegment`). The `Id` of a word is its position in that
// insertion order (the global index over the concatenation of all segments,
// in the order in which they were appended), and it never changes when
// further segments are appended. This is what allows persisted data (in
// particular the blobs of `NamedCachedQueryBlobManager`, in a follow-up
// change) to add words incrementally, one segment at a time, without
// invalidating the `Id`s of the earlier segments.
//
// There is no *semantic* (that is, by string value) order among the words of
// this vocabulary; they compare by their `Id`s alone, which is why all `Id`s
// of this vocabulary compare greater than all `Id`s of the main vocabulary.
//
// TODO<joka921> The eventual implementation additionally stores, for each
// word, the position at which that word would be sorted into the main
// vocabulary, which a *semantic* comparison of an `Id` of this vocabulary with
// an `Id` of the main vocabulary needs; it follows in the changes that build
// on this one.
class SecondaryVocabulary {
 private:
  // The words, stored as an append-only sequence of segments, each of which
  // holds its words in the order in which they were appended to that segment.
  std::vector<CompactVectorOfStrings<char>> segments_;

  // For each segment, the global index of its first word, that is, the sum of
  // the sizes of all the segments that precede it. Has the same number of
  // elements as `segments_`.
  std::vector<uint64_t> segmentOffsets_;

  // The global indices of all words, sorted by the word that each of them
  // refers to (using `std::string_view`'s comparison). Rebuilt whenever a
  // segment is appended, so that `getId` can look up a word by binary search.
  std::vector<uint64_t> sortedIndices_;

 public:
  SecondaryVocabulary() = default;

  // Create a vocabulary that holds the given `words` as a single segment,
  // none of which may be contained in the vocabulary of the main index. The
  // words may be in any order, because the `Id` of a word is its position in
  // `words`; they have to be pairwise distinct, which is checked.
  explicit SecondaryVocabulary(std::vector<std::string> words);

  // Append `segment` as a new segment of this vocabulary. The `Id`s of all the
  // words that were already contained in this vocabulary stay unchanged;
  // `segment`'s words are assigned the global indices that directly follow the
  // ones of the previously last segment. None of `segment`'s words may already
  // be contained in this vocabulary (across all of its segments), and the
  // words within `segment` itself have to be pairwise distinct; both of these
  // are checked. This is the operation that will let persisted data (in
  // particular the blobs of `NamedCachedQueryBlobManager`, in a follow-up
  // change) load its segments into the secondary vocabulary of the
  // corresponding index, one segment at a time. NOTE: If `segment` is a
  // zero-copy view (see `CompactVectorOfStrings::fromZeroCopyDeserializer`),
  // the buffer that it points into has to outlive this `SecondaryVocabulary`.
  void appendSegment(CompactVectorOfStrings<char> segment);

  // Return the number of words across all segments.
  size_t numWords() const;

  // Return the number of segments.
  size_t numSegments() const;

  // Return the word with the given index.
  std::string_view operator[](SecondaryVocabIndex index) const;

  // Look up `word`. Return its index if it is contained in this vocabulary, and
  // `std::nullopt` otherwise.
  std::optional<SecondaryVocabIndex> getId(std::string_view word) const;

 private:
  // Rebuild `sortedIndices_` from scratch, sorting the global indices of all
  // currently contained words by the word that each of them refers to.
  void rebuildSortedIndices();

  // Return the word with the given global index. Used as the projection that
  // sorts (and looks up) global indices by the word that each of them refers
  // to, in `getId` and `rebuildSortedIndices`.
  std::string_view wordAt(uint64_t globalIndex) const;
};

#endif  // QLEVER_SRC_INDEX_VOCABULARY_SECONDARYVOCABULARY_H
