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

#include <array>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "backports/span.h"
#include "global/IndexTypes.h"
#include "util/CompactStringVector.h"
#include "util/Exception.h"
#include "util/OwnedOrViewedVector.h"
#include "util/Serializer/SerializeVector.h"
#include "util/Serializer/Serializer.h"

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
// `NamedCachedQueryBlobManager`) to add words
// incrementally, one segment at a time, without invalidating the `Id`s of the
// earlier segments. Lexicographic ranks, in contrast, do change when a segment
// is appended.
//
// The `Id`s of this vocabulary, in contrast to the `Id`s of the main
// vocabulary, do not represent a *semantic* (that is, by string value) order
// of the words, but only the order in which the words were inserted. In
// particular, all `Id`s of this vocabulary compare greater than all `Id`s of
// the main vocabulary.
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
 public:
  // The magic bytes and the format version with which the serialization of a
  // `SecondaryVocabulary` starts (see `AD_SERIALIZE_FRIEND_FUNCTION` below).
  // The `serializationFormatVersion` has to be increased when the format of
  // the serialization is changed.
  static constexpr std::array<char, 8> serializationMagicBytes{
      'Q', 'L', 'S', 'E', 'C', 'V', 'O', 'C'};
  static constexpr uint16_t serializationFormatVersion = 1;

 private:
  // An array of global indices (see `segmentOffsets_` and `sortedIndices_`
  // below). It is either owned, or a non-owning, zero-copy view into the
  // buffer of the serializer from which this vocabulary was read (see the
  // serialization below). In the latter case, that buffer has to outlive this
  // vocabulary.
  using IndexArray = ad_utility::OwnedOrViewedVector<uint64_t>;

  // The words, stored as an append-only sequence of segments, each of which
  // holds its words in sorted order (see `appendSegment`).
  std::vector<CompactVectorOfStrings<char>> segments_;

  // For each segment, the global index of its first word, that is, the sum of
  // the sizes of all the segments that precede it. Has the same number of
  // elements as `segments_` and is sorted, so that `operator[]` can map a
  // global index to the corresponding `[segmentIndex, indexWithinSegment]`
  // pair by binary search.
  IndexArray segmentOffsets_;

  // The global indices of all words, ordered by the word that each of them
  // refers to; that is, `sortedIndices_[rank]` is the global index of the word
  // with lexicographic rank `rank`. For the example from the comment on this
  // class (the segments `["<b>", "<d>"]` and `["<a>"]`, appended in that
  // order), the global indices are `<b>` -> 0, `<d>` -> 1 and `<a>` -> 2, and
  // `sortedIndices_` is `[2, 0, 1]`: `sortedIndices_[0] == 2`, because the
  // lexicographically smallest word `<a>` has global index 2. `getId` binary-
  // searches in this array, and `appendSegment` merges the global indices of
  // the newly added words into it.
  IndexArray sortedIndices_;

 public:
  SecondaryVocabulary() = default;

  // Create a vocabulary that holds the given `words` as a single segment. The
  // `words` have to be sorted and pairwise distinct (which is checked, see
  // `appendSegment`), and none of them may be contained in the vocabulary of
  // the main index.
  explicit SecondaryVocabulary(ql::span<const std::string> words);

  // Append `segment` as a new segment of this vocabulary. The global indices
  // of all the words that were already contained stay unchanged; `segment`'s
  // words are assigned the global indices that directly follow the ones of the
  // previously last segment.
  //
  // The words of `segment` have to be sorted (via the plain lexicographical
  // bytewise comparison of `std::string(_view)`) and pairwise distinct, and
  // none of them may already be contained in one of the previous segments. Both
  // of these are checked, and both checks happen before anything is modified,
  // so that a rejected segment leaves this vocabulary unchanged. Establishing
  // the sorted order is the job of the caller. This allows the segments to be
  // read-only, zero-copy views, which cannot be reordered.
  //
  // NOTE 1: If `segment` is a zero-copy view (see
  // `CompactVectorOfStrings::fromZeroCopyDeserializer`), the buffer that it
  // points into has to outlive this `SecondaryVocabulary`.
  //
  // NOTE 2: An empty `segment` is ignored, that is, it doesn't count as a
  // segment (see `numSegments`).
  //
  // NOTE 3: If this vocabulary was read via zero-copy deserialization (see the
  // serialization below), then it is read-only, and appending a non-empty
  // `segment` throws. To extend such a vocabulary, extend a `clone()` of it.
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

  // Return a deep copy of this vocabulary, which owns all its segments (even
  // if the segments of this vocabulary are zero-copy views), and in which all
  // words have the same global indices as in this vocabulary. This is an
  // explicit function instead of a copy constructor, because the segments are
  // move-only, and because the copy is expensive.
  SecondaryVocabulary clone() const;

  // Serialize the vocabulary as the `serializationMagicBytes` and the
  // `serializationFormatVersion`, followed by its number of segments, the
  // segments (in the order in which they were appended, so that all words keep
  // their global indices), the `segmentOffsets_`, and the `sortedIndices_`.
  // The format version is stored explicitly, so that the format can be
  // changed in the future without breaking the reading of previously
  // serialized vocabularies.
  //
  // Reading is designed to be as cheap as possible: If the `serializer`
  // supports zero-copy deserialization (see `ZeroCopyReadSerializer`), then
  // the segments, the `segmentOffsets_`, and the `sortedIndices_` are all
  // zero-copy views into its buffer, which then has to outlive this
  // vocabulary (see NOTE 1 at `appendSegment`). Otherwise, they are read into
  // owned storage. In both cases, nothing is recomputed.
  //
  // NOTE: Apart from the magic bytes, the version, and some O(1) consistency
  // checks of the sizes, the input is trusted and not checked (in particular,
  // the checks of `appendSegment` are not run), because a check would have to
  // look at all the words. When reading, `arg` has to be empty.
  AD_SERIALIZE_FRIEND_FUNCTION(SecondaryVocabulary) {
    using namespace ad_utility::serialization;
    if constexpr (WriteSerializer<S>) {
      serializer << serializationMagicBytes;
      serializer << serializationFormatVersion;
      serializer << arg.segments_;
      serializer << arg.segmentOffsets_;
      serializer << arg.sortedIndices_;
    } else {
      AD_CONTRACT_CHECK(arg.numSegments() == 0,
                        "A secondary vocabulary can only be deserialized into "
                        "an empty one");
      std::decay_t<decltype(serializationMagicBytes)> magicBytes{};
      serializer >> magicBytes;
      AD_CONTRACT_CHECK(magicBytes == serializationMagicBytes,
                        "The serialized secondary vocabulary does not start "
                        "with the expected magic bytes");
      uint16_t formatVersion = 0;
      serializer >> formatVersion;
      AD_CONTRACT_CHECK(formatVersion == serializationFormatVersion,
                        "The serialized secondary vocabulary has the "
                        "unsupported format version ",
                        formatVersion, ", expected ",
                        serializationFormatVersion);
      if constexpr (ZeroCopyReadSerializer<S>) {
        // Read the segments in the format of a `std::vector` (the number of
        // segments, followed by the segments), but as zero-copy views.
        size_t numSegments = 0;
        serializer >> numSegments;
        arg.segments_.reserve(numSegments);
        for (size_t i = 0; i < numSegments; ++i) {
          arg.segments_.push_back(
              CompactVectorOfStrings<char>::fromZeroCopyDeserializer(
                  serializer));
        }
      } else {
        serializer >> arg.segments_;
      }
      const size_t numSegments = arg.segments_.size();
      auto readIndexArray = [&serializer](IndexArray& array) {
        if constexpr (ZeroCopyReadSerializer<S>) {
          array = IndexArray::fromZeroCopyDeserializer(serializer);
        } else {
          serializer >> array;
        }
      };
      readIndexArray(arg.segmentOffsets_);
      readIndexArray(arg.sortedIndices_);
      AD_CORRECTNESS_CHECK(arg.segmentOffsets_.size() == numSegments);
      AD_CORRECTNESS_CHECK(numSegments == 0 || arg.segmentOffsets_[0] == 0);
      AD_CORRECTNESS_CHECK(arg.sortedIndices_.size() == arg.numWords());
    }
  }

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
  // into `sortedIndices` (the owned elements of `sortedIndices_`) at the given
  // `insertPositions`, which have to be non-decreasing and to refer to the
  // state of `sortedIndices` from before this insertion (see
  // `insertPositionsInSortedIndices`). Fill the array from the back, so that
  // each of the previously contained global indices is moved at most once.
  static void mergeIntoSortedIndices(std::vector<uint64_t>& sortedIndices,
                                     const std::vector<size_t>& insertPositions,
                                     uint64_t firstGlobalIndex);

  // Return the first position in `[first, sortedIndices_.size())` whose word
  // is not less than `word`, together with whether the word at that position
  // is equal to `word` (that is, whether `word` is contained in that range).
  std::pair<size_t, bool> lowerBoundInSortedIndices(std::string_view word,
                                                    size_t first) const;

  // Return the word with the given global index. Used as the projection that
  // orders (and looks up) global indices by the word that each of them refers
  // to, in `lowerBoundInSortedIndices`.
  std::string_view wordAt(uint64_t globalIndex) const;
};

#endif  // QLEVER_SRC_INDEX_VOCABULARY_SECONDARYVOCABULARY_H
