// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_MERGER_SEGMENTLOCALID_H
#define QLEVER_SRC_INDEX_VOCABULARY_MERGER_SEGMENTLOCALID_H

#include <cstddef>
#include <cstdint>
#include <ostream>

#include "backports/three_way_comparison.h"
#include "global/Id.h"
#include "util/BitUtils.h"
#include "util/Exception.h"

// The segment-local ID of a merged word, see
// `index/vocabulary_merger/Segment.h` (which also explains the terminology of
// the sub-vocabularies). It lives in its own header because the
// `SegmentMetaData` (see `index/vocabulary_merger/VocabularyMetaData.h`) also
// uses it.
namespace ad_utility::vocabulary_merger::detail {

// The ID of a distinct word within its segment. It packs two things into 64
// bits:
//
// - In the upper `numSubVocabIndexBits` bits, the index of the sub-vocabulary
//   of the word (see `Segment.h`), or `blankNodeSubVocabularyIndex` (all bits
//   set) for a blank node.
// - In the lower `numIndexBits` bits, the index of the word within its
//   segment and sub-vocabulary: the number of words of the same sub-vocabulary
//   that stand before it in the segment, as transformed by
//   `ParallelWordWriterBase::indexOf` (which may encode further information,
//   like a marker bit, into it). For a blank node, the number of blank nodes
//   before it in the segment. The `IdConverterForSegment` (see `Segment.h`)
//   later turns this into the global index by adding the number of words of
//   the sub-vocabulary (or of blank nodes) in all the previous segments.
//
// The index has as many bits as a vocabulary index,
// so that the global index fits into a `ValueId` afterwards.
//
// Example (with the current `ValueId::numVocabIndexBits` of 60): the third word
// (index `2`) of the sub-vocabulary `1` has the bits `0001 | 000...0010`, that
// is, the value `(1 << 60) | 2`; the first blank node of a segment has the
// bits `1111 | 000...0000`, that is, `15 << 60`.
class SegmentLocalId {
 public:
  static constexpr size_t numIndexBits = ValueId::numVocabIndexBits;
  static constexpr size_t numSubVocabIndexBits = 64 - numIndexBits;
  static_assert(numSubVocabIndexBits >= 4,
                "At least four bits are needed for the sub-vocabulary index");
  // The largest index that fits into the lower bits.
  static constexpr uint64_t maxIndex = bitMaskForLowerBits(numIndexBits);
  // The index of the pseudo sub-vocabulary of the blank nodes, the largest
  // value that fits into the upper bits. The real sub-vocabularies have
  // smaller indices.
  static constexpr uint8_t blankNodeSubVocabularyIndex =
      bitMaskForLowerBits(numSubVocabIndexBits);

 private:
  // NOTE: Like a plain integer, a default-initialized ID (`SegmentLocalId
  // id;`) has an indeterminate value, while a value-initialized one
  // (`SegmentLocalId{}`, or the elements of a resized vector) is zero, because
  // the default constructor is defaulted. An initializer would gain nothing.
  uint64_t bits_;

 public:
  SegmentLocalId() = default;

  // Create the ID of the word with the `index` in the (real) sub-vocabulary
  // with the index `subVocabIndex`. Throw if the index does not fit into its
  // bits, or if `subVocabIndex` is not that of a real sub-vocabulary (see
  // `blankNodeSubVocabularyIndex`).
  static SegmentLocalId makeFromWordIndex(uint8_t subVocabIndex,
                                          uint64_t index) {
    AD_CORRECTNESS_CHECK(subVocabIndex < blankNodeSubVocabularyIndex);
    return make(subVocabIndex, index);
  }

  // Create the ID of the blank node with the given `number` in the segment.
  static SegmentLocalId makeBlankNode(uint64_t number) {
    return make(blankNodeSubVocabularyIndex, number);
  }

  // The index of the sub-vocabulary (the upper bits), see above.
  uint8_t subVocabIndex() const {
    return static_cast<uint8_t>(bits_ >> numIndexBits);
  }

  // The index in the sub-vocabulary (the lower bits), see above.
  uint64_t index() const { return bits_ & maxIndex; }

  // Whether this is the ID of a blank node.
  bool isBlankNode() const {
    return subVocabIndex() == blankNodeSubVocabularyIndex;
  }

  // The packed 64 bits.
  uint64_t bits() const { return bits_; }

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(SegmentLocalId, bits_)

  // Make the output of failed tests readable.
  friend std::ostream& operator<<(std::ostream& os, const SegmentLocalId& id) {
    return os << "SegmentLocalId(" << static_cast<unsigned>(id.subVocabIndex())
              << ", " << id.index() << ')';
  }

 private:
  // Pack the `subVocabIndex` (a real one or that of the blank nodes) and the
  // `index`. Throw if either does not fit into its bits.
  static SegmentLocalId make(uint8_t subVocabIndex, uint64_t index) {
    AD_CORRECTNESS_CHECK(subVocabIndex <= blankNodeSubVocabularyIndex &&
                         index <= maxIndex);
    SegmentLocalId id;
    id.bits_ = (static_cast<uint64_t>(subVocabIndex) << numIndexBits) | index;
    return id;
  }
};
}  // namespace ad_utility::vocabulary_merger::detail

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_SEGMENTLOCALID_H
