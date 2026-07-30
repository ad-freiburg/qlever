// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_GLOBAL_VOCABINDEXMARKER_H
#define QLEVER_SRC_GLOBAL_VOCABINDEXMARKER_H

#include <cstdint>

#include "global/ValueId.h"
#include "util/BitUtils.h"
#include "util/Exception.h"

// The words of a vocabulary can be distributed over several sub-vocabularies
// (see `SplitVocabulary`), for example to store all WKT literals separately.
// Which sub-vocabulary a word belongs to is determined by the word alone, and
// it is encoded in the highest bits of the payload of that word's `Id`, the
// marker.
//
// As a consequence, the `Id`s of such a vocabulary form one contiguous
// ascending range per marker, and their order is *not* the order of the string
// values: a word of a sub-vocabulary with a higher marker is greater than every
// word of a sub-vocabulary with a lower marker, no matter what the two words
// are. QLever accepts this (the order only has to be consistent, see
// `ValueId::compareThreeWay`), and the auxiliary vocabulary of an auxiliary
// index (see `index/AuxVocabulary.h`) has to be consistent with it: it uses the
// same split, and hence the same markers, as the vocabulary of the main index,
// so that each of its sub-vocabularies can be sorted into the corresponding
// sub-vocabulary of the main vocabulary.
//
// This class holds the marker arithmetic that the two of them share. Unlike
// `SplitVocabulary`, the number of markers is a runtime value here, because the
// auxiliary vocabulary has to adapt to whichever vocabulary type the main index
// uses.
class VocabIndexMarker {
 private:
  uint8_t numMarkers_ = 1;
  uint64_t markerShift_ = ValueId::numDataBits;

 public:
  // The largest number of sub-vocabularies (and hence markers) that is
  // supported. Note that `SplitVocabulary` allows up to 255 of them; if a split
  // with more than `maxNumMarkers` sub-vocabularies is ever added, this
  // constant has to be raised (a `static_assert` in `SplitVocabulary` enforces
  // this).
  static constexpr uint8_t maxNumMarkers = 4;

  VocabIndexMarker() = default;
  explicit VocabIndexMarker(uint8_t numMarkers) : numMarkers_{numMarkers} {
    AD_CONTRACT_CHECK(numMarkers >= 1 && numMarkers <= maxNumMarkers);
    markerShift_ =
        ValueId::numDataBits - ad_utility::bitMaskSizeForValue(numMarkers - 1);
  }

  // The number of sub-vocabularies.
  uint8_t numMarkers() const { return numMarkers_; }

  // The marker of the given index (the payload of an `Id`, that is, its bits
  // without the datatype bits).
  uint8_t getMarker(uint64_t indexWithMarker) const {
    auto marker = indexWithMarker >> markerShift_;
    AD_CORRECTNESS_CHECK(marker < numMarkers_);
    return static_cast<uint8_t>(marker);
  }

  // The index of the given index within its sub-vocabulary, that is, the index
  // with the marker bits removed.
  uint64_t getIndexWithoutMarker(uint64_t indexWithMarker) const {
    return indexWithMarker & ad_utility::bitMaskForLowerBits(markerShift_);
  }

  // The marker of the payload of the given `Id`.
  uint8_t getMarker(ValueId id) const {
    return getMarker(id.getBits() &
                     ad_utility::bitMaskForLowerBits(ValueId::numDataBits));
  }
};

#endif  // QLEVER_SRC_GLOBAL_VOCABINDEXMARKER_H
