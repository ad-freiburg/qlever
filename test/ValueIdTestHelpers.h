//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_VALUEIDTESTHELPERS_H
#define QLEVER_VALUEIDTESTHELPERS_H

#include "./util/IdTestHelpers.h"
#include "global/Id.h"
#include "util/Random.h"

// Enabling cheaper unit tests when building in Debug mode
#ifdef QLEVER_RUN_EXPENSIVE_TESTS
static constexpr size_t numElements = 10'000;
#else
static constexpr size_t numElements = 10;
#endif

// All helpers are templates on the type of the `Id` (by default the `Id` of the
// current build), so that `IdBaseTest.cpp` can use them for both layouts.

// Random values for an `Id` of type `IdType`.
template <typename IdType = ValueId>
inline auto positiveRepresentableDoubleGenerator =
    ad_utility::RandomDoubleGenerator(IdType::minPositiveDouble,
                                      std::numeric_limits<double>::max());
template <typename IdType = ValueId>
inline auto negativeRepresentableDoubleGenerator =
    ad_utility::RandomDoubleGenerator(-std::numeric_limits<double>::max(),
                                      -IdType::minPositiveDouble);
template <typename IdType = ValueId>
inline auto nonRepresentableDoubleGenerator = ad_utility::RandomDoubleGenerator(
    -IdType::minPositiveDouble, IdType::minPositiveDouble);
template <typename IdType = ValueId>
inline auto indexGenerator =
    ad_utility::SlowRandomIntGenerator<uint64_t>(0, IdType::maxIndex);
template <typename IdType = ValueId>
inline auto invalidIndexGenerator =
    ad_utility::SlowRandomIntGenerator<uint64_t>(
        IdType::maxIndex, std::numeric_limits<uint64_t>::max());
template <typename IdType = ValueId>
inline auto nonOverflowingNBitGenerator =
    ad_utility::SlowRandomIntGenerator<int64_t>(IdType::IntegerType::min(),
                                                IdType::IntegerType::max());
// The integers that don't fit into an `Id`. There are none for the split
// layout, see `makeRandomIds` below.
template <typename IdType = ValueId>
inline auto overflowingNBitGenerator =
    ad_utility::SlowRandomIntGenerator<int64_t>(
        IdType::IntegerType::max() + 1, std::numeric_limits<int64_t>::max());
template <typename IdType = ValueId>
inline auto underflowingNBitGenerator =
    ad_utility::SlowRandomIntGenerator<int64_t>(
        std::numeric_limits<int64_t>::min(), IdType::IntegerType::min() - 1);

// Convert `uint64_t` values directly to and from `Id`s of an index type.
template <typename IdType = ValueId>
IdType makeVocabId(uint64_t value) {
  return IdType::makeFromVocabIndex(VocabIndex::make(value));
}
template <typename IdType = ValueId>
IdType makeLocalVocabId(uint64_t value) {
  return IdType::makeFromLocalVocabIndex(
      ad_utility::testing::LocalVocabId(value).getLocalVocabIndex());
}
template <typename IdType = ValueId>
IdType makeTextRecordId(uint64_t value) {
  return IdType::makeFromTextRecordIndex(TextRecordIndex::make(value));
}
template <typename IdType = ValueId>
IdType makeWordVocabId(uint64_t value) {
  return IdType::makeFromWordVocabIndex(WordVocabIndex::make(value));
}
template <typename IdType = ValueId>
IdType makeBlankNodeId(uint64_t value) {
  return IdType::makeFromBlankNodeIndex(BlankNodeIndex::make(value));
}
template <typename IdType = ValueId>
IdType makeSecondaryVocabId(uint64_t value) {
  return IdType::makeFromSecondaryVocabIndex(SecondaryVocabIndex::make(value));
}
template <typename IdType = ValueId>
uint64_t getVocabIndex(IdType id) {
  return id.getVocabIndex().get();
}
// TODO<joka921> Make the tests more precise for the localVocabIndices.
template <typename IdType = ValueId>
std::string getLocalVocabIndex(IdType id) {
  AD_CORRECTNESS_CHECK(id.getDatatype() == Datatype::LocalVocabIndex);
  return std::string{id.getLocalVocabIndex()->getContent()};
}
template <typename IdType = ValueId>
uint64_t getTextRecordIndex(IdType id) {
  return id.getTextRecordIndex().get();
}
template <typename IdType = ValueId>
uint64_t getWordVocabIndex(IdType id) {
  return id.getWordVocabIndex().get();
}
template <typename IdType = ValueId>
uint64_t getSecondaryVocabIndex(IdType id) {
  return id.getSecondaryVocabIndex().get();
}

inline auto addIdsFromGenerator = [](auto& generator, auto makeIds, auto& ids) {
  ad_utility::SlowRandomIntGenerator<uint8_t> numRepetitionGenerator(1, 4);
  for (size_t i = 0; i < numElements; ++i) {
    auto randomValue = generator();
    auto numRepetitions = numRepetitionGenerator();
    for (size_t j = 0; j < numRepetitions; ++j) {
      ids.push_back(makeIds(randomValue));
    }
  }
};

template <typename IdType = ValueId>
std::vector<IdType> makeRandomDoubleIds() {
  std::vector<IdType> ids;
  addIdsFromGenerator(positiveRepresentableDoubleGenerator<IdType>,
                      &IdType::makeFromDouble, ids);
  addIdsFromGenerator(negativeRepresentableDoubleGenerator<IdType>,
                      &IdType::makeFromDouble, ids);

  for (size_t i = 0; i < numElements; ++i) {
    ids.push_back(IdType::makeFromDouble(0.0));
    ids.push_back(IdType::makeFromDouble(-0.0));
    auto inf = std::numeric_limits<double>::infinity();
    ids.push_back(IdType::makeFromDouble(inf));
    ids.push_back(IdType::makeFromDouble(-inf));
    auto quietNan = std::numeric_limits<double>::quiet_NaN();
    ids.push_back(IdType::makeFromDouble(quietNan));
    auto signalingNan = std::numeric_limits<double>::signaling_NaN();
    ids.push_back(IdType::makeFromDouble(signalingNan));
    auto max = std::numeric_limits<double>::max();
    auto min = std::numeric_limits<double>::min();
    ids.push_back(IdType::makeFromDouble(max));
    ids.push_back(IdType::makeFromDouble(min));
  }
  ad_utility::randomShuffle(ids.begin(), ids.end());
  return ids;
}

template <typename IdType = ValueId>
std::vector<IdType> makeRandomIds() {
  std::vector<IdType> ids = makeRandomDoubleIds<IdType>();
  addIdsFromGenerator(indexGenerator<IdType>, &makeVocabId<IdType>, ids);
  addIdsFromGenerator(indexGenerator<IdType>, &makeLocalVocabId<IdType>, ids);
  addIdsFromGenerator(indexGenerator<IdType>, &makeTextRecordId<IdType>, ids);
  addIdsFromGenerator(indexGenerator<IdType>, &makeWordVocabId<IdType>, ids);
  addIdsFromGenerator(indexGenerator<IdType>, &makeBlankNodeId<IdType>, ids);
  addIdsFromGenerator(indexGenerator<IdType>, &makeSecondaryVocabId<IdType>,
                      ids);
  addIdsFromGenerator(nonOverflowingNBitGenerator<IdType>, &IdType::makeFromInt,
                      ids);
  if constexpr (IdType::maxInt < std::numeric_limits<int64_t>::max()) {
    addIdsFromGenerator(overflowingNBitGenerator<IdType>, &IdType::makeFromInt,
                        ids);
    addIdsFromGenerator(underflowingNBitGenerator<IdType>, &IdType::makeFromInt,
                        ids);
  }

  for (size_t i = 0; i < numElements; ++i) {
    ids.push_back(IdType::makeUndefined());
  }

  ad_utility::randomShuffle(ids.begin(), ids.end());
  return ids;
}

#endif  // QLEVER_VALUEIDTESTHELPERS_H
