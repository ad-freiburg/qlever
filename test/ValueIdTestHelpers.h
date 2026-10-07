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

template <typename IdType = Id>
inline auto positiveRepresentableDoubleGenerator =
    ad_utility::RandomDoubleGenerator(IdType::minPositiveDouble,
                                      std::numeric_limits<double>::max());
template <typename IdType = Id>
inline auto negativeRepresentableDoubleGenerator =
    ad_utility::RandomDoubleGenerator(-std::numeric_limits<double>::max(),
                                      -IdType::minPositiveDouble);
template <typename IdType = Id>
inline auto nonRepresentableDoubleGenerator = ad_utility::RandomDoubleGenerator(
    -IdType::minPositiveDouble, Id::minPositiveDouble);
template <typename IdType = Id>
inline auto indexGenerator =
    ad_utility::SlowRandomIntGenerator<uint64_t>(0, IdType::maxIndex);
template <typename IdType = Id>
inline auto invalidIndexGenerator =
    ad_utility::SlowRandomIntGenerator<uint64_t>(
        IdType::maxIndex, std::numeric_limits<uint64_t>::max());

template <typename IdType = Id>
inline auto nonOverflowingNBitGenerator =
    ad_utility::SlowRandomIntGenerator<int64_t>(IdType::IntegerType::min(),
                                                IdType::IntegerType::max());
template <typename IdType = Id>
inline auto overflowingNBitGenerator =
    ad_utility::SlowRandomIntGenerator<int64_t>(
        IdType::IntegerType::max() + 1, std::numeric_limits<int64_t>::max());
template <typename IdType = Id>
inline auto underflowingNBitGenerator =
    ad_utility::SlowRandomIntGenerator<int64_t>(
        std::numeric_limits<int64_t>::min(), IdType::IntegerType::min() - 1);

// Some helper functions to convert uint64_t values directly to and from index
// type `Id`s.
template <typename IdType = Id>
inline IdType makeVocabId(uint64_t value) {
  return IdType::makeFromVocabIndex(VocabIndex::make(value));
}
template <typename IdType = Id>
inline IdType makeLocalVocabId(uint64_t value) {
  return ad_utility::testing::LocalVocabId(value);
}
template <typename IdType = Id>
inline IdType makeTextRecordId(uint64_t value) {
  return IdType::makeFromTextRecordIndex(TextRecordIndex::make(value));
}
template <typename IdType = Id>
inline IdType makeWordVocabId(uint64_t value) {
  return IdType::makeFromWordVocabIndex(WordVocabIndex::make(value));
}
template <typename IdType = Id>
inline IdType makeBlankNodeId(uint64_t value) {
  return IdType::makeFromBlankNodeIndex(BlankNodeIndex::make(value));
}
template <typename IdType = Id>
inline IdType makeSecondaryVocabId(uint64_t value) {
  return IdType::makeFromSecondaryVocabIndex(SecondaryVocabIndex::make(value));
}
template <typename IdType = Id>
inline uint64_t getVocabIndex(IdType id) {
  return id.getVocabIndex().get();
}
// TODO<joka921> Make the tests more precise for the localVocabIndices.
template <typename IdType = Id>
inline std::string getLocalVocabIndex(IdType id) {
  AD_CORRECTNESS_CHECK(id.getDatatype() == Datatype::LocalVocabIndex);
  return std::string{asStringViewUnsafe(id.getLocalVocabIndex()->getContent())};
}
template <typename IdType = Id>
inline uint64_t getTextRecordIndex(IdType id) {
  return id.getTextRecordIndex().get();
}
template <typename IdType = Id>
inline uint64_t getWordVocabIndex(IdType id) {
  return id.getWordVocabIndex().get();
}
template <typename IdType = Id>
inline uint64_t getSecondaryVocabIndex(IdType id) {
  return id.getSecondaryVocabIndex().get();
}

template <typename IdType = Id>
inline auto addIdsFromGenerator =
    [](auto& generator, auto makeIds, std::vector<IdType>& ids) {
      ad_utility::SlowRandomIntGenerator<uint8_t> numRepetitionGenerator(1, 4);
      for (size_t i = 0; i < numElements; ++i) {
        auto randomValue = generator();
        auto numRepetitions = numRepetitionGenerator();
        for (size_t j = 0; j < numRepetitions; ++j) {
          ids.push_back(makeIds(randomValue));
        }
      }
    };
template <typename IdType = Id>
inline auto makeRandomDoubleIds = []() {
  std::vector<IdType> ids;
  addIdsFromGenerator<IdType>(positiveRepresentableDoubleGenerator<IdType>,
                              &IdType::makeFromDouble, ids);
  addIdsFromGenerator<IdType>(negativeRepresentableDoubleGenerator<IdType>,
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
};

template <typename IdType = Id>
inline auto makeRandomIds = [] {
  std::vector<IdType> ids = makeRandomDoubleIds<IdType>();
  addIdsFromGenerator<IdType>(indexGenerator<IdType>,
                              &makeVocabId<MixedValueId>, ids);
  addIdsFromGenerator<IdType>(indexGenerator<IdType>,
                              &makeLocalVocabId<MixedValueId>, ids);
  addIdsFromGenerator<IdType>(indexGenerator<IdType>,
                              &makeTextRecordId<MixedValueId>, ids);
  addIdsFromGenerator<IdType>(indexGenerator<IdType>,
                              &makeWordVocabId<MixedValueId>, ids);
  addIdsFromGenerator<IdType>(indexGenerator<IdType>,
                              &makeBlankNodeId<MixedValueId>, ids);
  addIdsFromGenerator<IdType>(indexGenerator<IdType>,
                              &makeSecondaryVocabId<MixedValueId>, ids);
  addIdsFromGenerator<IdType>(nonOverflowingNBitGenerator<IdType>,
                              &IdType::makeFromInt, ids);
  addIdsFromGenerator<IdType>(overflowingNBitGenerator<IdType>,
                              &IdType::makeFromInt, ids);
  addIdsFromGenerator<IdType>(underflowingNBitGenerator<IdType>,
                              &IdType::makeFromInt, ids);

  for (size_t i = 0; i < numElements; ++i) {
    ids.push_back(IdType::makeUndefined());
  }

  ad_utility::randomShuffle(ids.begin(), ids.end());
  return ids;
};

#endif  // QLEVER_VALUEIDTESTHELPERS_H
