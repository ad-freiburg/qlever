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

inline auto positiveRepresentableDoubleGenerator =
    ad_utility::RandomDoubleGenerator(Id::minPositiveDouble,
                                      std::numeric_limits<double>::max());
inline auto negativeRepresentableDoubleGenerator =
    ad_utility::RandomDoubleGenerator(-std::numeric_limits<double>::max(),
                                      -Id::minPositiveDouble);
inline auto nonRepresentableDoubleGenerator = ad_utility::RandomDoubleGenerator(
    -Id::minPositiveDouble, Id::minPositiveDouble);
inline auto indexGenerator =
    ad_utility::SlowRandomIntGenerator<uint64_t>(0, Id::maxIndex);
inline auto invalidIndexGenerator =
    ad_utility::SlowRandomIntGenerator<uint64_t>(
        Id::maxIndex, std::numeric_limits<uint64_t>::max());

inline auto nonOverflowingNBitGenerator =
    ad_utility::SlowRandomIntGenerator<int64_t>(Id::IntegerType::min(),
                                                Id::IntegerType::max());
inline auto overflowingNBitGenerator =
    ad_utility::SlowRandomIntGenerator<int64_t>(
        Id::IntegerType::max() + 1, std::numeric_limits<int64_t>::max());
inline auto underflowingNBitGenerator =
    ad_utility::SlowRandomIntGenerator<int64_t>(
        std::numeric_limits<int64_t>::min(), Id::IntegerType::min() - 1);

// Some helper functions to convert uint64_t values directly to and from index
// type `Id`s.
inline Id makeVocabId(uint64_t value) {
  return Id::makeFromVocabIndex(VocabIndex::make(value));
}
inline Id makeLocalVocabId(uint64_t value) {
  return ad_utility::testing::LocalVocabId(value);
}
inline Id makeTextRecordId(uint64_t value) {
  return Id::makeFromTextRecordIndex(TextRecordIndex::make(value));
}
inline Id makeWordVocabId(uint64_t value) {
  return Id::makeFromWordVocabIndex(WordVocabIndex::make(value));
}
inline Id makeBlankNodeId(uint64_t value) {
  return Id::makeFromBlankNodeIndex(BlankNodeIndex::make(value));
}
inline Id makeSecondaryVocabId(uint64_t value) {
  return Id::makeFromSecondaryVocabIndex(SecondaryVocabIndex::make(value));
}

inline uint64_t getVocabIndex(Id id) { return id.getVocabIndex().get(); }
// TODO<joka921> Make the tests more precise for the localVocabIndices.
inline std::string getLocalVocabIndex(Id id) {
  AD_CORRECTNESS_CHECK(id.getDatatype() == Datatype::LocalVocabIndex);
  return std::string{asStringViewUnsafe(id.getLocalVocabIndex()->getContent())};
}
inline uint64_t getTextRecordIndex(Id id) {
  return id.getTextRecordIndex().get();
}
inline uint64_t getWordVocabIndex(Id id) {
  return id.getWordVocabIndex().get();
}
inline uint64_t getSecondaryVocabIndex(Id id) {
  return id.getSecondaryVocabIndex().get();
}

inline auto addIdsFromGenerator = [](auto& generator, auto makeIds,
                                     std::vector<Id>& ids) {
  ad_utility::SlowRandomIntGenerator<uint8_t> numRepetitionGenerator(1, 4);
  for (size_t i = 0; i < numElements; ++i) {
    auto randomValue = generator();
    auto numRepetitions = numRepetitionGenerator();
    for (size_t j = 0; j < numRepetitions; ++j) {
      ids.push_back(makeIds(randomValue));
    }
  }
};
inline auto makeRandomDoubleIds = []() {
  std::vector<Id> ids;
  addIdsFromGenerator(positiveRepresentableDoubleGenerator, &Id::makeFromDouble,
                      ids);
  addIdsFromGenerator(negativeRepresentableDoubleGenerator, &Id::makeFromDouble,
                      ids);

  for (size_t i = 0; i < numElements; ++i) {
    ids.push_back(Id::makeFromDouble(0.0));
    ids.push_back(Id::makeFromDouble(-0.0));
    auto inf = std::numeric_limits<double>::infinity();
    ids.push_back(Id::makeFromDouble(inf));
    ids.push_back(Id::makeFromDouble(-inf));
    auto quietNan = std::numeric_limits<double>::quiet_NaN();
    ids.push_back(Id::makeFromDouble(quietNan));
    auto signalingNan = std::numeric_limits<double>::signaling_NaN();
    ids.push_back(Id::makeFromDouble(signalingNan));
    auto max = std::numeric_limits<double>::max();
    auto min = std::numeric_limits<double>::min();
    ids.push_back(Id::makeFromDouble(max));
    ids.push_back(Id::makeFromDouble(min));
  }
  ad_utility::randomShuffle(ids.begin(), ids.end());
  return ids;
};

inline auto makeRandomIds = []() {
  std::vector<Id> ids = makeRandomDoubleIds();
  addIdsFromGenerator(indexGenerator, &makeVocabId, ids);
  addIdsFromGenerator(indexGenerator, &makeLocalVocabId, ids);
  addIdsFromGenerator(indexGenerator, &makeTextRecordId, ids);
  addIdsFromGenerator(indexGenerator, &makeWordVocabId, ids);
  addIdsFromGenerator(indexGenerator, &makeBlankNodeId, ids);
  addIdsFromGenerator(indexGenerator, &makeSecondaryVocabId, ids);
  addIdsFromGenerator(nonOverflowingNBitGenerator, &Id::makeFromInt, ids);
  addIdsFromGenerator(overflowingNBitGenerator, &Id::makeFromInt, ids);
  addIdsFromGenerator(underflowingNBitGenerator, &Id::makeFromInt, ids);

  for (size_t i = 0; i < numElements; ++i) {
    ids.push_back(Id::makeUndefined());
  }

  ad_utility::randomShuffle(ids.begin(), ids.end());
  return ids;
};

#endif  // QLEVER_VALUEIDTESTHELPERS_H
