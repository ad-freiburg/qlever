//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_VALUEIDTESTHELPERS_H
#define QLEVER_VALUEIDTESTHELPERS_H

#include "../src/global/ValueId.h"
#include "../src/util/Random.h"

// Enabling cheaper unit tests when building in Debug mode
#ifdef QLEVER_RUN_EXPENSIVE_TESTS
static constexpr size_t numElements = 10'000;
#else
static constexpr size_t numElements = 10;
#endif

inline auto positiveRepresentableDoubleGenerator = RandomDoubleGenerator(
    ValueId::minPositiveDouble, std::numeric_limits<double>::max());
inline auto negativeRepresentableDoubleGenerator = RandomDoubleGenerator(
    -std::numeric_limits<double>::max(), -ValueId::minPositiveDouble);
inline auto nonRepresentableDoubleGenerator = RandomDoubleGenerator(
    -ValueId::minPositiveDouble, ValueId::minPositiveDouble);
inline auto indexGenerator =
    SlowRandomIntGenerator<uint64_t>(0, ValueId::maxIndex);
inline auto invalidIndexGenerator = SlowRandomIntGenerator<uint64_t>(
    ValueId::maxIndex, std::numeric_limits<uint64_t>::max());

inline auto nonOverflowingNBitGenerator = SlowRandomIntGenerator<int64_t>(
    ValueId::IntegerType::min(), ValueId::IntegerType::max());
inline auto overflowingNBitGenerator = SlowRandomIntGenerator<int64_t>(
    ValueId::IntegerType::max() + 1, std::numeric_limits<int64_t>::max());
inline auto underflowingNBitGenerator = SlowRandomIntGenerator<int64_t>(
    std::numeric_limits<int64_t>::min(), ValueId::IntegerType::min() - 1);

// Some helper functions to convert uint64_t values directly to and from index
// type `ValueId`s.
inline ValueId makeVocabId(uint64_t value) {
  return ValueId::makeFromVocabIndex(VocabIndex::make(value));
}
inline ValueId makeLocalVocabId(uint64_t value) {
  return ValueId::makeFromLocalVocabIndex(LocalVocabIndex::make(value));
}
inline ValueId makeTextRecordId(uint64_t value) {
  return ValueId::makeFromTextRecordIndex(TextRecordIndex::make(value));
}
inline ValueId makeWordVocabId(uint64_t value) {
  return ValueId::makeFromWordVocabIndex(WordVocabIndex::make(value));
}

inline uint64_t getVocabIndex(ValueId id) { return id.getVocabIndex().get(); }
inline uint64_t getLocalVocabIndex(ValueId id) {
  return id.getLocalVocabIndex().get();
}
inline uint64_t getTextRecordIndex(ValueId id) {
  return id.getTextRecordIndex().get();
}
inline uint64_t getWordVocabIndex(ValueId id) {
  return id.getWordVocabIndex().get();
}

inline auto addIdsFromGenerator = [](auto& generator, auto makeIds,
                                     std::vector<ValueId>& ids) {
  SlowRandomIntGenerator<uint8_t> numRepetitionGenerator(1, 4);
  for (size_t i = 0; i < numElements; ++i) {
    auto randomValue = generator();
    auto numRepetitions = numRepetitionGenerator();
    for (size_t j = 0; j < numRepetitions; ++j) {
      ids.push_back(makeIds(randomValue));
    }
  }
};
inline auto makeRandomDoubleIds = []() {
  std::vector<ValueId> ids;
  addIdsFromGenerator(positiveRepresentableDoubleGenerator,
                      &ValueId::makeFromDouble, ids);
  addIdsFromGenerator(negativeRepresentableDoubleGenerator,
                      &ValueId::makeFromDouble, ids);

  for (size_t i = 0; i < numElements; ++i) {
    ids.push_back(ValueId::makeFromDouble(0.0));
    ids.push_back(ValueId::makeFromDouble(-0.0));
    auto inf = std::numeric_limits<double>::infinity();
    ids.push_back(ValueId::makeFromDouble(inf));
    ids.push_back(ValueId::makeFromDouble(-inf));
    auto quietNan = std::numeric_limits<double>::quiet_NaN();
    ids.push_back(ValueId::makeFromDouble(quietNan));
    auto signalingNan = std::numeric_limits<double>::signaling_NaN();
    ids.push_back(ValueId::makeFromDouble(signalingNan));
    auto max = std::numeric_limits<double>::max();
    auto min = std::numeric_limits<double>::min();
    ids.push_back(ValueId::makeFromDouble(max));
    ids.push_back(ValueId::makeFromDouble(min));
  }
  randomShuffle(ids.begin(), ids.end());
  return ids;
};
inline auto makeRandomIds = []() {
  std::vector<ValueId> ids = makeRandomDoubleIds();
  addIdsFromGenerator(indexGenerator, &makeVocabId, ids);
  addIdsFromGenerator(indexGenerator, &makeLocalVocabId, ids);
  addIdsFromGenerator(indexGenerator, &makeTextRecordId, ids);
  addIdsFromGenerator(indexGenerator, &makeWordVocabId, ids);
  addIdsFromGenerator(nonOverflowingNBitGenerator, &ValueId::makeFromInt, ids);
  addIdsFromGenerator(overflowingNBitGenerator, &ValueId::makeFromInt, ids);
  addIdsFromGenerator(underflowingNBitGenerator, &ValueId::makeFromInt, ids);

  for (size_t i = 0; i < numElements; ++i) {
    ids.push_back(ValueId::makeUndefined());
  }

  randomShuffle(ids.begin(), ids.end());
  return ids;
};

#endif  // QLEVER_VALUEIDTESTHELPERS_H
