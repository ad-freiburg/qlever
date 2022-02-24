//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "../src/util/Random.h"
#include "../src/util/Views.h"

TEST(Views, bufferedAsyncView) {
  auto testWithVector = []<typename T>(const T& inputVector) {
    auto view = ad_utility::bufferedAsyncView(inputVector, 100);
    T result;
    result.reserve(inputVector.size());
    for (const auto& element : view) {
      result.push_back(element);
    }
    ASSERT_EQ(result, inputVector);
  };

  uint64_t numElements = 1000;
  const std::vector<uint64_t> ints = [numElements]() {
    std::vector<uint64_t> ints;
    ints.reserve(numElements);
    for (uint64_t i = 0; i < numElements; ++i) {
      ints.push_back(numElements - i);
    }
    return ints;
  }();

  const std::vector<std::string> strings = [numElements]() {
    std::vector<std::string> strings;
    strings.reserve(numElements);
    for (uint64_t i = 0; i < numElements; ++i) {
      strings.push_back(std::to_string(numElements - i));
    }
    return strings;
  }();

  testWithVector(ints);
  testWithVector(strings);
}

TEST(Views, UniqueView) {
  std::vector<int> originalInts;
  const uint64_t numInts = 50'000;
  originalInts.reserve(numInts);
  SlowRandomIntGenerator<int> r{-200'000, 200'000};
  for (size_t i = 0; i < numInts; ++i) {
    originalInts.push_back(r());
  }

  std::vector<int> duplicateInts;
  duplicateInts.reserve(3 * numInts);
  for (size_t i = 0; i < 3; ++i) {
    for (auto num : originalInts) {
      duplicateInts.push_back(num);
    }
  }

  std::sort(duplicateInts.begin(), duplicateInts.end());
  auto unique = ad_utility::uniqueView(duplicateInts);
  std::vector<int> result;
  for (const auto& element : unique) {
    result.push_back(element);
  }
  std::sort(originalInts.begin(), originalInts.end());
  // Erase "accidentally" unique duplicates from the random initialization.
  auto it = std::unique(originalInts.begin(), originalInts.end());
  originalInts.erase(it, originalInts.end());
  ASSERT_EQ(originalInts.size(), result.size());
  ASSERT_EQ(originalInts, result);
}
