//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "../src/util/Random.h"
#include "../src/util/Views.h"

TEST(Views, BufferedAsyncView) {
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
  std::vector<int> ints;
  const uint64_t numInts = 50'000;
  ints.reserve(numInts);
  SlowRandomIntGenerator<int> r;
  for (size_t i = 0; i < numInts; ++i) {
    ints.push_back(r());
  }

  std::vector<int> intsWithDuplicates;
  intsWithDuplicates.reserve(3 * numInts);
  for (size_t i = 0; i < 3; ++i) {
    for (auto num : ints) {
      intsWithDuplicates.push_back(num);
    }
  }

  std::sort(intsWithDuplicates.begin(), intsWithDuplicates.end());
  auto unique = ad_utility::uniqueView(intsWithDuplicates);
  std::vector<int> result;
  for (const auto& element : unique) {
    result.push_back(element);
  }
  std::sort(ints.begin(), ints.end());
  // Erase "accidentally" unique duplicates from the random initialization.
  auto it = std::unique(ints.begin(), ints.end());
  ints.erase(it, ints.end());
  ASSERT_EQ(ints.size(), result.size());
  ASSERT_EQ(ints, result);
}

TEST(Views, owningView) {
  using namespace ad_utility;
  // Static asserts for the desired concepts.
  static_assert(std::ranges::input_range<OwningView<cppcoro::generator<int>>>);
  static_assert(
      !std::ranges::forward_range<OwningView<cppcoro::generator<int>>>);
  static_assert(std::ranges::random_access_range<OwningView<std::vector<int>>>);

  auto toVec = [](auto& range) {
    std::vector<std::string> result;
    std::ranges::copy(range, std::back_inserter(result));
    return result;
  };

  // check the functionality and the ownership.
  auto vecView = OwningView{std::vector<std::string>{
      "4", "fourhundredseventythousandBlimbambum", "3", "1"}};
  ASSERT_THAT(toVec(vecView),
              ::testing::ElementsAre(
                  "4", "fourhundredseventythousandBlimbambum", "3", "1"));

  auto generator = []() -> cppcoro::generator<std::string> {
    co_yield "4";
    co_yield "fourhundredseventythousandBlimbambum";
    co_yield "3";
    co_yield "1";
  };

  auto genView = OwningView{generator()};
  ASSERT_THAT(toVec(genView),
              ::testing::ElementsAre(
                  "4", "fourhundredseventythousandBlimbambum", "3", "1"));
}

TEST(Views, integerRange) {
  std::vector<size_t> expected;
  for (size_t i = 0; i < 42; ++i) {
    expected.push_back(i);
  }

  std::vector<size_t> actual;
  std::ranges::copy(ad_utility::integerRange(42u), std::back_inserter(actual));
  ASSERT_THAT(actual, ::testing::ElementsAreArray(expected));
}
