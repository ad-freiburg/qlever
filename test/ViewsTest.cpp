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
    EXPECT_THAT(result, ::testing::ContainerEq(inputVector));
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

TEST(Views, uniqueView) {
  std::vector<int> ints;
  const uint64_t numInts = 50'000;
  ints.reserve(numInts);
  ad_utility::SlowRandomIntGenerator<int> r;
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
  // Erase "accidental" duplicates from the random initialization.
  auto it = std::unique(ints.begin(), ints.end());
  ints.erase(it, ints.end());
  ASSERT_EQ(ints.size(), result.size());
  ASSERT_EQ(ints, result);
}

TEST(Views, uniqueBlockView) {
  const uint64_t numInts = 50'000;
  std::vector<int> ints;

  ad_utility::SlowRandomIntGenerator<int> r(0, 1000);
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

  ad_utility::SlowRandomIntGenerator<int> vecSizeGenerator(2, 200);

  size_t i = 0;
  std::vector<std::vector<int>> inputs;
  while (i < intsWithDuplicates.size()) {
    auto vecSize = vecSizeGenerator();
    size_t nextI = std::min(i + vecSize, intsWithDuplicates.size());
    inputs.emplace_back(intsWithDuplicates.begin() + i,
                        intsWithDuplicates.begin() + nextI);
    i = nextI;
  }

  auto unique = ql::views::join(
      ad_utility::OwningView{ad_utility::uniqueBlockView(inputs)});
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
  static_assert(ql::ranges::input_range<OwningView<cppcoro::generator<int>>>);
  static_assert(
      !ql::ranges::forward_range<OwningView<cppcoro::generator<int>>>);
  static_assert(ql::ranges::random_access_range<OwningView<std::vector<int>>>);

  auto toVec = [](auto& range) {
    std::vector<std::string> result;
    ql::ranges::copy(range, std::back_inserter(result));
    return result;
  };

  // check the functionality and the ownership.
  auto vecView = OwningView{std::vector<std::string>{
      "4", "fourhundredseventythousandBlimbambum", "3", "1"}};
  ASSERT_THAT(toVec(vecView),
              ::testing::ElementsAre(
                  "4", "fourhundredseventythousandBlimbambum", "3", "1"));

  ASSERT_THAT(toVec(std::as_const(vecView)),
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
  ql::ranges::copy(ad_utility::integerRange(42u), std::back_inserter(actual));
  ASSERT_THAT(actual, ::testing::ElementsAreArray(expected));
}

// __________________________________________________________________________
TEST(Views, inPlaceTransform) {
  std::vector v{0, 1, 2, 3, 4, 5};
  auto twice = [](int& i) { i *= 2; };
  auto transformed = ad_utility::inPlaceTransformView(v, twice);
  std::vector<int> res1;
  std::vector<int> res2;
  std::vector<int> res3;
  for (auto it = transformed.begin(); it != transformed.end(); ++it) {
    res1.push_back(*it);
    res2.push_back(*it);
    res3.push_back(*it);
  }

  EXPECT_THAT(res1, ::testing::ElementsAre(0, 2, 4, 6, 8, 10));
  // The original range was also modified.
  EXPECT_THAT(v, ::testing::ElementsAre(0, 2, 4, 6, 8, 10));

  EXPECT_THAT(res2, ::testing::ElementsAreArray(res1));
  EXPECT_THAT(res3, ::testing::ElementsAreArray(res1));
}

// __________________________________________________________________________

std::string_view toView(std::span<char> span) {
  return {span.data(), span.size()};
}

// __________________________________________________________________________
TEST(Views, verifyLineByLineWorksWithMinimalChunks) {
  auto range =
      std::string_view{"\nabc\ndefghij\n"} |
      ql::views::transform([](char c) { return ql::ranges::single_view(c); });
  auto lineByLineGenerator =
      ad_utility::reChunkAtSeparator(std::move(range), '\n');

  auto iterator = lineByLineGenerator.begin();
  ASSERT_NE(iterator, lineByLineGenerator.end());
  EXPECT_EQ(toView(*iterator), "");

  ++iterator;
  ASSERT_NE(iterator, lineByLineGenerator.end());
  EXPECT_EQ(toView(*iterator), "abc");

  ++iterator;
  ASSERT_NE(iterator, lineByLineGenerator.end());
  EXPECT_EQ(toView(*iterator), "defghij");

  ++iterator;
  ASSERT_EQ(iterator, lineByLineGenerator.end());
}

// __________________________________________________________________________
TEST(Views, verifyLineByLineWorksWithNoTrailingNewline) {
  auto range = std::string_view{"abc"} | ql::views::transform([](char c) {
                 return ql::ranges::single_view(c);
               });

  auto lineByLineGenerator =
      ad_utility::reChunkAtSeparator(std::move(range), '\n');

  auto iterator = lineByLineGenerator.begin();
  ASSERT_NE(iterator, lineByLineGenerator.end());
  EXPECT_EQ(toView(*iterator), "abc");

  ++iterator;
  ASSERT_EQ(iterator, lineByLineGenerator.end());
}

// __________________________________________________________________________
TEST(Views, verifyLineByLineWorksWithChunksBiggerThanLines) {
  using namespace std::string_view_literals;

  auto lineByLineGenerator = ad_utility::reChunkAtSeparator(
      std::vector{"\nabc\nd"sv, "efghij"sv, "\n"sv}, '\n');

  auto iterator = lineByLineGenerator.begin();
  ASSERT_NE(iterator, lineByLineGenerator.end());
  EXPECT_EQ(toView(*iterator), "");

  ++iterator;
  ASSERT_NE(iterator, lineByLineGenerator.end());
  EXPECT_EQ(toView(*iterator), "abc");

  ++iterator;
  ASSERT_NE(iterator, lineByLineGenerator.end());
  EXPECT_EQ(toView(*iterator), "defghij");

  ++iterator;
  ASSERT_EQ(iterator, lineByLineGenerator.end());
}

// Tests for ad_utility::CachingTransformInputRange
TEST(Views, CachingTransformInputRange) {
  using V = std::vector<std::vector<int>>;
  std::vector<std::vector<int>> input{{1, 2}, {3, 4}};
  const std::vector<std::vector<int>> inputCopy = input;
  const std::vector<std::vector<int>> expected{{3, 2}, {5, 4}};
  using namespace ad_utility;

  // This function will move the vector if possible (i.e. if it is not const)
  // and then increment the first element by `2`.
  auto firstPlusTwo = [](auto& vec) {
    auto copy = std::move(vec);
    copy.at(0) += 2;
    return copy;
  };

  // Store all the elements of the `view` in a vector for easier testing.
  auto toVec = [](auto&& view) {
    V res;
    for (auto& v : view) {
      res.push_back(std::move(v));
    }
    return res;
  };

  // As we pass in the input as a const value, it cannot be moved out.
  {
    auto view = CachingTransformInputRange(std::as_const(input), firstPlusTwo);
    auto res = toVec(view);
    EXPECT_EQ(res, expected);
    EXPECT_EQ(input, inputCopy);
  }

  // As the `input` is not const, its individual elements are going to be moved.
  {
    auto view = CachingTransformInputRange(input, firstPlusTwo);
    auto res = toVec(view);
    EXPECT_EQ(res, expected);
    V elementsMoved{{}, {}};
    EXPECT_EQ(input.size(), 2);
    input = inputCopy;
  }

  // We move the input, so it is of course being moved.
  {
    auto view = CachingTransformInputRange(std::move(input), firstPlusTwo);
    auto res = toVec(view);
    EXPECT_EQ(res, expected);
    EXPECT_TRUE(input.empty());
    input = inputCopy;
  }
}
TEST(Views, CachingContinuableTransformInputRange) {
  // This function will move the vector if possible (i.e. if it is not const)
  // and then increment the first element by `2`.
  using namespace ad_utility;
  using namespace loopControl;
  using L = LoopControl<std::vector<int>>;
  auto firstPlusTwo = [](auto& vec) -> L {
    if (vec.at(1) % 2 == 1) {
      return L{Continue{}};
    }
    if (vec.at(1) > 5) {
      return L{Break{}};
    }
    auto copy = std::move(vec);
    copy.at(0) += 2;
    return loopControl::LoopControl<std::vector<int>>{std::move(copy)};
  };

  using V = std::vector<std::vector<int>>;
  V input{{1, 2}, {1, 3}, {3, 4}, {3, 9}};
  const V inputCopy = input;
  const V expected{{3, 2}, {5, 4}};
  const V elementwiseMovedFrom{{}, {1, 3}, {}, {3, 9}};

  // Store all the elements of the `view` in a vector for easier testing.
  auto toVec = [](auto&& view) {
    V res;
    for (auto& v : view) {
      res.push_back(std::move(v));
    }
    return res;
  };

  // As we pass in the input as a const value, it cannot be moved out.
  {
    auto view = CachingContinuableTransformInputRange(std::as_const(input),
                                                      firstPlusTwo);
    auto res = toVec(view);
    EXPECT_EQ(res, expected);
    EXPECT_EQ(input, inputCopy);
  }

  // As the `input` is not const, its individual elements are going to be moved.
  {
    auto view = CachingContinuableTransformInputRange(input, firstPlusTwo);
    auto res = toVec(view);
    EXPECT_EQ(res, expected);
    EXPECT_EQ(input, elementwiseMovedFrom);
    input = inputCopy;
  }

  // We move the input, so it is of course being moved.
  {
    auto view =
        CachingContinuableTransformInputRange(std::move(input), firstPlusTwo);
    auto res = toVec(view);
    EXPECT_EQ(res, expected);
    EXPECT_TRUE(input.empty());
    input = inputCopy;
  }
}
