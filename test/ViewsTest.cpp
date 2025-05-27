//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "util/InputRangeUtils.h"
#include "util/Random.h"
#include "util/ValueIdentity.h"
#include "util/Views.h"

TEST(Views, BufferedAsyncView) {
  auto testWithVector = [](const auto& inputVector) {
    using T = std::decay_t<decltype(inputVector)>;
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

std::string_view toView(ql::span<char> span) {
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

// _____________________________________________________________________________
TEST(Views, RvalueView) {
  // A simple struct that knows if it has been moved from.
  struct MoveTracker {
    bool wasMoved_ = false;
    MoveTracker() = default;
    MoveTracker(const MoveTracker&) = default;
    MoveTracker& operator=(const MoveTracker&) = default;
    MoveTracker(MoveTracker&& rhs)
        : wasMoved_{std::exchange(rhs.wasMoved_, true)} {}
    MoveTracker operator=(MoveTracker&& rhs) {
      wasMoved_ = std::exchange(rhs.wasMoved_, true);
      return *this;
    }
  };

  // This impl tests the different ways an `RvalueView` can be created:
  // Either from a const or mutable input (first argument of type
  // `ValueIdentity<bool>`, And the view is either copied or moved into the
  // place where its used (second argument).
  auto testImpl = [](auto isConst, bool doMove) {
    std::vector<MoveTracker> vec;
    vec.resize(13);

    std::vector<MoveTracker> target;
    // Move the first 5 elements via an `RvalueView` + ql::ranges::copy.
    auto getView = [&]() {
      if constexpr (isConst) {
        return ad_utility::RvalueView{std::as_const(vec)};
      } else {
        return ad_utility::RvalueView{vec};
      }
    };
    static_assert(ql::ranges::random_access_range<
                  std::invoke_result_t<decltype(getView)>>);
    if (doMove) {
      ql::ranges::copy(getView() | ql::views::take(5),
                       std::back_inserter(target));
    } else {
      auto view = getView();
      ASSERT_EQ(view.size(), 13);
      ql::ranges::copy(view | ql::views::take(5), std::back_inserter(target));
    }
    ASSERT_EQ(target.size(), 5);
    for (size_t i = 0; i < 5; ++i) {
      ASSERT_NE(vec[i].wasMoved_, isConst);
    }
    for (size_t i = 5; i < vec.size(); ++i) {
      ASSERT_FALSE(vec[i].wasMoved_);
    }
    for (auto& el : target) {
      ASSERT_FALSE(el.wasMoved_);
    }
  };

  using namespace ad_utility::use_value_identity;
  testImpl(vi<true>, true);
  testImpl(vi<true>, false);
  testImpl(vi<false>, false);
  testImpl(vi<false>, true);
}

// _____________________________________________________________________________
TEST(Views, ForceInputView) {
  using ad_utility::ForceInputView;
  std::vector<int> vec{1, 2, 3};
  auto view = ForceInputView{vec};
  using V = decltype(view);
  static_assert(ql::ranges::view<V>);
  static_assert(ql::ranges::input_range<V>);
  static_assert(!ql::ranges::forward_range<V>);
  std::vector<int> res;
  ql::ranges::copy(view, std::back_inserter(res));
  EXPECT_THAT(res, ::testing::ElementsAre(1, 2, 3));
  // `begin` has already been called via the `ranges::copy` above, so additional
  // iterations should throw.
  EXPECT_ANY_THROW(view.begin());
}
