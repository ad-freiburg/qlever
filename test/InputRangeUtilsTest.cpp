//  Copyright 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "util/InputRangeUtils.h"

namespace {
// Store all the elements of the `view` in a vector for easier testing.
constexpr auto toVec = [](auto&& view) {
  std::vector<ql::ranges::range_value_t<std::decay_t<decltype(view)>>> res;
  for (auto& v : view) {
    res.push_back(std::move(v));
  }
  return res;
};

// The input and expected output of a run of a transforming view.
// `elementwiseMoved_` is the expected state of the input after running a
// transforming view that moves all the elements it actually touches (e.g.
// because they are not filtered out).
template <typename T>
struct TransformViewTestHelpers {
  using V = std::vector<T>;
  V input_;
  V expected_;
  V elementwiseMoved_;
};

// Run all the tests for a given transfomr view that takes and returns elements
// of type `T` which are transformed using the `function` .
template <template <typename...> typename ViewTemplate, typename T, typename F>
void testTransformView(const TransformViewTestHelpers<T>& inAndOutputs,
                       F function) {
  {
    auto c = inAndOutputs;
    // As we pass in the input as a const value, it cannot be moved out.
    auto view = ViewTemplate(std::as_const(c.input_), function);
    auto res = toVec(view);
    EXPECT_EQ(res, c.expected_);
    EXPECT_EQ(c.input_, inAndOutputs.input_);
  }

  {
    // As the `input` is not const, its individual elements are going to be
    // moved.
    auto c = inAndOutputs;
    auto view = ViewTemplate(c.input_, function);
    auto res = toVec(view);
    EXPECT_EQ(res, c.expected_);
    EXPECT_EQ(c.input_, c.elementwiseMoved_);
  }

  {
    // We move the input, so it is completely empty after the call.
    auto c = inAndOutputs;
    auto view = ViewTemplate(std::move(c.input_), function);
    auto res = toVec(view);
    EXPECT_EQ(res, c.expected_);
    EXPECT_TRUE(c.input_.empty());
  }
}

// Tests for ad_utility::lazySingleValueRange
TEST(lazySingleValueRange, ReturnLazySingleValueRange) {
  int variable = 0;
  auto transformation = [&variable]() {
    variable++;
    return variable + 1;
  };

  auto range = ad_utility::lazySingleValueRange(std::move(transformation));
  // initial variable should be unchanged
  EXPECT_EQ(variable, 0);
  std::optional<int> element = range.get();

  // The first call to `get()` should return a value.
  EXPECT_TRUE(element.has_value());
  EXPECT_EQ(element.value(), 2);
  EXPECT_EQ(variable, 1);

  // No more values after the first.
  EXPECT_FALSE(range.get().has_value());
  EXPECT_EQ(variable, 1);
}

// Tests for ad_utility::CachingTransformInputRange
TEST(CachingTransformInputRange, BasicTests) {
  // This function will move the `vec` if possible (i.e. if it is not const)
  // and then increment the first element by `2`.
  auto firstPlusTwo = [](auto& vec) {
    auto copy = std::move(vec);
    copy.at(0) += 2;
    return copy;
  };

  TransformViewTestHelpers<std::vector<int>> helpers;
  helpers.input_ = {{1, 2}, {3, 4}};
  helpers.expected_ = {{3, 2}, {5, 4}};
  helpers.elementwiseMoved_ = {{}, {}};
  testTransformView<ad_utility::CachingTransformInputRange>(helpers,
                                                            firstPlusTwo);
}

// Test for iterating past the end of a CachingTransformInput range
TEST(CachingTransformInputRange, IteratePastEnd) {
  // This test ensures that nullopt is returned repeatedly after all elements
  // have already been iterated
  std::vector<int> view{42};
  auto simpleMove = [](auto& p) { return std::move(p); };
  ad_utility::CachingTransformInputRange range{view, simpleMove};
  std::optional<int> element = range.get();

  // The first element shall be returned
  ASSERT_TRUE(element.has_value());
  EXPECT_EQ(42, element.value());

  // Subsequent calls shall return nullopt
  EXPECT_FALSE(range.get().has_value());
  EXPECT_FALSE(range.get().has_value());
  EXPECT_FALSE(range.get().has_value());
}

// Test for the combination of `CachingTransformInputRagne` with the associated
// `Details`.
TEST(CachingTransformInputRange, Details) {
  struct Details {
    int value = 42;
  };

  struct Gen : public ad_utility::InputRangeFromGet<int, Details> {
    int i = 0;
    std::optional<int> get() override {
      auto v = i++;
      if (v >= 5) {
        return std::nullopt;
      }
      details().value = v * 2;
      return v;
    }
  };

  auto timesTwo = [](auto el) { return el * 2; };
  auto r = ad_utility::CachingTransformInputRange<ad_utility::OwningView<Gen>,
                                                  decltype(timesTwo), Details>(
      Gen{}, timesTwo);
  for (auto [a, i] : ::ranges::views::enumerate(r)) {
    EXPECT_EQ(i, r.details().value);
    EXPECT_EQ(i, a * 2);
  }
}

// Tests for the generator with additional control flow.
TEST(CachingContinuableTransformInputRange, BreakAndContinue) {
  // This function will move the vector if possible (i.e. if it is not const)
  // and then increment the first element by `2`.
  using namespace ad_utility;
  using L = LoopControl<std::vector<int>>;
  auto firstPlusTwo = [](auto& vec) -> L {
    if (vec.at(1) % 2 == 1) {
      return L::makeContinue();
    }
    if (vec.at(1) > 5) {
      return L::makeBreak();
    }
    auto copy = std::move(vec);
    copy.at(0) += 2;
    return L::yieldValue(std::move(copy));
  };

  TransformViewTestHelpers<std::vector<int>> helpers;
  helpers.input_ = {{1, 2}, {1, 3}, {3, 4}, {3, 8}, {1, 2}};
  helpers.expected_ = {{3, 2}, {5, 4}};
  helpers.elementwiseMoved_ = {{}, {1, 3}, {}, {3, 8}, {1, 2}};
  testTransformView<ad_utility::CachingContinuableTransformInputRange>(
      helpers, firstPlusTwo);
}

// Same as the tests above, but check the `breakWithValue`.
TEST(CachingContinuableTransformInputRange, BreakWithValue) {
  // This function will move the vector if possible (i.e. if it is not const)
  // and then increment the first element by `2`.
  using namespace ad_utility;
  using L = LoopControl<std::vector<int>>;
  auto firstPlusTwo = [](auto& vec) -> L {
    if (vec.at(1) % 2 == 1) {
      return L::makeContinue();
    }
    auto copy = std::move(vec);
    copy.at(0) += 2;
    if (copy.at(1) > 5) {
      return L::breakWithValue(std::move(copy));
    }
    return L::yieldValue(std::move(copy));
  };

  TransformViewTestHelpers<std::vector<int>> helpers;
  helpers.input_ = {{1, 2}, {1, 3}, {3, 4}, {3, 8}, {1, 2}};
  helpers.expected_ = {{3, 2}, {5, 4}, {5, 8}};
  helpers.elementwiseMoved_ = {{}, {1, 3}, {}, {}, {1, 2}};
  testTransformView<ad_utility::CachingContinuableTransformInputRange>(
      helpers, firstPlusTwo);
}

// _____________________________________________________________________________
TEST(CachingContinuableTransformInputRange, yieldAll) {
  // This function will move the vector if possible (i.e. if it is not const)
  // and then increment the first element by `2`. All but the last input
  // will be yielded twice to test the `yieldAll` facility.
  using namespace ad_utility;
  using L = LoopControl<std::vector<int>>;
  auto firstPlusTwo = [](auto& vec) -> L {
    if (vec.at(1) % 2 == 1) {
      return L::makeContinue();
    }
    auto copy = std::move(vec);
    copy.at(0) += 2;
    if (copy.at(1) > 5) {
      return L::breakWithValue(std::move(copy));
    }
    std::array arr{copy, copy};
    return L::yieldAll(std::move(arr));
  };

  TransformViewTestHelpers<std::vector<int>> helpers;
  helpers.input_ = {{1, 2}, {1, 3}, {3, 4}, {3, 8}, {1, 2}};
  helpers.expected_ = {{3, 2}, {3, 2}, {5, 4}, {5, 4}, {5, 8}};
  helpers.elementwiseMoved_ = {{}, {1, 3}, {}, {}, {1, 2}};
  testTransformView<ad_utility::CachingContinuableTransformInputRange>(
      helpers, firstPlusTwo);
}

// Same as the tests above, but check the `breakWithValue`.
TEST(CachingContinuableTransformInputRange, NoBreak) {
  // This function will move the vector if possible (i.e. if it is not const)
  // and then increment the first element by `2`. Never break.
  using namespace ad_utility;
  using L = LoopControl<std::vector<int>>;
  auto firstPlusTwo = [](auto& vec) -> L {
    if (vec.at(1) % 2 == 1) {
      return L::makeContinue();
    }
    auto copy = std::move(vec);
    copy.at(0) += 2;
    return L::yieldValue(std::move(copy));
  };

  TransformViewTestHelpers<std::vector<int>> helpers;
  helpers.input_ = {{1, 2}, {1, 3}, {3, 4}, {3, 8}, {1, 2}};
  helpers.expected_ = {{3, 2}, {5, 4}, {5, 8}, {3, 2}};
  helpers.elementwiseMoved_ = {{}, {1, 3}, {}, {}, {}};
  testTransformView<ad_utility::CachingContinuableTransformInputRange>(
      helpers, firstPlusTwo);
}

// This is an example on how a stateful functor can be used to implement more
// complex control flows.
TEST(CachingContinuableTransformInputRange, StatefulFunctor) {
  // This function will move the vector if possible (i.e. if it is not const)
  // and then increment the first element by `2`. Never break.
  using namespace ad_utility;
  using L = LoopControl<std::vector<int>>;

  // Pass the vectors on unchanged in principle, but
  // skip the first 3 vector elements (not vectors!), and after that
  // yield only 4 additional elements.
  // Semantically this is similar to `input | views::join | views::drop(3) |
  // views::take(4)`, but keeps the structure of the original batches intact
  // (aside from the first and last one which have to be truncated.
  auto applyLimit4Offset3 = [limit = size_t{4},
                             offset = size_t{3}](auto& vec) mutable -> L {
    if (limit == 0) {
      return L::makeBreak();
    }
    if (vec.size() <= offset) {
      offset -= vec.size();
      return L::makeContinue();
    }
    auto copy = std::move(vec);
    if (offset > 0) {
      copy.erase(copy.begin(), copy.begin() + offset);
      offset = 0;
    }
    copy.resize(std::min(copy.size(), limit));
    limit -= copy.size();
    return L::yieldValue(std::move(copy));
  };

  TransformViewTestHelpers<std::vector<int>> helpers;
  helpers.input_ = {{1, 2}, {3, 4}, {5, 6}, {7, 8}, {9, 10}};
  helpers.expected_ = {{4}, {5, 6}, {7}};
  helpers.elementwiseMoved_ = {{1, 2}, {}, {}, {}, {9, 10}};
  testTransformView<ad_utility::CachingContinuableTransformInputRange>(
      helpers, applyLimit4Offset3);
}

// Tests for `InputRangeFromLoopControlGet`.
TEST(InputRangeFromLoopControlGet, BasicTests) {
  using namespace ad_utility;
  using namespace testing;
  using L = LoopControl<int>;
  auto f = [i = 0]() mutable -> L {
    auto val = i++;
    if (val == 0) {
      return L::yieldValue(0);
    }
    if (val == 1) {
      return L::makeContinue();
    }
    return L::breakWithValue(42);
  };

  EXPECT_THAT(toVec(InputRangeFromLoopControlGet(f)), ElementsAre(0, 42));

  // Also add a test with a simple break;
  auto f2 = [i = 0]() mutable -> L {
    auto val = i++;
    if (val == 0) {
      return L::yieldValue(0);
    }
    if (val == 1) {
      return L::yieldValue(42);
    }
    if (val < 37) {
      return L::makeContinue();
    }
    if (val == 38) {
      return L::yieldValue(123);
    }
    if (val == 42) {
      return L::yieldAll(std::array{13, 18});
    }

    if (val == 45) {
      return L::yieldValue(9);
    }

    if (val < 47) {
      return L::makeContinue();
    }
    return L::makeBreak();
  };
  EXPECT_THAT(toVec(InputRangeFromLoopControlGet(f2)),
              ElementsAre(0, 42, 123, 13, 18, 9));
}

// ________________________________________________________________
TEST(InputRangeFromLoopControlGet, BreakWithYieldAll) {
  using namespace ad_utility;
  using namespace testing;
  using L = LoopControl<int>;

  // Test BreakWithYieldAll: should yield all values from range and then break
  auto f = [i = 0]() mutable -> L {
    auto val = i++;
    if (val == 0) {
      return L::yieldValue(10);
    }
    if (val == 1) {
      return L::yieldValue(20);
    }
    if (val == 2) {
      // This should yield all values from the array and then break
      // without calling the function again
      return L::breakWithYieldAll(std::array{100, 200, 300});
    }
    // This should never be reached because BreakWithYieldAll should
    // prevent further function calls
    return L::yieldValue(999);
  };

  EXPECT_THAT(toVec(InputRangeFromLoopControlGet(f)),
              ElementsAre(10, 20, 100, 200, 300));
}
}  // namespace
