// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <concepts>
#include <ranges>
#include <type_traits>
#include <vector>

#include "util/views/TakeUntilInclusiveView.h"

using namespace ad_utility;

namespace {

// Helper predicate to test with
auto isEven = [](int x) { return x % 2 == 0; };
auto isOdd = [](int x) { return x % 2 == 1; };
auto isGreaterThan5 = [](int x) { return x > 5; };
auto alwaysTrue = [](auto) { return true; };
auto alwaysFalseLocal = [](auto) { return false; };

// Test concept compliance
template <typename T>
concept IsView = ql::ranges::view<T>;

template <typename T>
concept IsInputRange = ql::ranges::input_range<T>;

template <typename T>
concept IsForwardRange = ql::ranges::forward_range<T>;

template <typename T>
concept IsBidirectionalRange = ql::ranges::bidirectional_range<T>;

template <typename T>
concept IsRandomAccessRange = ql::ranges::random_access_range<T>;

// Helper function to test TakeUntilInclusiveView with a vector<int> and
// predicate
template <typename Predicate>
void testTakeUntilView(const std::vector<int>& inputVector,
                       const std::vector<int>& expectedResult,
                       Predicate predicate) {
  auto view = TakeUntilInclusiveView{inputVector, predicate};

  std::vector<int> result;
  for (auto&& element : view) {
    result.push_back(element);
  }

  EXPECT_THAT(result, ::testing::ElementsAreArray(expectedResult));
}

}  // namespace

// Test basic functionality
TEST(TakeUntilInclusiveViewTest, BasicFunctionality) {
  testTakeUntilView({0, 2, 4, 3, 5, 6}, {0, 2, 4, 3}, isOdd);
}

// Test with empty range
TEST(TakeUntilInclusiveViewTest, EmptyRange) {
  testTakeUntilView({}, {}, isOdd);
}

// Test when no element satisfies predicate
TEST(TakeUntilInclusiveViewTest, NoElementSatisfiesPredicate) {
  testTakeUntilView({0, 2, 4, 6, 8}, {0, 2, 4, 6, 8}, isOdd);
}

// Test when first element satisfies predicate
TEST(TakeUntilInclusiveViewTest, FirstElementSatisfiesPredicate) {
  testTakeUntilView({1, 2, 4, 6, 8}, {1}, isOdd);
}

// Test when all elements satisfy predicate
TEST(TakeUntilInclusiveViewTest, AllElementsSatisfyPredicate) {
  testTakeUntilView({1, 3, 5, 7, 9}, {1}, alwaysTrue);
}

// Test when no element satisfies predicate (different predicate)
TEST(TakeUntilInclusiveViewTest, NoElementSatisfiesAlwaysFalse) {
  testTakeUntilView({1, 3, 5, 7, 9}, {1, 3, 5, 7, 9}, alwaysFalseLocal);
}

// Test single element range - predicate true
TEST(TakeUntilInclusiveViewTest, SingleElementPredicateTrue) {
  testTakeUntilView({5}, {5}, isOdd);
}

// Test single element range - predicate false
TEST(TakeUntilInclusiveViewTest, SingleElementPredicateFalse) {
  testTakeUntilView({4}, {4}, isOdd);
}

// Test iterator equality with sentinel
TEST(TakeUntilInclusiveViewTest, IteratorSentinelEquality) {
  std::vector<int> data{0, 2, 4, 3, 5, 6};
  auto view = TakeUntilInclusiveView{data, isOdd};

  auto it = view.begin();
  auto end = view.end();

  // Should not be equal initially
  EXPECT_FALSE(it == end);

  // Iterate to the end
  ++it;  // 0
  EXPECT_FALSE(it == end);
  ++it;  // 2
  EXPECT_FALSE(it == end);
  ++it;  // 4
  EXPECT_FALSE(it == end);
  ++it;  // 3 (satisfies predicate, should be the last)
  EXPECT_TRUE(it == end);
}

// Test iterator dereferencing and operator*
TEST(TakeUntilInclusiveViewTest, IteratorDereferencing) {
  std::vector<int> data{10, 20, 30};
  auto view = TakeUntilInclusiveView{data, isGreaterThan5};

  auto it = view.begin();
  EXPECT_EQ(*it, 10);

  // Check that operator* can be called multiple times
  EXPECT_EQ(*it, 10);
  EXPECT_EQ(*it, 10);
}

// Test post-increment operator
TEST(TakeUntilInclusiveViewTest, PostIncrementOperator) {
  std::vector<int> data{1, 2, 3};
  auto view = TakeUntilInclusiveView{std::move(data), isEven};

  auto it = view.begin();
  EXPECT_EQ(*it, 1);

  it++;  // Post-increment returns void
  EXPECT_EQ(*it, 2);
}

// Test with different predicate types
TEST(TakeUntilInclusiveViewTest, DifferentPredicateTypes) {
  // Lambda
  testTakeUntilView({1, 2, 3, 4, 5}, {1, 2, 3}, [](int x) { return x == 3; });

  // Function pointer
  auto func_ptr = +[](int x) { return x == 3; };
  testTakeUntilView({1, 2, 3, 4, 5}, {1, 2, 3}, func_ptr);
}

// Test concept compliance
TEST(TakeUntilInclusiveViewTest, ConceptCompliance) {
  std::vector<int> data{1, 2, 3, 4, 5};
  auto view = TakeUntilInclusiveView{data, isOdd};
  using ViewType = decltype(view);

  // Should satisfy view and input_range concepts
  EXPECT_TRUE(IsView<ViewType>);
  EXPECT_TRUE(IsInputRange<ViewType>);

  // Should NOT satisfy stronger range concepts
  EXPECT_FALSE(IsForwardRange<ViewType>);
  EXPECT_FALSE(IsBidirectionalRange<ViewType>);
  EXPECT_FALSE(IsRandomAccessRange<ViewType>);
}

// Test range adaptor object pipeability
TEST(TakeUntilInclusiveViewTest, RangeAdaptorPipeability) {
  std::vector<int> data{0, 2, 4, 3, 5, 6};

  // Test with the adaptor object
  auto result1 = data | views::takeUntilInclusive(isOdd);
  std::vector<int> vec1;
  for (auto&& element : result1) {
    vec1.push_back(element);
  }
  EXPECT_THAT(vec1, ::testing::ElementsAre(0, 2, 4, 3));

  // Test simpler combination with transform
  std::vector<int> simple_data{1, 2, 3};
  auto result2 = simple_data |
                 ql::views::transform([](int x) { return x + 10; }) |
                 views::takeUntilInclusive([](int x) { return x == 12; });

  std::vector<int> vec2;
  for (auto&& element : result2) {
    vec2.push_back(element);
  }
  EXPECT_THAT(vec2,
              ::testing::ElementsAre(11, 12));  // 1+10, 2+10 until 2+10=12
}

// Test that calling views::takeUntilInclusive directly works
TEST(TakeUntilInclusiveViewTest, DirectAdaptorCall) {
  std::vector<int> data{1, 3, 5, 2, 4, 6};

  auto result = views::takeUntilInclusive(data, isEven);
  std::vector<int> vec;
  for (auto&& element : result) {
    vec.push_back(element);
  }
  EXPECT_THAT(vec, ::testing::ElementsAre(1, 3, 5, 2));
}

// Test deduction guides
TEST(TakeUntilInclusiveViewTest, DeductionGuides) {
  testTakeUntilView({1, 2, 3, 4, 5}, {1, 2}, isEven);
}

// Test with different underlying ranges
TEST(TakeUntilInclusiveViewTest, DifferentUnderlyingRanges) {
  // Test with array
  int arr[] = {1, 3, 5, 2, 4, 6};
  auto arr_view = TakeUntilInclusiveView{arr, isEven};
  std::vector<int> arr_result;
  for (auto&& element : arr_view) {
    arr_result.push_back(element);
  }
  EXPECT_THAT(arr_result, ::testing::ElementsAre(1, 3, 5, 2));

  // Test with simple vector (avoiding potential iota issues)
  std::vector<int> range_data{1, 2, 3, 4, 5, 6, 7, 8, 9};
  auto range_view =
      range_data | views::takeUntilInclusive([](int x) { return x == 5; });
  std::vector<int> range_result;
  for (auto&& element : range_view) {
    range_result.push_back(element);
  }
  EXPECT_THAT(range_result, ::testing::ElementsAre(1, 2, 3, 4, 5));
}

// Test iterator state consistency
TEST(TakeUntilInclusiveViewTest, IteratorStateConsistency) {
  std::vector<int> data{1, 2, 3, 4, 5};
  auto view = TakeUntilInclusiveView{data, [](int x) { return x == 3; }};

  auto it = view.begin();

  // Test that calling operator* multiple times doesn't change iterator state
  EXPECT_EQ(*it, 1);
  EXPECT_EQ(*it, 1);
  EXPECT_EQ(*it, 1);

  ++it;
  EXPECT_EQ(*it, 2);
  EXPECT_EQ(*it, 2);

  ++it;
  EXPECT_EQ(*it, 3);
  EXPECT_EQ(*it, 3);

  // After incrementing past the predicate-satisfying element, should reach end
  ++it;
  EXPECT_TRUE(it == view.end());
}

// Test that the view correctly handles the case where predicate is evaluated
// exactly once per element
TEST(TakeUntilInclusiveViewTest, PredicateEvaluatedOncePerElement) {
  std::vector<int> data{1, 2, 3, 4, 5};
  int evaluation_count = 0;
  auto counting_predicate = [&evaluation_count](int x) {
    ++evaluation_count;
    return x == 3;
  };

  auto view = TakeUntilInclusiveView{data, counting_predicate};

  std::vector<int> result;
  for (auto&& element : view) {
    result.push_back(element);
  }

  EXPECT_THAT(result, ::testing::ElementsAre(1, 2, 3));
  // Predicate should be evaluated exactly 3 times (for elements 1, 2, 3)
  EXPECT_EQ(evaluation_count, 3);
}

// Test behavior when skipping elements (e.g., with views::drop)
TEST(TakeUntilInclusiveViewTest, SkippingElements) {
  std::vector<int> data{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  int evaluation_count = 0;
  auto counting_predicate = [&evaluation_count](int x) {
    ++evaluation_count;
    return x == 4;  // Stop at 4 to avoid potential memory issues
  };

  auto takeUntilView = data | views::takeUntilInclusive(counting_predicate);
  auto view = takeUntilView | std::views::drop(2);  // Skip first 2 elements

  std::vector<int> result;
  for (auto&& element : view) {
    result.push_back(element);
  }

  EXPECT_THAT(result, ::testing::ElementsAre(3, 4));
  // The predicate should be evaluated for elements 1, 2, 3, 4
  // because takeUntilInclusive processes all elements until 4 (inclusive),
  // then drop(2) skips the first 2 from the resulting view
  EXPECT_EQ(evaluation_count, 4);
}

// Test with move-only elements
TEST(TakeUntilInclusiveViewTest, MoveOnlyElements) {
  std::vector<std::unique_ptr<int>> data;
  data.push_back(std::make_unique<int>(1));
  data.push_back(std::make_unique<int>(2));
  data.push_back(std::make_unique<int>(3));
  data.push_back(std::make_unique<int>(4));

  auto view = TakeUntilInclusiveView{
      std::move(data), [](const std::unique_ptr<int>& p) { return *p == 3; }};

  std::vector<int> result;
  for (auto& element : view) {
    auto moved = std::move(element);
    result.push_back(*moved);
  }

  EXPECT_THAT(result, ::testing::ElementsAre(1, 2, 3));
}

// Test range-for loop semantics
TEST(TakeUntilInclusiveViewTest, RangeForLoopSemantics) {
  std::vector<int> data{5, 10, 15, 8, 20, 25};
  auto view = TakeUntilInclusiveView{data, [](int x) { return x >= 20; }};

  std::vector<int> result;
  for (const auto& element : view) {
    result.push_back(element);
  }

  EXPECT_THAT(result, ::testing::ElementsAre(5, 10, 15, 8, 20));
}

// Test with const view
TEST(TakeUntilInclusiveViewTest, ConstView) {
  testTakeUntilView({1, 2, 3, 4, 5}, {1, 2, 3}, [](int x) { return x == 3; });
}
