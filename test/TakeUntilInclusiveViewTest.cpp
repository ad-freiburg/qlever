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
auto alwaysFalse = [](auto) { return false; };

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

}  // namespace

// Test basic functionality
TEST(TakeUntilInclusiveViewTest, BasicFunctionality) {
  std::vector<int> data{0, 2, 4, 3, 5, 6};
  auto view = TakeUntilInclusiveView{data, isOdd};

  std::vector<int> result;
  for (auto&& element : view) {
    result.push_back(element);
  }

  EXPECT_THAT(result, ::testing::ElementsAre(0, 2, 4, 3));
}

// Test with empty range
TEST(TakeUntilInclusiveViewTest, EmptyRange) {
  std::vector<int> data{};
  auto view = TakeUntilInclusiveView{data, isOdd};

  std::vector<int> result;
  for (auto&& element : view) {
    result.push_back(element);
  }

  EXPECT_TRUE(result.empty());
}

// Test when no element satisfies predicate
TEST(TakeUntilInclusiveViewTest, NoElementSatisfiesPredicate) {
  std::vector<int> data{0, 2, 4, 6, 8};
  auto view = TakeUntilInclusiveView{data, isOdd};

  std::vector<int> result;
  for (auto&& element : view) {
    result.push_back(element);
  }

  EXPECT_THAT(result, ::testing::ElementsAre(0, 2, 4, 6, 8));
}

// Test when first element satisfies predicate
TEST(TakeUntilInclusiveViewTest, FirstElementSatisfiesPredicate) {
  std::vector<int> data{1, 2, 4, 6, 8};
  auto view = TakeUntilInclusiveView{data, isOdd};

  std::vector<int> result;
  for (auto&& element : view) {
    result.push_back(element);
  }

  EXPECT_THAT(result, ::testing::ElementsAre(1));
}

// Test when all elements satisfy predicate
TEST(TakeUntilInclusiveViewTest, AllElementsSatisfyPredicate) {
  std::vector<int> data{1, 3, 5, 7, 9};
  auto view = TakeUntilInclusiveView{data, alwaysTrue};

  std::vector<int> result;
  for (auto&& element : view) {
    result.push_back(element);
  }

  EXPECT_THAT(result, ::testing::ElementsAre(1));
}

// Test when no element satisfies predicate (different predicate)
TEST(TakeUntilInclusiveViewTest, NoElementSatisfiesAlwaysFalse) {
  std::vector<int> data{1, 3, 5, 7, 9};
  auto view = TakeUntilInclusiveView{data, alwaysFalse};

  std::vector<int> result;
  for (auto&& element : view) {
    result.push_back(element);
  }

  EXPECT_THAT(result, ::testing::ElementsAre(1, 3, 5, 7, 9));
}

// Test single element range - predicate true
TEST(TakeUntilInclusiveViewTest, SingleElementPredicateTrue) {
  std::vector<int> data{5};
  auto view = TakeUntilInclusiveView{data, isOdd};

  std::vector<int> result;
  for (auto&& element : view) {
    result.push_back(element);
  }

  EXPECT_THAT(result, ::testing::ElementsAre(5));
}

// Test single element range - predicate false
TEST(TakeUntilInclusiveViewTest, SingleElementPredicateFalse) {
  std::vector<int> data{4};
  auto view = TakeUntilInclusiveView{data, isOdd};

  std::vector<int> result;
  for (auto&& element : view) {
    result.push_back(element);
  }

  EXPECT_THAT(result, ::testing::ElementsAre(4));
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
  auto view = TakeUntilInclusiveView{data, isEven};

  auto it = view.begin();
  EXPECT_EQ(*it, 1);

  auto old_it = it++;
  EXPECT_EQ(*old_it, 1);
  EXPECT_EQ(*it, 2);
}

// Test with different predicate types
TEST(TakeUntilInclusiveViewTest, DifferentPredicateTypes) {
  std::vector<int> data{1, 2, 3, 4, 5};

  // Lambda
  auto lambda_view = TakeUntilInclusiveView{data, [](int x) { return x == 3; }};
  std::vector<int> lambda_result;
  for (auto&& element : lambda_view) {
    lambda_result.push_back(element);
  }
  EXPECT_THAT(lambda_result, ::testing::ElementsAre(1, 2, 3));

  // Function pointer
  auto func_ptr = +[](int x) { return x == 3; };
  auto func_ptr_view = TakeUntilInclusiveView{data, func_ptr};
  std::vector<int> func_ptr_result;
  for (auto&& element : func_ptr_view) {
    func_ptr_result.push_back(element);
  }
  EXPECT_THAT(func_ptr_result, ::testing::ElementsAre(1, 2, 3));
}

// Test concept compliance
TEST(TakeUntilInclusiveViewTest, ConceptCompliance) {
  std::vector<int> data{1, 2, 3, 4, 5};
  using ViewType = TakeUntilInclusiveView<std::vector<int>&, decltype(isOdd)>;

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

  // Test combining with other standard library views
  auto result2 = data | std::views::transform([](int x) { return x * 2; }) |
                 views::takeUntilInclusive([](int x) { return x > 10; });

  std::vector<int> vec2;
  for (auto&& element : result2) {
    vec2.push_back(element);
  }
  EXPECT_THAT(vec2,
              ::testing::ElementsAre(
                  0, 4, 8, 6));  // 0*2, 2*2, 4*2, 3*2 until 3*2=6 which is the
                                 // first element satisfying predicate
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
  std::vector<int> data{1, 2, 3, 4, 5};

  // Test that deduction guide works correctly
  auto view = TakeUntilInclusiveView{data, isEven};
  std::vector<int> result;
  for (auto&& element : view) {
    result.push_back(element);
  }
  EXPECT_THAT(result, ::testing::ElementsAre(1, 2));
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

  // Test with std::views::iota
  auto iota_view = std::views::iota(1, 10) |
                   views::takeUntilInclusive([](int x) { return x == 5; });
  std::vector<int> iota_result;
  for (auto&& element : iota_view) {
    iota_result.push_back(element);
  }
  EXPECT_THAT(iota_result, ::testing::ElementsAre(1, 2, 3, 4, 5));
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
    return x == 7;
  };

  auto view = data | views::takeUntilInclusive(counting_predicate) |
              std::views::drop(3);  // Skip first 3 elements

  std::vector<int> result;
  for (auto&& element : view) {
    result.push_back(element);
  }

  EXPECT_THAT(result, ::testing::ElementsAre(4, 5, 6, 7));
  // The predicate should be evaluated for elements that are actually accessed
  // When drop(3) is used, elements 1, 2, 3 are skipped without calling
  // operator* So the predicate should be called for 4, 5, 6, 7
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
  for (auto&& element : view) {
    result.push_back(*element);
  }

  EXPECT_THAT(result, ::testing::ElementsAre(1, 2, 3));
}

// Test range-for loop semantics
TEST(TakeUntilInclusiveViewTest, RangeForLoopSemantics) {
  std::vector<int> data{5, 10, 15, 8, 20, 25};
  auto view = TakeUntilInclusiveView{data, [](int x) { return x < 10; }};

  std::vector<int> result;
  for (const auto& element : view) {
    result.push_back(element);
  }

  EXPECT_THAT(result, ::testing::ElementsAre(5, 10, 15, 8));
}

// Test with const view
TEST(TakeUntilInclusiveViewTest, ConstView) {
  const std::vector<int> data{1, 2, 3, 4, 5};
  const auto view = TakeUntilInclusiveView{data, [](int x) { return x == 3; }};

  std::vector<int> result;
  for (auto&& element : view) {
    result.push_back(element);
  }

  EXPECT_THAT(result, ::testing::ElementsAre(1, 2, 3));
}
