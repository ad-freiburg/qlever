// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <gmock/gmock.h>

#include "./util/GTestHelpers.h"
#include "util/VariantRangeFilter.h"

namespace {

// Helper for testing `filterRangeOfVariantsByType`.
template <typename V, typename T>
void expectFilteredRange(
    std::vector<V> input, std::vector<T> expected,
    ad_utility::source_location location = AD_CURRENT_SOURCE_LOC()) {
  auto l = generateLocationTrace(location);
  auto matcher = liftMatcherToElementsAreArray<T, std::vector<T>>(
      [](auto value) { return ::testing::Eq(value); });
  auto actual = ad_utility::filterRangeOfVariantsByType<T>(input) |
                ::ranges::to<std::vector<T>>;
  EXPECT_THAT(actual, matcher(expected));
}

// _____________________________________________________________________________
TEST(VariantRangeFilterTest, Test) {
  using V = std::variant<int, char, bool, double>;
  std::vector<V> vec{1, 'c', true, false, true, 3, 'f'};

  expectFilteredRange<V, int>(vec, {1, 3});
  expectFilteredRange<V, char>(vec, {'c', 'f'});
  expectFilteredRange<V, bool>(vec, {true, false, true});
  expectFilteredRange<V, double>(vec, {});
}

}  // namespace
