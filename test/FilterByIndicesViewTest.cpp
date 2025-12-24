#include <gtest/gtest.h>

#include <numeric>
#include <vector>

#include "util/FilterByIndicesView.h"

using ad_utility::FilterByIndicesView;

TEST(FilterByIndicesViewTest, BasicSelection) {
  std::vector<int> data(10);
  std::iota(data.begin(), data.end(), 0);

  std::vector<size_t> indices{0, 3, 5, 9};

  FilterByIndicesView view{data, indices, data.size()};

  std::vector<int> result;
  for (auto v : view) {
    result.push_back(v);
  }

  ASSERT_EQ(result.size(), indices.size());
  EXPECT_EQ(result[0], 0);
  EXPECT_EQ(result[1], 3);
  EXPECT_EQ(result[2], 5);
  EXPECT_EQ(result[3], 9);
}

TEST(FilterByIndicesViewTest, EmptyIndices) {
  std::vector<int> data(5);
  std::iota(data.begin(), data.end(), 0);

  std::vector<size_t> indices{};

  FilterByIndicesView view{data, indices, data.size()};

  size_t count = 0;
  for (auto v : view) {
    (void)v;
    ++count;
  }
  EXPECT_EQ(count, 0);
}

TEST(FilterByIndicesViewTest, ConsecutiveIndices) {
  std::vector<int> data(8);
  std::iota(data.begin(), data.end(), 0);

  std::vector<size_t> indices{2, 3, 4, 5};

  FilterByIndicesView view{data, indices, data.size()};

  std::vector<int> result;
  for (auto v : view) {
    result.push_back(v);
  }
  std::vector<int> expected{2, 3, 4, 5};
  EXPECT_EQ(result, expected);
}

TEST(FilterByIndicesViewTest, SingleIndex) {
  std::vector<int> data(6);
  std::iota(data.begin(), data.end(), 0);

  std::vector<size_t> indices{4};

  FilterByIndicesView view{data, indices, data.size()};

  std::vector<int> result;
  for (auto v : view) {
    result.push_back(v);
  }
  ASSERT_EQ(result.size(), 1);
  EXPECT_EQ(result[0], 4);
}
