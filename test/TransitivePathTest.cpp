// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#include <array>
#include <vector>

#include <gtest/gtest.h>

#include "../src/engine/TransitivePath.h"
#include "../src/global/Id.h"

// First sort both of the inputs and then ASSERT their equality. Needed for
// results of the TransitivePath operations which have a non-deterministic order
// because of the hash maps which are used internally.
void assertSameUnorderedContent(const IdTable& a, const IdTable& b) {
  auto aCpy = a;
  auto bCpy = b;
  ASSERT_EQ(a.cols(), b.cols());
  auto sorter = [](const auto& rowFromA, const auto& rowFromB) {
    for (size_t i = 0; i < rowFromA.cols(); ++i) {
      if (rowFromA[i] != rowFromB[i]) {
        return rowFromA[i] < rowFromB[i];
      }
    }
    // equal means "not smaller"
    return false;
  };
  std::sort(aCpy.begin(), aCpy.end(), sorter);
  std::sort(bCpy.begin(), bCpy.end(), sorter);
  ASSERT_EQ(aCpy, bCpy);
}

ad_utility::AllocatorWithLimit<Id>& allocator() {
  static ad_utility::AllocatorWithLimit<Id> a{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(
          std::numeric_limits<size_t>::max())};
  return a;
}

TEST(TransitivePathTest, computeTransitivePath) {
  IdTable sub(2, allocator());
  sub.push_back({0, 2});
  sub.push_back({2, 4});
  sub.push_back({4, 7});
  sub.push_back({0, 7});
  sub.push_back({3, 3});
  sub.push_back({7, 0});
  // Disconnected component.
  sub.push_back({10, 11});

  IdTable result(2, allocator());

  IdTable expected(2, allocator());
  expected.push_back({0, 2});
  expected.push_back({0, 4});
  expected.push_back({0, 7});
  expected.push_back({0, 0});
  expected.push_back({2, 4});
  expected.push_back({2, 7});
  expected.push_back({2, 0});
  expected.push_back({2, 2});
  expected.push_back({4, 7});
  expected.push_back({4, 0});
  expected.push_back({4, 2});
  expected.push_back({4, 4});
  expected.push_back({3, 3});
  expected.push_back({7, 0});
  expected.push_back({7, 2});
  expected.push_back({7, 4});
  expected.push_back({7, 7});
  expected.push_back({10, 11});

  TransitivePath T(nullptr, nullptr, false, false, 0, 0, 0, 0, "bim"s, "bam"s,
                   0, 0);

  T.computeTransitivePath<2>(&result, sub, true, true, 0, 1, 0, 0, 1,
                             std::numeric_limits<size_t>::max());
  assertSameUnorderedContent(expected, result);

  result.clear();
  expected.clear();
  expected.push_back({0, 2});
  expected.push_back({0, 4});
  expected.push_back({0, 7});
  expected.push_back({0, 0});
  expected.push_back({2, 4});
  expected.push_back({2, 7});
  expected.push_back({4, 7});
  expected.push_back({4, 0});
  expected.push_back({3, 3});
  expected.push_back({7, 0});
  expected.push_back({7, 2});
  expected.push_back({7, 7});
  expected.push_back({10, 11});

  T.computeTransitivePath<2>(&result, sub, true, true, 0, 1, 0, 0, 1, 2);
  assertSameUnorderedContent(expected, result);

  result.clear();
  expected.clear();
  expected.push_back({7, 0});
  expected.push_back({7, 2});
  expected.push_back({7, 7});

  T.computeTransitivePath<2>(&result, sub, false, true, 0, 1, 7, 0, 1, 2);
  assertSameUnorderedContent(expected, result);

  result.clear();
  expected.clear();
  expected.push_back({0, 2});
  expected.push_back({7, 2});

  T.computeTransitivePath<2>(&result, sub, true, false, 0, 1, 0, 2, 1, 2);
  assertSameUnorderedContent(expected, result);
}
