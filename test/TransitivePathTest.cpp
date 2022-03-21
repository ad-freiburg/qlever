// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#include <gtest/gtest.h>

#include <array>
#include <vector>

#include "../src/engine/TransitivePath.h"
#include "../src/global/Id.h"

auto V = [](const auto& id) { return Id::Vocab(id); };

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
  sub.push_back({V(0), V(2)});
  sub.push_back({V(2), V(4)});
  sub.push_back({V(4), V(7)});
  sub.push_back({V(0), V(7)});
  sub.push_back({V(3), V(3)});
  sub.push_back({V(7), V(0)});
  // Disconnected component.
  sub.push_back({V(10), V(11)});

  IdTable result(2, allocator());

  IdTable expected(2, allocator());
  expected.push_back({V(0), V(2)});
  expected.push_back({V(0), V(4)});
  expected.push_back({V(0), V(7)});
  expected.push_back({V(0), V(0)});
  expected.push_back({V(2), V(4)});
  expected.push_back({V(2), V(7)});
  expected.push_back({V(2), V(0)});
  expected.push_back({V(2), V(2)});
  expected.push_back({V(4), V(7)});
  expected.push_back({V(4), V(0)});
  expected.push_back({V(4), V(2)});
  expected.push_back({V(4), V(4)});
  expected.push_back({V(3), V(3)});
  expected.push_back({V(7), V(0)});
  expected.push_back({V(7), V(2)});
  expected.push_back({V(7), V(4)});
  expected.push_back({V(7), V(7)});
  expected.push_back({V(10), V(11)});

  TransitivePath T(nullptr, nullptr, false, false, 0, 0, V(0), V(0), "bim"s,
                   "bam"s, 0, 0);

  T.computeTransitivePath<2>(&result, sub, true, true, 0, 1, V(0), V(0), 1,
                             std::numeric_limits<size_t>::max());
  assertSameUnorderedContent(expected, result);

  result.clear();
  expected.clear();
  expected.push_back({V(0), V(2)});
  expected.push_back({V(0), V(4)});
  expected.push_back({V(0), V(7)});
  expected.push_back({V(0), V(0)});
  expected.push_back({V(2), V(4)});
  expected.push_back({V(2), V(7)});
  expected.push_back({V(4), V(7)});
  expected.push_back({V(4), V(0)});
  expected.push_back({V(3), V(3)});
  expected.push_back({V(7), V(0)});
  expected.push_back({V(7), V(2)});
  expected.push_back({V(7), V(7)});
  expected.push_back({V(10), V(11)});

  T.computeTransitivePath<2>(&result, sub, true, true, 0, 1, V(0), V(0), 1, 2);
  assertSameUnorderedContent(expected, result);

  result.clear();
  expected.clear();
  expected.push_back({V(7), V(0)});
  expected.push_back({V(7), V(2)});
  expected.push_back({V(7), V(7)});

  T.computeTransitivePath<2>(&result, sub, false, true, 0, 1, V(7), V(0), 1, 2);
  assertSameUnorderedContent(expected, result);

  result.clear();
  expected.clear();
  expected.push_back({V(0), V(2)});
  expected.push_back({V(7), V(2)});

  T.computeTransitivePath<2>(&result, sub, true, false, 0, 1, V(0), V(2), 1, 2);
  assertSameUnorderedContent(expected, result);
}
