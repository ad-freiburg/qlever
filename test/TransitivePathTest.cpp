// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#include <gtest/gtest.h>

#include <array>
#include <string>
#include <vector>

#include "engine/TransitivePath.h"
#include "global/Id.h"

auto I = [](const auto& id) {
  return Id::makeFromVocabIndex(VocabIndex::make(id));
};

// First sort both of the inputs and then ASSERT their equality. Needed for
// results of the TransitivePath operations which have a non-deterministic order
// because of the hash maps which are used internally.
void assertSameUnorderedContent(const IdTable& a, const IdTable& b) {
  auto aCpy = a.clone();
  auto bCpy = b.clone();
  ASSERT_EQ(a.numColumns(), b.numColumns());
  auto sorter = [](const auto& rowFromA, const auto& rowFromB) {
    for (size_t i = 0; i < rowFromA.numColumns(); ++i) {
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
  sub.push_back({I(0), I(2)});
  sub.push_back({I(2), I(4)});
  sub.push_back({I(4), I(7)});
  sub.push_back({I(0), I(7)});
  sub.push_back({I(3), I(3)});
  sub.push_back({I(7), I(0)});
  // Disconnected component.
  sub.push_back({I(10), I(11)});

  IdTable result(2, allocator());

  IdTable expected(2, allocator());
  expected.push_back({I(0), I(2)});
  expected.push_back({I(0), I(4)});
  expected.push_back({I(0), I(7)});
  expected.push_back({I(0), I(0)});
  expected.push_back({I(2), I(4)});
  expected.push_back({I(2), I(7)});
  expected.push_back({I(2), I(0)});
  expected.push_back({I(2), I(2)});
  expected.push_back({I(4), I(7)});
  expected.push_back({I(4), I(0)});
  expected.push_back({I(4), I(2)});
  expected.push_back({I(4), I(4)});
  expected.push_back({I(3), I(3)});
  expected.push_back({I(7), I(0)});
  expected.push_back({I(7), I(2)});
  expected.push_back({I(7), I(4)});
  expected.push_back({I(7), I(7)});
  expected.push_back({I(10), I(11)});

  TransitivePath T(nullptr, nullptr, false, false, 0, 0, I(0), I(0),
                   Variable{"?bim"}, Variable{"?bam"}, 0, 0);

  T.computeTransitivePath<2>(&result, sub, true, true, 0, 1, I(0), I(0), 1,
                             std::numeric_limits<size_t>::max());
  assertSameUnorderedContent(expected, result);

  result.clear();
  expected.clear();
  expected.push_back({I(0), I(2)});
  expected.push_back({I(0), I(4)});
  expected.push_back({I(0), I(7)});
  expected.push_back({I(0), I(0)});
  expected.push_back({I(2), I(4)});
  expected.push_back({I(2), I(7)});
  expected.push_back({I(4), I(7)});
  expected.push_back({I(4), I(0)});
  expected.push_back({I(3), I(3)});
  expected.push_back({I(7), I(0)});
  expected.push_back({I(7), I(2)});
  expected.push_back({I(7), I(7)});
  expected.push_back({I(10), I(11)});

  T.computeTransitivePath<2>(&result, sub, true, true, 0, 1, I(0), I(0), 1, 2);
  assertSameUnorderedContent(expected, result);

  result.clear();
  expected.clear();
  expected.push_back({I(7), I(0)});
  expected.push_back({I(7), I(2)});
  expected.push_back({I(7), I(7)});

  T.computeTransitivePath<2>(&result, sub, false, true, 0, 1, I(7), I(0), 1, 2);
  assertSameUnorderedContent(expected, result);

  result.clear();
  expected.clear();
  expected.push_back({I(0), I(2)});
  expected.push_back({I(7), I(2)});

  T.computeTransitivePath<2>(&result, sub, true, false, 0, 1, I(0), I(2), 1, 2);
  assertSameUnorderedContent(expected, result);
}
