// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#include <gtest/gtest.h>

#include <array>

#include "./IndexTestHelpers.h"
#include "./util/AllocatorTestHelpers.h"
#include "./util/IdTestHelpers.h"
#include "engine/TransitivePath.h"
#include "global/Id.h"

using ad_utility::testing::getQec;
using ad_utility::testing::makeAllocator;
namespace {
auto V = ad_utility::testing::VocabId;

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
}  // namespace

TEST(TransitivePathTest, idToId) {
  IdTable sub(2, makeAllocator());
  sub.push_back({V(0), V(1)});
  sub.push_back({V(1), V(2)});
  sub.push_back({V(1), V(3)});
  sub.push_back({V(2), V(3)});

  IdTable result(2, makeAllocator());

  IdTable expected(2, makeAllocator());
  expected.push_back({V(0), V(3)});

  TransitivePathSide left(std::nullopt, 0, V(0), 0);
  TransitivePathSide right(std::nullopt, 1, V(3), 1);
  TransitivePath T(getQec(), nullptr, left, right, 1,
                   std::numeric_limits<size_t>::max());

  T.computeTransitivePath<2, 2>(&result, sub, left, right);
  assertSameUnorderedContent(expected, result);
}

TEST(TransitivePathTest, idToVar) {
  IdTable sub(2, makeAllocator());
  sub.push_back({V(0), V(1)});
  sub.push_back({V(1), V(2)});
  sub.push_back({V(1), V(3)});
  sub.push_back({V(2), V(3)});

  IdTable result(2, makeAllocator());

  IdTable expected(2, makeAllocator());
  expected.push_back({V(0), V(1)});
  expected.push_back({V(0), V(2)});
  expected.push_back({V(0), V(3)});

  TransitivePathSide left(std::nullopt, 0, V(0), 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  TransitivePath T(getQec(), nullptr, left, right, 1,
                   std::numeric_limits<size_t>::max());

  T.computeTransitivePath<2, 2>(&result, sub, left, right);
  assertSameUnorderedContent(expected, result);
}

TEST(TransitivePathTest, varTovar) {
  IdTable sub(2, makeAllocator());
  sub.push_back({V(0), V(1)});
  sub.push_back({V(1), V(2)});
  sub.push_back({V(1), V(3)});
  sub.push_back({V(2), V(3)});

  IdTable result(2, makeAllocator());

  IdTable expected(2, makeAllocator());
  expected.push_back({V(0), V(1)});
  expected.push_back({V(0), V(2)});
  expected.push_back({V(0), V(3)});
  expected.push_back({V(1), V(2)});
  expected.push_back({V(1), V(3)});
  expected.push_back({V(2), V(3)});

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  TransitivePath T(getQec(), nullptr, right, left, 1,
                   std::numeric_limits<size_t>::max());

  T.computeTransitivePath<2, 2>(&result, sub, left, right);
  assertSameUnorderedContent(expected, result);
}

TEST(TransitivePathTest, unlimitedMaxLength) {
  IdTable sub(2, makeAllocator());
  sub.push_back({V(0), V(2)});
  sub.push_back({V(2), V(4)});
  sub.push_back({V(4), V(7)});
  sub.push_back({V(0), V(7)});
  sub.push_back({V(3), V(3)});
  sub.push_back({V(7), V(0)});
  // Disconnected component.
  sub.push_back({V(10), V(11)});

  IdTable result(2, makeAllocator());

  IdTable expected(2, makeAllocator());
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

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  TransitivePath T(getQec(), nullptr, left, right, 1,
                   std::numeric_limits<size_t>::max());

  T.computeTransitivePath<2, 2>(&result, sub, left, right);
  assertSameUnorderedContent(expected, result);
}

TEST(TransitivePathTest, maxLength2) {
  IdTable sub(2, makeAllocator());
  sub.push_back({V(0), V(2)});
  sub.push_back({V(2), V(4)});
  sub.push_back({V(4), V(7)});
  sub.push_back({V(0), V(7)});
  sub.push_back({V(3), V(3)});
  sub.push_back({V(7), V(0)});
  // Disconnected component.
  sub.push_back({V(10), V(11)});

  IdTable result(2, makeAllocator());

  IdTable expected(2, makeAllocator());

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

  TransitivePathSide left(std::nullopt, 0, Variable{"?start"}, 0);
  TransitivePathSide right(std::nullopt, 1, Variable{"?target"}, 1);
  TransitivePath T(getQec(), nullptr, left, right, 1, 2);
  T.computeTransitivePath<2, 2>(&result, sub, left, right);
  assertSameUnorderedContent(expected, result);

  result.clear();
  expected.clear();
  expected.push_back({V(7), V(0)});
  expected.push_back({V(7), V(2)});
  expected.push_back({V(7), V(7)});

  left.value_ = V(7);
  right.value_ = Variable{"?target"};
  T.computeTransitivePath<2, 2>(&result, sub, left, right);
  assertSameUnorderedContent(expected, result);

  result.clear();
  expected.clear();
  expected.push_back({V(0), V(2)});
  expected.push_back({V(7), V(2)});

  left.value_ = Variable{"?start"};
  right.value_ = V(2);
  T.computeTransitivePath<2, 2>(&result, sub, right, left);
  assertSameUnorderedContent(expected, result);
}
