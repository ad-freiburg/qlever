// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR/QL
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
// QL =  QLeverize AG

#include <gmock/gmock.h>

#include <forward_list>
#include <vector>

#include "backports/iterator.h"
#include "util/ResetWhenMoved.h"

namespace {

using R = ad_utility::ResetWhenMoved<bool, false>;
// A value that is initially `true` and becomes `false` when being moved.
R r{true};

// A value that is false and will stay false (used for comparison).
const R rFalse{false};

// The test suite is templated on a container that stores elements of type `R`
// (see above).
template <typename T>
struct MoveSentinelTest : public ::testing::Test {
 public:
  // Initial state, 10 unmoved elements.
  T container_ = T(10, r);

  // Expected state after elementwise moving of the `container`.
  T afterMove_ = T(10, rFalse);

  // An empty container.
  T emptyContainer_{};
  void SetUp() override {}
};

// Test for `vector`(which is a very common type) and `forward_list`(which has
// different types for `begin()` and `end()`);
using ContainerTypes = ::testing::Types<std::vector<R>, std::forward_list<R>>;
TYPED_TEST_SUITE(MoveSentinelTest, ContainerTypes);

// _____________________________________________________________________________
TYPED_TEST(MoveSentinelTest, doesInFactMove) {
  auto& cont = this->container_;
  auto beg = std::make_move_iterator(cont.begin());
  auto end = ql::move_sentinel{cont.end()};

  [[maybe_unused]] R target{false};
  for (; beg != end; ++beg) {
    // This is a move-assigment because `beg` is a move-iterator.
    target = *beg;
  }
  EXPECT_THAT(cont, ::testing::ElementsAreArray(this->afterMove_));
}

// Manually test `==`, `!=`, and `base()`.
TYPED_TEST(MoveSentinelTest, basicFunctions) {
  {
    // In the empty container, `begin()` and `end()` compare equal.
    auto& empty = this->emptyContainer_;
    EXPECT_EQ(empty.begin(), empty.end());
    auto emptySent = ql::move_sentinel{empty.end()};
    EXPECT_EQ(std::make_move_iterator(empty.begin()), emptySent);
    EXPECT_EQ(emptySent.base(), empty.end());
    EXPECT_EQ(emptySent.base(), empty.begin());
  }

  {
    // Non-empty container, begin() and end() are not equal.
    auto& cont = this->container_;
    auto sent = ql::move_sentinel{cont.end()};
    EXPECT_NE(cont.begin(), cont.end());
    EXPECT_NE(std::make_move_iterator(cont.begin()), sent);
    EXPECT_EQ(sent.base(), cont.end());
    EXPECT_NE(sent.base(), cont.begin());
  }
}
}  // namespace
