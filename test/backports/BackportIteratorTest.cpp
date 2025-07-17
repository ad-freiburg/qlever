// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR, QL
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
R r{true};
R rFalse{false};

template <typename T>
struct MoveSentinelTest : public ::testing::Test {
 public:
  T container = []() { return T(10, r); }();
  T afterMove = []() { return T(10, rFalse); }();
  T emptyContainer{};
  void SetUp() override {}
};

using ContainerTypes = ::testing::Types<std::vector<R>, std::forward_list<R>>;
TYPED_TEST_SUITE(MoveSentinelTest, ContainerTypes);

// _____________________________________________________________________________
TYPED_TEST(MoveSentinelTest, doesInFactMove) {
  auto& cont = this->container;
  auto beg = std::make_move_iterator(cont.begin());
  auto end = ql::move_sentinel{cont.end()};

  [[maybe_unused]] R target{false};
  for (; beg != end; ++beg) {
    target = *beg;
  }
  EXPECT_THAT(cont, ::testing::ElementsAreArray(this->afterMove));
}

// Manually test `==`, `!=`, and `base()`.
TYPED_TEST(MoveSentinelTest, basicFunctions) {
  {
    auto& empty = this->emptyContainer;
    EXPECT_EQ(empty.begin(), empty.end());
    auto emptySent = ql::move_sentinel{empty.end()};
    EXPECT_EQ(std::make_move_iterator(empty.begin()), emptySent);
    EXPECT_EQ(emptySent.base(), empty.end());
    EXPECT_EQ(emptySent.base(), empty.begin());
  }

  {
    auto& cont = this->container;
    auto sent = ql::move_sentinel{cont.end()};
    EXPECT_NE(cont.begin(), cont.end());
    EXPECT_NE(std::make_move_iterator(cont.begin()), sent);
    EXPECT_EQ(sent.base(), cont.end());
    EXPECT_NE(sent.base(), cont.begin());
  }
}
}  // namespace
