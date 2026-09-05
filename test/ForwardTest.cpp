//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "util/Forward.h"

template <typename Expected, typename T>
void tester(T&& t) {
  static_assert(std::is_same_v<Expected, decltype(t)>);
  auto innerTester = [](auto&& innerT) {
    static_assert(std::is_same_v<Expected, decltype(innerT)>);
  };
  innerTester(AD_FWD(t));
}

TEST(Forward, ExpectedTypes) {
  int intVal = 0;
  const int constIntVal = 0;
  int& intRef = intVal;
  const int& constIntRef = intVal;
  int&& movedIntRef = std::move(intVal);

  tester<int&>(intVal);
  tester<int&&>(std::move(intVal));
  tester<int&&>(AD_FWD(intVal));

  tester<const int&>(constIntVal);
  tester<const int&&>(AD_FWD(constIntVal));
  tester<const int&&>(std::move(constIntVal));

  tester<int&>(intRef);
  tester<const int&>(constIntRef);

  // subtle:: rvalue references are lvalues themselves, so we also have to call
  // std::move() or forward on them if we want to move.
  tester<int&>(movedIntRef);
  tester<int&&>(std::move(movedIntRef));
  tester<int&&>(AD_FWD(movedIntRef));
  tester<int&&>(42);
}

namespace {
struct WasMoved {
  static inline size_t numCopies = 0;
  static inline size_t numMoves = 0;
  WasMoved() = default;
  WasMoved(const WasMoved&) { numCopies++; }
  WasMoved& operator=(const WasMoved&) {
    numCopies++;
    return *this;
  }

  WasMoved(WasMoved&&) noexcept { numMoves++; }
  WasMoved& operator=(WasMoved&&) noexcept {
    numMoves++;
    return *this;
  }
};
}  // namespace
// _________________________________________________________________________
TEST(Forward, AD_MOVE) {
  const size_t& numMoves = WasMoved::numMoves;
  const size_t& numCopies = WasMoved::numCopies;
  ASSERT_EQ(numMoves, 0u);
  auto temp = []() { return WasMoved{}; };
  {
    [[maybe_unused]] auto x = temp();
    ASSERT_EQ(numMoves, 0u);
  }
  // The following code would emit a warning because of the redundant move.
  /*
  {
    auto x = std::move(temp());
    ASSERT_EQ(numMoves, 1u);
  }
   */
  {
    [[maybe_unused]] auto x = AD_MOVE(temp());
    ASSERT_EQ(numMoves, 0u);
  }
  {
    WasMoved x;
    [[maybe_unused]] auto y = std::move(x);
    ASSERT_EQ(numMoves, 1u);
  }
  {
    WasMoved x;
    [[maybe_unused]] auto y = AD_MOVE(x);
    ASSERT_EQ(numMoves, 2u);
  }
  ASSERT_EQ(numCopies, 0u);
  {
    const WasMoved x;
    [[maybe_unused]] auto y = AD_MOVE(x);
    ASSERT_EQ(numMoves, 2u);
    ASSERT_EQ(numCopies, 1u);
  }
}

namespace {
// A type that records whether a given instance was created by a copy or by a
// move. In contrast to `WasMoved` above this uses no global state, so the
// tests below do not depend on the order in which they are run.
struct MoveOrCopy {
  bool wasCopied_ = false;
  bool wasMoved_ = false;

  MoveOrCopy() = default;
  MoveOrCopy(const MoveOrCopy&) : wasCopied_{true} {}
  MoveOrCopy(MoveOrCopy&&) noexcept : wasMoved_{true} {}
};
}  // namespace

// _____________________________________________________________________________
TEST(Forward, moveIfReturnsTheExpectedTypes) {
  int value = 0;
  const int constValue = 0;
  static_assert(
      std::is_same_v<decltype(ad_utility::moveIf<true>(value)), int&&>);
  static_assert(
      std::is_same_v<decltype(ad_utility::moveIf<false>(value)), int&>);
  // The constness is preserved, so `moveIf<true>` on a `const` lvalue does not
  // actually move.
  static_assert(std::is_same_v<decltype(ad_utility::moveIf<true>(constValue)),
                               const int&&>);
  static_assert(std::is_same_v<decltype(ad_utility::moveIf<false>(constValue)),
                               const int&>);
  // No matter which of the two is used, the value itself is unchanged.
  EXPECT_EQ(ad_utility::moveIf<true>(value), 0);
  EXPECT_EQ(ad_utility::moveIf<false>(value), 0);
}

// _____________________________________________________________________________
TEST(Forward, moveIfMovesExactlyIfRequested) {
  MoveOrCopy source;
  MoveOrCopy copied = ad_utility::moveIf<false>(source);
  EXPECT_TRUE(copied.wasCopied_);
  EXPECT_FALSE(copied.wasMoved_);

  MoveOrCopy moved = ad_utility::moveIf<true>(source);
  EXPECT_FALSE(moved.wasCopied_);
  EXPECT_TRUE(moved.wasMoved_);

  // A `const` source is copied even if the move was requested.
  const MoveOrCopy constSource;
  MoveOrCopy fromConst = ad_utility::moveIf<true>(constSource);
  EXPECT_TRUE(fromConst.wasCopied_);
  EXPECT_FALSE(fromConst.wasMoved_);
}
