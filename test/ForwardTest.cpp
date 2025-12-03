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
