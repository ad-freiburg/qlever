//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "util/LambdaHelpers.h"

TEST(MakeAssignableLambda, SimpleLambda) {
  // Non-capturing lambdas are assignable (they decay to function pointers.)
  auto twice = [](int x) { return 2 * x; };
  using T = decltype(twice);
  static_assert(std::is_copy_assignable_v<T>);
  static_assert(std::is_move_assignable_v<T>);

  // Capturing lambdas are not assignable.
  int m = 4;
  auto multiply = [m](int x) { return m * x; };
  using M = decltype(multiply);
  static_assert(!std::is_copy_assignable_v<M>);
  static_assert(!std::is_move_assignable_v<M>);

  // This works, as it is a copy constructor
  [[maybe_unused]] auto simpleCopy = multiply;

  auto multiplyAssignable = ad_utility::makeAssignableLambda(multiply);
  using A = decltype(multiplyAssignable);
  static_assert(std::is_copy_assignable_v<A>);
  static_assert(std::is_move_assignable_v<A>);

  ASSERT_EQ(8, multiplyAssignable(2));
  auto copy = multiplyAssignable;
  // Actually assign.
  copy = multiplyAssignable;
  ASSERT_EQ(8, copy(2));
}

TEST(MakeAssignableLambda, NonConst) {
  auto increment =
      ad_utility::makeAssignableLambda([i = 0]() mutable { return i++; });
  ASSERT_EQ(0, increment());
  ASSERT_EQ(1, increment());

  auto copy = increment;
  copy = increment;
  auto copy2 = increment;
  copy2 = std::move(increment);

  // The internal state `i` is 2, but each of the copies gets its own copy of
  // `i`.
  ASSERT_EQ(2, copy());
  ASSERT_EQ(2, copy2());

  ASSERT_EQ(3, copy());
  ASSERT_EQ(3, copy2());
}

TEST(MakeAssignableLambda, CopyVsMove) {
  // The detour via `vector` guarantees that moving the capture `v` will empty
  // it. We will use this to check whether a move actually happened.
  auto makeString = [](const std::string& s) {
    // Note: Although there is no `mutable` here the vector will still be moved
    // if the lambda is moved. The `mutable` only refers to the `operator()` of
    // the lambda and not to the closure member that corresponds to the capture.
    return ad_utility::makeAssignableLambda(
        [v = std::vector<char>(s.begin(), s.end())]() {
          return std::string(v.begin(), v.end());
        });
  };

  auto hallo = makeString("hallo");
  ASSERT_EQ("hallo", hallo());

  // Create objects of the proper type.
  auto copy = makeString("copy");
  auto moved = makeString("moved");

  ASSERT_EQ("copy", copy());
  ASSERT_EQ("moved", moved());

  copy = hallo;
  ASSERT_EQ("hallo", hallo());
  ASSERT_EQ("hallo", copy());

  // The move empties the (mutable) capture inside `hallo`.
  moved = std::move(hallo);
  ASSERT_EQ("hallo", moved());
  ASSERT_TRUE(hallo().empty());
}

TEST(MakeAssignableLambda, MoveOnly) {
  // The return type is not copyable because it captures `unique_ptr` which is a
  // move-only type.
  auto makeConstString = [](const std::string& s) {
    return ad_utility::makeAssignableLambda(
        [s = std::make_unique<std::string>(s)]() mutable { return s.get(); });
  };

  auto hallo = makeConstString("hallo");
  ASSERT_EQ("hallo", *hallo());

  static_assert(!std::is_copy_assignable_v<decltype(hallo)>);

  // Create objects of the proper type.
  auto moved = makeConstString("moved");
  ASSERT_EQ("moved", *moved());
  moved = std::move(hallo);
  ASSERT_EQ("hallo", *moved());
  ASSERT_EQ(nullptr, hallo());
}
