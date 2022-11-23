//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include <ranges>

#include "engine/CallFixedSize.h"
#include "gtest/gtest.h"

using namespace ad_utility;

auto returnIPlusArgs = []<int I>(int arg1 = 0, int arg2 = 0) {
  return I + arg1 + arg2;
};

TEST(CallFixedSize, callLambdaWithStaticInt) {
  using namespace ad_utility::detail;

  static constexpr int maxValue = 242;
  for (int i = 0; i <= maxValue; ++i) {
    ASSERT_EQ(callLambdaWithStaticInt<maxValue>(i, returnIPlusArgs), i);
    ASSERT_EQ(callLambdaWithStaticInt<maxValue>(i, returnIPlusArgs, 2, 4),
              i + 6);
  }

  for (int i = maxValue + 1; i < maxValue + 5; ++i) {
    ASSERT_THROW(callLambdaWithStaticInt<maxValue>(i, returnIPlusArgs),
                 std::exception);
    ASSERT_THROW(callLambdaWithStaticInt<maxValue>(i, returnIPlusArgs, 2, 4),
                 std::exception);
  }
}

namespace oneVar {

// The two ways of setting up a callable for `callFixedSize1`, first with
// explicitly typed arguments:
auto explicitIntConstant = []<int I>(int arg1 = 0, int arg2 = 0) {
  return I + arg1 + arg2;
};

// And now with `auto` arguments to demonstrate that generic lambdas also work,
// and that the rather lengthy `integral_constant` type does not have to be
// explicitly stated. Note that in this case default arguments would not work
// properly, as one would always have to pass a value for them for the code
// to compile.
auto autoIntConstant = []<int I>(auto arg1, auto arg2) {
  return I + arg1 + arg2;
};

// A plain function templated on integer arguments to demonstrate the usage
// of the `CALL_FIXED_SIZE_1` macro. Note that here we have to state all the
// types of the arguments explicitly and default values do not work.
template <int I>
auto templatedFunction(int arg1 = 0, int arg2 = 0) {
  return I + arg1 + arg2;
}
}  // namespace oneVar

// ____________________________________________________________________________
TEST(CallFixedSize, CallFixedSize1) {
  using namespace oneVar;
  // static constexpr int m = DEFAULT_MAX_NUM_COLUMNS_STATIC_ID_TABLE;
  auto testWithGivenUpperBound = [](auto m, bool useMacro) {
    for (int i = 0; i <= m; ++i) {
      ASSERT_EQ(callFixedSize<m>(std::array{i}, explicitIntConstant), i);
      ASSERT_EQ(callFixedSize<m>(std::array{i}, explicitIntConstant, 2, 3),
                i + 5);
      ASSERT_EQ(callFixedSize<m>(std::array{i}, autoIntConstant, 2, 3), i + 5);
      if (useMacro) {
        ASSERT_EQ(CALL_FIXED_SIZE((std::array{i}), templatedFunction, 2, 3),
                  i + 5);
      }
    }

    // Values that are greater than `m` will be mapped to zero before being
    // passed to the actual function.
    for (int i = m + 1; i <= m + m + 1; ++i) {
      ASSERT_EQ(callFixedSize<m>(std::array{i}, explicitIntConstant), 0);
      ASSERT_EQ(callFixedSize<m>(std::array{i}, explicitIntConstant, 2, 3), 5);
      ASSERT_EQ(callFixedSize<m>(std::array{i}, autoIntConstant, 2, 3), 5);
      if (useMacro) {
        ASSERT_EQ(CALL_FIXED_SIZE((std::array{i}), templatedFunction, 2, 3), 5);
      }
    }
  };
  testWithGivenUpperBound(
      std::integral_constant<int, DEFAULT_MAX_NUM_COLUMNS_STATIC_ID_TABLE>{},
      true);
  // Custom upper bounds cannot be tested with the macros, as the macros don't
  // allow redefining the upper bound.
  testWithGivenUpperBound(std::integral_constant<int, 12>{}, false);
}

// Tests for two variables. The test cases are similar to the one variable
// case, see above for detailed documentation.
namespace twoVars {

// _____________________________________________________________________________
auto explicitIntConstant = []<int I, int J>(int arg1 = 0, int arg2 = 0) {
  return I - J + arg1 + arg2;
};

// And now with `auto` arguments to demonstrate that generic lambdas also work,
// and that the rather lengthy `integral_constant` type does not have to be
// explicitly stated. Note that in this case default arguments would not work
// properly, as one would always have to pass a value for them for the code
// to compile.
auto autoIntConstant = []<int I, int J>(auto arg1 = 0, auto arg2 = 0) {
  return I - J + arg1 + arg2;
};

// A plain function templated on integer arguments to demonstrate the usage
// of the `CALL_FIXED_SIZE_1` macro. Note that here we have to state all the
// types of the arguments explicitly and default arguments do not work.
template <int I, int J>
auto templatedFunction(int arg1, int arg2) {
  return I - J + arg1 + arg2;
}
}  // namespace twoVars

// ____________________________________________________________________________
TEST(CallFixedSize, CallFixedSize2) {
  using namespace twoVars;
  using namespace ad_utility::detail;

  auto testWithGivenUpperBound = [](auto m, bool useMacro) {
    // TODO<joka921, Clang16> the ranges of the loop can be greatly simplified
    // using `std::views::iota`, but views don't work yet on clang.
    for (int i = 0; i <= m; ++i) {
      for (int j = 0; j <= m; ++j) {
        ASSERT_EQ(callFixedSize<m>(std::array{i, j}, explicitIntConstant),
                  i - j)
            << i << ", " << j;
        ASSERT_EQ(callFixedSize<m>(std::array{i, j}, explicitIntConstant, 2, 3),
                  i - j + 5);
        ASSERT_EQ(callFixedSize<m>(std::array{i, j}, autoIntConstant, 2, 3),
                  i - j + 5);
        if (useMacro) {
          CALL_FIXED_SIZE((std::array{i, j}), templatedFunction, 2, 3);
          ASSERT_EQ(
              CALL_FIXED_SIZE((std::array{i, j}), templatedFunction, 2, 3),
              i - j + 5);
        }
      }
    }

    // Values that are greater than `m` will be mapped to zero before being
    // passed to the actual function.
    // Test all three possibilities: `i` becoming 0, `j` becoming 0, both
    // becoming zero.
    for (int i = 0; i <= m; ++i) {
      for (int j = m + 1; j <= m + m + 1; ++j) {
        ASSERT_EQ(callFixedSize<m>(std::array{i, j}, explicitIntConstant), i);
        ASSERT_EQ(callFixedSize<m>(std::array{i, j}, explicitIntConstant, 2, 3),
                  i + 5);
        ASSERT_EQ(callFixedSize<m>(std::array{i, j}, autoIntConstant, 2, 3),
                  i + 5);
        if (useMacro) {
          ASSERT_EQ(
              CALL_FIXED_SIZE((std::array{i, j}), templatedFunction, 2, 3),
              i + 5);
        }
      }
    }

    for (int i = m + 1; i <= m + m + 1; ++i) {
      for (int j = 0; j <= m; ++j) {
        ASSERT_EQ(callFixedSize<m>(std::array{i, j}, explicitIntConstant), -j);
        ASSERT_EQ(callFixedSize<m>(std::array{i, j}, explicitIntConstant, 2, 3),
                  -j + 5);
        ASSERT_EQ(callFixedSize<m>(std::array{i, j}, autoIntConstant, 2, 3),
                  -j + 5);
        if (useMacro) {
          ASSERT_EQ(
              CALL_FIXED_SIZE((std::array{i, j}), templatedFunction, 2, 3),
              -j + 5);
        }
      }
    }

    for (int i = m + 1; i <= m + m + 1; ++i) {
      for (int j = m + 1; j <= m + m + 1; ++j) {
        ASSERT_EQ(callFixedSize<m>(std::array{i, j}, explicitIntConstant), 0);
        ASSERT_EQ(callFixedSize<m>(std::array{i, j}, explicitIntConstant, 2, 3),
                  5);
        ASSERT_EQ(callFixedSize<m>(std::array{i, j}, autoIntConstant, 2, 3), 5);
        if (useMacro) {
          ASSERT_EQ(
              CALL_FIXED_SIZE((std::array{i, j}), templatedFunction, 2, 3), 5);
        }
      }
    }
  };

  testWithGivenUpperBound(
      std::integral_constant<int, DEFAULT_MAX_NUM_COLUMNS_STATIC_ID_TABLE>{},
      true);
  // Custom upper bounds cannot be tested with the macros, as the macros don't
  // allow redefining the upper bound.
  testWithGivenUpperBound(std::integral_constant<int, 12>{}, false);
}

// TODO<joka921> Tests for three variables, but first make the implementation
// generic.
