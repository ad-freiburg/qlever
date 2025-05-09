//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "engine/CallFixedSize.h"
#include "gtest/gtest.h"

using namespace ad_utility;

TEST(CallFixedSize, callLambdaForIntArray) {
  using namespace ad_utility::detail;

  auto returnIPlusArgs = ad_utility::ApplyAsValueIdentity{
      [](auto valueIdentityI, int arg1 = 0, int arg2 = 0) {
        static constexpr int I = valueIdentityI.value;
        return I + arg1 + arg2;
      }};

  static constexpr int maxValue = 242;
  for (int i = 0; i <= maxValue; ++i) {
    ASSERT_EQ(callLambdaForIntArray<maxValue>(i, returnIPlusArgs), i);
    ASSERT_EQ(callLambdaForIntArray<maxValue>(i, returnIPlusArgs, 2, 4), i + 6);
  }

  for (int i = maxValue + 1; i < maxValue + 5; ++i) {
    ASSERT_THROW(callLambdaForIntArray<maxValue>(i, returnIPlusArgs),
                 std::exception);
    ASSERT_THROW(callLambdaForIntArray<maxValue>(i, returnIPlusArgs, 2, 4),
                 std::exception);
  }

  // Check for an array of size > 1
  auto returnIJKPlusArgs = ad_utility::ApplyAsValueIdentity{
      [](auto valueIdentityI, auto valueIdentityJ, auto valueIdentityK,
         int arg1 = 0, int arg2 = 0) {
        static constexpr int I = valueIdentityI.value;
        static constexpr int J = valueIdentityJ.value;
        static constexpr int K = valueIdentityK.value;
        return I + J + K + arg1 + arg2;
      }};
  static constexpr int maxValue3 = 5;
  for (int i = 0; i <= maxValue3; ++i) {
    for (int j = 0; j <= maxValue3; ++j) {
      for (int k = 0; k <= maxValue3; ++k) {
        ASSERT_EQ(callLambdaForIntArray<maxValue3>(std::array{i, j, k},
                                                   returnIJKPlusArgs),
                  i + j + k);
        ASSERT_EQ(callLambdaForIntArray<maxValue3>(std::array{i, j, k},
                                                   returnIJKPlusArgs, 2, 4),
                  i + j + k + 6);
      }
    }
  }

  ASSERT_THROW(callLambdaForIntArray<maxValue3>(std::array{maxValue3 + 1, 0, 0},
                                                returnIJKPlusArgs),
               std::exception);

  ASSERT_THROW(callLambdaForIntArray<maxValue3>(std::array{0, maxValue3 + 1, 0},
                                                returnIJKPlusArgs),
               std::exception);
  ASSERT_THROW(callLambdaForIntArray<maxValue3>(std::array{0, 0, maxValue3 + 1},
                                                returnIJKPlusArgs),
               std::exception);
}

namespace oneVar {

// A simple lambda that has one explicit template parameter of type `int`
// and can thus be used with `callFixedSize`.
auto lambda = [](auto valueIdentityI, int arg1 = 0, int arg2 = 0) {
  static constexpr int I = valueIdentityI.value;
  return I + arg1 + arg2;
};

// A plain function templated on integer arguments to demonstrate the usage
// of the `CALL_FIXED_SIZE` macro. Note that here we have to state all the
// types of the arguments explicitly and default values do not work.
template <int I>
auto freeFunction(int arg1 = 0, int arg2 = 0) {
  return I + arg1 + arg2;
}

// A static and non-static member function that can be also used with the
// `CALL_FIXED_SIZE` macro
struct S {
  template <int I>
  auto memberFunction(int arg1 = 0, int arg2 = 0) {
    return I + arg1 + arg2;
  }

  template <int I>
  static auto staticFunction(int arg1 = 0, int arg2 = 0) {
    return I + arg1 + arg2;
  }
};
}  // namespace oneVar

// ____________________________________________________________________________
TEST(CallFixedSize, CallFixedSize1) {
  using namespace oneVar;
  // static constexpr int m = DEFAULT_MAX_NUM_COLUMNS_STATIC_ID_TABLE;
  auto testWithGivenUpperBound = [](auto m, bool useMacro) {
    for (int i = 0; i <= m; ++i) {
      ASSERT_EQ(callFixedSizeVi<m>(i, lambda), i);
      ASSERT_EQ(callFixedSizeVi<m>(i, lambda, 2, 3), i + 5);
      if (useMacro) {
        ASSERT_EQ(CALL_FIXED_SIZE(i, freeFunction, 2, 3), i + 5);
        S s;
        ASSERT_EQ(CALL_FIXED_SIZE(i, &S::memberFunction, &s, 2, 3), i + 5);
        ASSERT_EQ(CALL_FIXED_SIZE(i, &S::staticFunction, 2, 3), i + 5);
      }
    }

    // Values that are greater than `m` will be mapped to zero before being
    // passed to the actual function.
    for (int i = m + 1; i <= m + m + 1; ++i) {
      ASSERT_EQ(callFixedSizeVi<m>(i, lambda), 0);
      ASSERT_EQ(callFixedSizeVi<m>(i, lambda, 2, 3), 5);
      if (useMacro) {
        ASSERT_EQ(CALL_FIXED_SIZE(i, freeFunction, 2, 3), 5);
        S s;
        ASSERT_EQ(CALL_FIXED_SIZE(i, &S::memberFunction, &s, 2, 3), 5);
        ASSERT_EQ(CALL_FIXED_SIZE(i, &S::staticFunction, 2, 3), 5);
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

// The same types of functions as above in the `oneVar` namespace, but these
// versions take two integer template parameters.

auto lambda = [](auto valueIdentityI, auto valueIdentityJ, int arg1 = 0,
                 int arg2 = 0) {
  static constexpr int I = valueIdentityI.value;
  static constexpr int J = valueIdentityJ.value;
  return I - J + arg1 + arg2;
};

template <int I, int J>
auto freeFunction(int arg1 = 0, int arg2 = 0) {
  return I - J + arg1 + arg2;
}

// A static and non-static member function that can be also used with the
// `CALL_FIXED_SIZE` macro
struct S {
  template <int I, int J>
  auto memberFunction(int arg1 = 0, int arg2 = 0) {
    return I - J + arg1 + arg2;
  }

  template <int I, int J>
  static auto staticFunction(int arg1 = 0, int arg2 = 0) {
    return I - J + arg1 + arg2;
  }
};
}  // namespace twoVars

// ____________________________________________________________________________
TEST(CallFixedSize, CallFixedSize2) {
  using namespace twoVars;
  using namespace ad_utility::detail;

  auto testWithGivenUpperBound = [](auto m, bool useMacro) {
    // For given values for the template parameters I and J, and the result
    // I - J perform a set of tests.
    auto testForIAndJ = [&](auto array, auto resultOfIJ) {
      ASSERT_EQ(callFixedSizeVi<m>(array, lambda), resultOfIJ);
      ASSERT_EQ(callFixedSizeVi<m>(array, lambda, 2, 3), resultOfIJ + 5);
      if (useMacro) {
        ASSERT_EQ(CALL_FIXED_SIZE(array, freeFunction, 2, 3), resultOfIJ + 5);
        S s;
        ASSERT_EQ(CALL_FIXED_SIZE(array, &S::memberFunction, &s, 2, 3),
                  resultOfIJ + 5);
        ASSERT_EQ(CALL_FIXED_SIZE(array, &S::staticFunction, 2, 3),
                  resultOfIJ + 5);
      }
    };
    // TODO<joka921, Clang16> the ranges of the loop can be greatly simplified
    // using `ql::views::iota`, but views don't work yet on clang.
    // TODO<joka921> We can then also setup a lambda that does the loop,
    // going from 4*4 to just 4 lines of calling code.
    for (int i = 0; i <= m; ++i) {
      for (int j = 0; j <= m; ++j) {
        testForIAndJ(std::array{i, j}, i - j);
      }
    }

    // Values that are greater than `m` will be mapped to zero before being
    // passed to the actual function.
    // Test all three possibilities: `i` becoming 0, `j` becoming 0, both
    // becoming zero.
    for (int i = 0; i <= m; ++i) {
      for (int j = m + 1; j <= m + m + 1; ++j) {
        testForIAndJ(std::array{i, j}, i);
      }
    }

    for (int i = m + 1; i <= m + m + 1; ++i) {
      for (int j = 0; j <= m; ++j) {
        testForIAndJ(std::array{i, j}, -j);
      }
    }

    for (int i = m + 1; i <= m + m + 1; ++i) {
      for (int j = m + 1; j <= m + m + 1; ++j) {
        testForIAndJ(std::array{i, j}, 0);
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
