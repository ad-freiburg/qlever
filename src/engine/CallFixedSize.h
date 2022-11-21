// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#pragma once

#include <functional>

#include "util/Forward.h"

// The macros in this file provide automatic generation of if clauses that
// enable transformation of runtime variables to a limited range of compile
// time values and a default case. There are currently two possibilities to use this interface:
// 1. Using the macros CALL_FIXED_SIZE_1, CALL_FIXED_SIZE_2 and CALL_FIXED_SIZE_3.
//    `CALL_FIXED_SIZE_1(i, func, additionalArguments...)` calls
//     `std::invoke(func<i'>, additionalArguments...)` where `i' = i <=5? i : 0`
//    `i` must be an integer.
//   `CALL_FIXED_SIZE_2` and `CALL_FIXED_SIZE_3` work similarly, but with 2 or 3
//   integer arguments that are converted to template parameters.
// 2. Using the templated functions `callFixedSize1`, `callFixedSize2` and `callFixedSize3`,
//    that all live inside the `ad_utility` namespace.
//    `callFixedSize1(i, lambda, args...)` calls
//    `lambda(std::integer_constant<int, i'>, args...)`
// The first version allows a convenient syntax for most cases.
// The second version provides a macro-free typesafe interface that can be also
// used in the corner cases where the first version fails. It also is easier
// to maintain (e.g. to set the maximal value the template arguments to more
// than 5.

namespace ad_utility {
namespace detail {
template <int i>
static constexpr auto INT = std::integral_constant<int, i>{};
// Internal helper function that calls the `lambda` with `i'`(see above)
// as an explicit template parameter.
decltype(auto) callLambdaWithStaticInt(int i, auto&& lambda) {
  if (i == 1) {
    return lambda.template operator()<1>();
  } else if (i == 2) {
    return lambda.template operator()<2>();
  } else if (i == 3) {
    return lambda.template operator()<3>();
  } else if (i == 4) {
    return lambda.template operator()<4>();
  } else if (i == 5) {
    return lambda.template operator()<5>();
  } else {
    return lambda.template operator()<0>();
  }
}
template<int maxValue>
decltype(auto) callLambdaWithStaticInt2(int i, auto&& lambda, auto&&...args) {
  using Result = decltype(lambda.template operator()<1>);

  using Storage = std::conditional_t<std::is_void<Result>, void, std::optional<Result>>;

  auto applyIf =

  if (i == 1) {
    return lambda.template operator()<1>();
  } else if (i == 2) {
    return lambda.template operator()<2>();
  } else if (i == 3) {
    return lambda.template operator()<3>();
  } else if (i == 4) {
    return lambda.template operator()<4>();
  } else if (i == 5) {
    return lambda.template operator()<5>();
  } else {
    return lambda.template operator()<0>();
  }
}
}

// =============================================================================
// One Variable
// =============================================================================
// The main implementation for the case of one template variable.
// A good starting point for understanding the mechanisms.
decltype(auto) callFixedSize1(int i, auto&& functor, auto&&... args) {
  using namespace detail;
  auto lambda = [&]<int I>() -> decltype(auto) {
    return std::invoke(AD_FWD(functor), detail::INT<I>, AD_FWD(args)...);
  };
  return callLambdaWithStaticInt(i, std::move(lambda));
};

// =============================================================================
// Two Variables
// =============================================================================
namespace detail {
// Helper function for the two parameter case when we have
// already lifted the first runtime parameter i to a compile
// time constant with value i'.
template <int i>
decltype(auto) callFixedSize2I(int j, auto&& functor, auto&&... args) {
  using namespace detail;
  auto lambda = [&]<int J>() -> decltype(auto) {
    return std::invoke(AD_FWD(functor), INT<i>, INT<J>, AD_FWD(args)...);
  };
  return callLambdaWithStaticInt(j, lambda);
}
}

// Main implementation for the two variable case.
decltype(auto) callFixedSize2(int i, int j, auto&& functor, auto&&... args) {
  using namespace detail;
  auto lambda = [&]<int I>() -> decltype(auto) {
    return callFixedSize2I<I>(j, AD_FWD(functor), AD_FWD(args)...);
  };
  return callLambdaWithStaticInt(i, std::move(lambda));
}

// =============================================================================
// Three Variables
// =============================================================================

namespace detail {
// Helper function for the three variable case where the first two parameters
// `i` and `j` have already been lifted to compile time parameters `i'` and `j'`.
template <int i, int j>
decltype(auto) callFixedSize3IJ(int k, auto&& functor, auto&&... args) {
  auto lambda = [&]<int K>() -> decltype(auto) {
    return std::invoke(AD_FWD(functor), INT<i>, INT<j>, INT<K>,
                       AD_FWD(args)...);
  };
  return callLambdaWithStaticInt(k, lambda);
};

// Helper function for the three variable case where the first parameter
// `i` has already been lifted to a compile time parameters `i'`.
template <int i>
decltype(auto) callFixedSize3I(int j, int k, auto&& functor, auto&&... args) {
  auto lambda = [&]<int J>() -> decltype(auto) {
    return callFixedSize3IJ<i, J>(k, AD_FWD(functor), AD_FWD(args)...);
  };
  return callLambdaWithStaticInt(j, lambda);
}
}

namespace detail {

decltype(auto) lift(auto nextInt, auto&& lambda) {
  return [&]() {
    if (nextInt == 1) {
      return lambda(INT<1>);
    } else {
      return lambda(INT<0>);
    }
  };



}

}


// The main implementation for the three variable case.
decltype(auto) callFixedSize3(int i, int j, int k, auto&& functor,
                              auto&&... args) {
  using namespace detail;
  auto lambda = [&]<int I>() -> decltype(auto) {
    return callFixedSize3I<I>(j, k, AD_FWD(functor), AD_FWD(args)...);
  };
  return callLambdaWithStaticInt(i, lambda);

  /*
   return lift(i, lift(j, lift(k, someLambda)));
   */
}
}  // namespace ad_utility

// The definitions of the lambdas.
#define CALL_FIXED_SIZE_1(i, func, ...)                      \
  ad_utility::callFixedSize1(                                \
      i,                                                     \
      []<>(auto I, auto&&... args) -> decltype(auto) { \
        return std::invoke(func<I>, AD_FWD(args)...);       \
      },                                                     \
      __VA_ARGS__)

#define CALL_FIXED_SIZE_2(i, j, func, ...)                          \
  ad_utility::callFixedSize2(                                       \
      i,                                                            \
      j, [](auto I, auto J, auto&&... args) ->decltype(auto) { \
        return std::invoke(func<I, J>,AD_FWD(args)...);                         \
      },                                                            \
      __VA_ARGS__)

#define CALL_FIXED_SIZE_3(i, j, k, func, ...)                        \
  ad_utility::callFixedSize3(                                        \
      i, j, k,                                                       \
      [&](auto I, auto J, auto K, auto&&... args) -> decltype(auto) { \
        return std::invoke(func<I, J, K>,AD_FWD(args)...);                       \
      },                                                             \
      __VA_ARGS__)