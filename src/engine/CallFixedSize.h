// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#pragma once

// TODO<joka921, clang> Clang 13 issues a lot of strange and false positive
// warnings (this bug was fixed in clang 14 and newer. This suppresses all
// warnings for this file. Remove this pragma as soon as we no longer support
// clang 13.
#ifdef __clang__
#pragma clang system_header
#endif

#include <functional>
#include <optional>

#include "util/Exception.h"
#include "util/Log.h"
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
// Internal helper function that calls the `lambda` with `i`(see above)
// as an explicit template parameter. Requires that `i <= maxValue`.
template <int maxValue>
decltype(auto) callLambdaWithStaticInt(int i, auto&& lambda, auto&&... args) {
  AD_CHECK(i <= maxValue);
  using Result = decltype(lambda.template operator()<1>());
  using Storage = std::conditional_t<std::is_void_v<Result>, int, std::optional<Result>>;

  Storage result;
  auto applyIf = [&result, lambda = AD_FWD(lambda),
                  &... args = AD_FWD(args)]<int I>(int i) mutable {
    if (i == I) {
      if constexpr (std::is_void_v<Result>) {
        lambda.template operator()<I>(AD_FWD(args)...);
      } else {
        result = lambda.template operator()<I>(AD_FWD(args)...);
      }
    }
  };

  auto f = [&applyIf, i ]<int... Is>(std::integer_sequence<int, Is...>) {
    (..., applyIf.template operator()<Is>(i));
  };
  f(std::make_integer_sequence<int, maxValue + 1>{});

  if constexpr (!std::is_void_v<Result>) {
    return std::move(result.value());
  }
}

constexpr int prime(int x, int maxValue) { return x <= maxValue ? x : 0; }
}

static constexpr int DEFAULT_MAX_NUM_COLUMNS_STATIC_ID_TABLE = 5;

template <const auto Arr, typename Seq = std::make_index_sequence<std::size(Arr)>>
struct make_seq;

template <typename T, std::size_t N, const std::array<T, N> Arr, std::size_t ... Is>
struct make_seq<Arr, std::index_sequence<Is...>>
{
  using type = std::integer_sequence<T, Arr[Is]...>;
};


template <size_t NumIntegers>
constexpr std::array<int, NumIntegers> mapBack (int value, int numValues) {
  std::array<int, NumIntegers> res;
  // TODO<joka921, clang16> use views for reversion.
  for (size_t i = 0; i < res.size(); ++i) {
    res[res.size() - 1 - i] = value % numValues;
    value /= numValues;
  }
  return res;
};

template <size_t NumIntegers, int MaxValue = DEFAULT_MAX_NUM_COLUMNS_STATIC_ID_TABLE>
decltype(auto) callFixedSizeN(std::array<int, NumIntegers> ints, auto&& functor, auto&&... args) {
  static_assert(NumIntegers > 0);
  using namespace detail;
  static constexpr int numValues = MaxValue + 1;
  auto p = [](int x) { return prime(x, MaxValue); };
  int value = p(std::get<0>(ints));
  // TODO<joka921, clang16> use `std::views`.
  for (size_t i = 1; i < ints.size(); ++i) {
    value *= numValues;
    value += p(ints[i]);
  }


  constexpr auto pow = [](int value) {
    int result = 1;
    for (size_t i = 0; i < NumIntegers; ++i) {
      result *= value;
    }
    return result;
  };

  auto applyOnIndexSequence = [&args...,functor = AD_FWD(functor) ]<int... ints>(std::integer_sequence<int, ints...>) mutable {
    return AD_FWD(functor)(INT<ints>..., AD_FWD(args)...);
  };

  auto lambda = [&applyOnIndexSequence]<int I>() mutable -> decltype(auto) {
    constexpr static std::array<int, NumIntegers> arr = mapBack<NumIntegers>(I, numValues);
    for (auto el : arr) {
    }
    return applyOnIndexSequence(typename make_seq<mapBack<NumIntegers>(I, numValues)>::type{});
  };
  return callLambdaWithStaticInt<pow(numValues)>(value, std::move(lambda));
}

// Main implementation for the two variable case.
template <int MaxValue = DEFAULT_MAX_NUM_COLUMNS_STATIC_ID_TABLE>
decltype(auto) callFixedSize2(int i, int j, auto&& functor, auto&&... args) {
  return callFixedSizeN(std::array{i, j}, AD_FWD(functor), AD_FWD(args)...);
}

template <int MaxValue = DEFAULT_MAX_NUM_COLUMNS_STATIC_ID_TABLE>
decltype(auto) callFixedSize1(int i, auto&& functor, auto&&... args) {
  return callFixedSizeN(std::array{i}, AD_FWD(functor), AD_FWD(args)...);
};

template <int MaxValue = DEFAULT_MAX_NUM_COLUMNS_STATIC_ID_TABLE>
decltype(auto) callFixedSize3(int i, int j, int k, auto&& functor, auto&&... args) {
  return callFixedSizeN(std::array{i, j, k}, AD_FWD(functor), AD_FWD(args)...);
}
}  // namespace ad_utility

// The definitions of the lambdas.
#define CALL_FIXED_SIZE_1(i, func, ...)                                        \
  ad_utility::callFixedSize1(i, [](auto I, auto&&... args) -> decltype(auto) { \
    return std::invoke(func<I>, AD_FWD(args)...);                              \
  } __VA_OPT__(, ) __VA_ARGS__)

#define CALL_FIXED_SIZE_2(i, j, func, ...)                                                \
  ad_utility::callFixedSize2(i, j, [](auto I, auto J, auto&&... args) -> decltype(auto) { \
    return std::invoke(func<I, J>, AD_FWD(args)...);                                      \
  } __VA_OPT__(, ) __VA_ARGS__)

#define CALL_FIXED_SIZE_3(i, j, k, func, ...)                                                \
  ad_utility::callFixedSize3(i, j, k,                                                        \
                             [&](auto I, auto J, auto K, auto&&... args) -> decltype(auto) { \
                               return std::invoke(func<I, J, K>, AD_FWD(args)...);           \
                             } __VA_OPT__(, ) __VA_ARGS__)
