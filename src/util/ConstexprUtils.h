//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_UTIL_CONSTEXPRUTILS_H
#define QLEVER_SRC_UTIL_CONSTEXPRUTILS_H

#include "backports/algorithm.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/TypeIdentity.h"
#include "util/TypeTraits.h"
#include "util/ValueIdentity.h"

// Various helper functions for compile-time programming.

namespace ad_utility {

// Compute `base ^ exponent` where `^` denotes exponentiation. This is consteval
// because for all runtime calls, a better optimized algorithm from the standard
// library should be chosen.
// TODO<joka921> why can't this be consteval when the result is bound to a
// `constexpr` variable?
template <typename T>
constexpr auto pow(T base, int exponent) {
  if (exponent < 0) {
    throw std::runtime_error{"negative exponent"};
  }
  decltype(base) result = 1;
  for (int i = 0; i < exponent; ++i) {
    result *= base;
  }
  return result;
};

/*
 * @brief A compile time for loop, which passes the loop index to the
 *  given loop body.
 *
 * @tparam Function The loop body should be a templated function, with one
 *  size_t template argument and no more. It also shouldn't take any function
 *  arguments. Should be passed per deduction.
 * @tparam ForLoopIndexes The indexes, that the for loop goes over. Should be
 *  passed per deduction.
 *
 * @param loopBody The body of the for loop.
 */
template <typename Function, size_t... ForLoopIndexes>
void ConstexprForLoop(const std::index_sequence<ForLoopIndexes...>&,
                      const Function& loopBody) {
  ((loopBody.template operator()<ForLoopIndexes>()), ...);
}

/*
 * @brief A compile time for loop, similar to ConstexprForLoop, but wrapping
 * loop body in `ApplyAsValueIdentity`.
 */
template <typename Function, size_t... ForLoopIndexes>
void ConstexprForLoopVi(const std::index_sequence<ForLoopIndexes...>&,
                        const Function& loopBody) {
  ((ApplyAsValueIdentity{loopBody}.template operator()<ForLoopIndexes>()), ...);
}

template <typename Func, typename CaseConstant, typename... ArgTypes>
CPP_requires(
    is_invocable_with_case_,
    requires(Func&& f, ArgTypes&&... args)(
        AD_FWD(f).template operator()<CaseConstant::value>(AD_FWD(args)...)));

// Concept that checks if a function can be called with the given arguments
template <typename Func, auto Case, typename... ArgTypes>
CPP_concept InvocableWithCase =
    CPP_requires_ref(is_invocable_with_case_, Func,
                     std::integral_constant<decltype(Case), Case>, ArgTypes...);

// A `constexpr` switch statement. Chooses the `MatchingCase` in `FirstCase,
// Cases...` that is equal to `value` and then calls
// `function.operator()<MatchingCase>(args...)`.
template <auto FirstCase, auto... Cases>
struct ConstexprSwitch {
  CPP_template(typename FuncType, typename ValueType, typename... Args)(
      requires((sizeof...(Cases) == 0) ||
               ad_utility::SameAsAny<decltype(FirstCase), decltype(Cases)...>)
          CPP_and ql::concepts::equality_comparable_with<decltype(FirstCase),
                                                         decltype(FirstCase)>
              CPP_and InvocableWithCase<FuncType, FirstCase, Args...>
                  CPP_and(InvocableWithCase<FuncType, Cases,
                                            Args...>&&...)) constexpr auto
  operator()(FuncType&& function, const ValueType& value, Args&&... args) const
      -> decltype(auto) {
    if (value == FirstCase) {
      return AD_FWD(function).template operator()<FirstCase>(AD_FWD(args)...);
    } else if constexpr (sizeof...(Cases) > 0) {
      return ConstexprSwitch<Cases...>{}(AD_FWD(function), value,
                                         AD_FWD(args)...);
    } else {
      AD_FAIL();
    }
  }
};

/*
 * @brief 'Converts' a run time value of `size_t` to a compile time value and
 * then calls `function.template operator()<value>()`. `value < MaxValue` must
 * be true, else an exception is thrown. *
 *
 * @tparam MaxValue The maximal value, that the function parameter value could
 *  take.
 * @tparam Function The given function should be a templated function, with one
 *  size_t template argument and no more. It also shouldn't take any function
 *  arguments. This parameter should be passed per deduction.
 *
 * @param value Value that you need as a compile time value.
 * @param function The templated function, which you wish to execute. Must be
 *  a function object (for example a lambda expression) that has an
 *  `operator()` which is templated on a single `size_t`.
 */
template <size_t MaxValue, typename Function>
void RuntimeValueToCompileTimeValue(const size_t& value, Function function) {
  AD_CONTRACT_CHECK(value <= MaxValue);  // Is the value valid?
  ConstexprForLoopVi(std::make_index_sequence<MaxValue + 1>{},
                     [&function, &value](auto index) {
                       if (index == value) {
                         function.template operator()<index.value>();
                       }
                     });
}

/*
 * @brief Similar to RuntimeValueToCompileTimeValue, but using
 * ConstexprForLoopVi which automatically wraps the function in
 * `ApplyAsValueIdentity`.
 */
template <size_t MaxValue, typename Function>
void RuntimeValueToCompileTimeValueVi(const size_t& value, Function function) {
  return RuntimeValueToCompileTimeValue<MaxValue>(
      value, ApplyAsValueIdentity{std::move(function)});
}

/*
@brief Returns the index of the first given type, that passes the given check.

@tparam checkFunction A `constexpr` function, that takes one template
parameter and return a bool.
@tparam Args The list of types, that should be checked over.

@return An index out of `[0, sizeof...(Args))`, if there was type, that passed
the check. Otherwise, returns `sizeof...(Args)`.
*/
template <auto checkFunction, typename... Args>
constexpr size_t getIndexOfFirstTypeToPassCheck() {
  using ad_utility::use_type_identity::ti;

  size_t index = 0;

  auto l = [&index](auto t) {
    using T = typename decltype(t)::type;
    if constexpr (checkFunction.template operator()<T>()) {
      return true;
    } else {
      ++index;
      return false;
    }
  };

  ((l(ti<Args>)) || ...);

  return index;
}

// An `ad_utility::ValueSequence<T, values...>` has the same functionality as
// `std::integer_sequence`. This replacement is needed to compile QLever with
// libc++, because libc++ strictly enforces the `std::integral` constraint for
// `std::integer_sequence`, and we also need non-integral types as values, for
// example `std::array<...>`.

// An `ad_utility::ValueSequenceRef<T, values...>` does exactly the same, but
// the values are passed in as `const&`. This requires them to be constexpr
// objects with linkage, but increases the usability in C++17 mode, where
// objects like `std::array` can't be passed as template arguments by value.
namespace detail {
template <typename T, T... values>
struct ValueSequenceImpl {};

template <typename T, const T&... values>
struct ValueSequenceRefImpl {};
};  // namespace detail

template <typename T, T... values>
using ValueSequence = detail::ValueSequenceImpl<T, values...>;

template <typename T, const T&... values>
using ValueSequenceRef = detail::ValueSequenceRefImpl<T, values...>;

namespace detail {
// The implementation for the `toIntegerSequence` function (see below).
// For the ideas and alternative implementations see
// https://stackoverflow.com/questions/56799396/
template <auto Array, size_t... indexes>
constexpr auto toIntegerSequenceHelper(std::index_sequence<indexes...>) {
  return ValueSequence<typename decltype(Array)::value_type,
                       std::get<indexes>(Array)...>{};
}

// Implementation for `toIntegerSequenceRef` below.
template <const auto& Array, size_t... indexes>
constexpr auto toIntegerSequenceRefHelper(std::index_sequence<indexes...>) {
  return ValueSequence<typename std::decay_t<decltype(Array)>::value_type,
                       std::get<indexes>(Array)...>{};
}
}  // namespace detail

// Convert a compile-time `std::array` to a `ValueSequence` that
// contains the same values. This is useful because arrays can be easily created
// in constexpr functions using `normal` syntax, whereas `ValueSequence`s are
// useful for working with templates that take several ints as template
// parameters. For an example usage see `CallFixedSize.h`
template <auto Array>
auto toIntegerSequence() {
  return detail::toIntegerSequenceHelper<Array>(
      std::make_index_sequence<Array.size()>{});
}

// Exactly the same as `toIntegerSequence` directly above, but the `Array` is
// passed as a `const&`, which makes this function usable in C++17 mode.
template <const auto& Array>
auto toIntegerSequenceRef() {
  return detail::toIntegerSequenceRefHelper<Array>(
      std::make_index_sequence<Array.size()>{});
}

// Map a single integer `value` that is in the range `[0, ..., (maxValue + 1) ^
// NumIntegers - 1 to an array of `NumIntegers` many integers that are each in
// the range
// `[0, ..., (maxValue)]`
CPP_template(typename Int, size_t NumIntegers)(
    requires ql::concepts::integral<Int>) constexpr std::
    array<Int, NumIntegers> integerToArray(Int value, Int numValues) {
  std::array<Int, NumIntegers> res{};
  for (auto& el : res | ql::views::reverse) {
    el = value % numValues;
    value /= numValues;
  }
  return res;
};

// Store the result of `integerToArray` in a `constexpr` variable which has
// linkage, and can therefore be used in C++17 mode as a `const&` template
// parameter.
template <typename Int, size_t NumIntegers, Int value, Int numValues>
constexpr inline std::array<Int, NumIntegers> integerToArrayStaticVar =
    integerToArray<Int, NumIntegers>(value, numValues);

// Return a `std::array<std::array<Int, Num>, pow(Upper, Num)>` (where `Int` is
// the type of `Upper`) that contains each
// value from `[0, ..., Upper - 1] ^ Num` exactly once. `^` denotes the
// cartesian power.
CPP_template(auto Upper, size_t Num)(
    requires ql::concepts::integral<
        decltype(Upper)>) constexpr auto cartesianPowerAsArray() {
  using Int = decltype(Upper);
  constexpr auto numValues = pow(Upper, Num);
  std::array<std::array<Int, Num>, numValues> arr{};
  for (Int i = 0; i < numValues; ++i) {
    arr[i] = integerToArray<Int, Num>(i, Upper);
  }
  return arr;
}

// Store the result of `cartesianPowerAsArray()` from above in a `constexpr`
// variable with linkage that can be used as a `const&` template parameter in
// C++17 mode.
CPP_template(auto Upper, size_t Num)(
    requires ql::concepts::integral<
        decltype(Upper)>) constexpr auto cartesianPowerAsArrayVal =
    cartesianPowerAsArray<Upper, Num>();

// Return a `ad_utility::ValueSequence<Int,...>` that contains each
// value from `[0, ..., Upper - 1] X Num` exactly once. `X` denotes the
// cartesian product of sets. The elements of the `integer_sequence` are
// of type `std::array<Int, Num>` where `Int` is the type of `Upper`.
CPP_template(auto Upper,
             size_t Num)(requires ql::concepts::integral<
                         decltype(Upper)>) auto cartesianPowerAsIntegerArray() {
  return toIntegerSequenceRef<cartesianPowerAsArrayVal<Upper, Num>>();
}

/*
@brief Call the given lambda function with each of the given types `Ts` as
explicit template parameter, keeping the same order.
*/
template <typename... Ts, typename F>
constexpr void forEachTypeInParameterPack(const F& lambda) {
  (lambda.template operator()<Ts>(), ...);
}

// Same as the function above, but the types are passed to the lambda as a first
// argument `ql::type_identity<T>{}`.
template <typename... Ts>
constexpr void forEachTypeInParameterPackWithTI(const auto& lambda) {
  (lambda(use_type_identity::ti<Ts>), ...);
}

/*
Implementation for `forEachTypeInTemplateType`.

In order to go through the types inside a templated type, we need to use
template type specialization.
*/
namespace detail {
template <class T>
struct forEachTypeInTemplateTypeImpl;

template <template <typename...> typename Template, typename... Ts>
struct forEachTypeInTemplateTypeImpl<Template<Ts...>> {
  template <typename F>
  constexpr void operator()(const F& lambda) const {
    forEachTypeInParameterPack<Ts...>(lambda);
  }
};

template <class T>
struct forEachTypeInTemplateTypeWithTIImpl;

template <template <typename...> typename Template, typename... Ts>
struct forEachTypeInTemplateTypeWithTIImpl<Template<Ts...>> {
  constexpr void operator()(const auto& lambda) const {
    forEachTypeInParameterPackWithTI<Ts...>(lambda);
  }
};
}  // namespace detail

/*
@brief Call the given lambda function with each type in the given instantiated
template type as explicit template parameter, keeping the same order.
*/
template <typename TemplateType, typename F>
constexpr void forEachTypeInTemplateType(const F& lambda) {
  detail::forEachTypeInTemplateTypeImpl<TemplateType>{}(lambda);
}

// Same as the function above, but the template type is passed in as a
// `ql::type_identity<TemplateType>`.
template <typename TemplateType>
constexpr void forEachTypeInTemplateTypeWithTI(
    use_type_identity::TI<TemplateType>, const auto& lambda) {
  detail::forEachTypeInTemplateTypeWithTIImpl<TemplateType>{}(lambda);
}

// Call the `lambda`, which takes an implicit template parameter of type `T` for
// each value in the `ValueSequence` that is being passed in as the first
// argument.
template <typename T, T... values, typename F>
constexpr void forEachValueInValueSequence(ValueSequence<T, values...>,
                                           F&& lambda) {
  (lambda.template operator()<values>(), ...);
}

// Same as above, but for `std::integer_sequence` arguments.
template <typename T, T... values, typename F>
constexpr void forEachValueInValueSequence(std::integer_sequence<T, values...>,
                                           F&& lambda) {
  (lambda.template operator()<values>(), ...);
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_CONSTEXPRUTILS_H
