// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

// Type traits for template metaprogramming

#pragma once
#include <concepts>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "backports/concepts.h"
#include "util/Forward.h"

namespace ad_utility {

namespace detail {
/*
Check (via the compiler's template matching mechanism) whether a given type
is an instantiation of a given template.
This also works with aliases, that were created using `using`.
For example:
For
```
template <typenanme... Ts>
using A = B<Ts...>;
```
the 'call' `IsInstantiationOf<A>::Instantiation<B<int>>::value` and
`IsInstantiationOf<B>::Instantiation<B<int>>::value` would both return true.
*/
template <template <typename...> typename Template>
struct IsInstantiationOf {
  template <typename T>
  struct Instantiation : std::false_type {};

  template <typename... Ts>
  struct Instantiation<Template<Ts...>> : std::true_type {};
};

// Given a templated type (e.g. std::variant<A, B, C>), provide a type where the
// inner types are "lifted" by a given outer type.
// Example: LiftInnerTypes<std::variant,
// std::vector>::TypeToLift<std::variant<A, B, C>>::LiftedType is
// std::variant<std::vector<A>, std::vector<B>, std::vector<C>>
template <template <typename...> typename Template,
          template <typename> typename TypeLifter>
struct LiftInnerTypes {
  template <typename>
  struct TypeToLift {};

  template <typename... InnerTypes>
  struct TypeToLift<Template<InnerTypes...>> {
    using LiftedType = Template<TypeLifter<InnerTypes>...>;
  };
};

// TupleToVariantImpl<std::tuple<A, B, ...>>::type = std::variant<A, B, C>
template <typename T>
struct TupleToVariantImpl {};

template <typename... Ts>
struct TupleToVariantImpl<std::tuple<Ts...>> {
  using type = std::variant<Ts...>;
};

// Implementation for Last
template <typename, typename... Ts>
struct LastT : LastT<Ts...> {};

template <typename T>
struct LastT<T> : public std::type_identity<T> {};

// Implementation for First
template <typename T, typename...>
struct FirstWrapper : public std::type_identity<T> {};

}  // namespace detail

/// The concept is fulfilled iff `T` is an instantiation of `TemplatedType`.
/// Examples:
///
/// isInstantiation<std::vector<int>, std::vector> == true;
/// isInstantiation<const std::vector<int>&, std::vector> == false;
template <typename T, template <typename...> typename TemplatedType>
CPP_concept isInstantiation =
    detail::IsInstantiationOf<TemplatedType>::template Instantiation<T>::value;

/// The concept is fulfilled iff `T` is `ad_utility::SimilarTo` an
/// instantiation of `TemplatedType`. Examples:
///
/// similarToInstantiation<std::vector, std::vector<int>> == true;
/// similarToInstantiation<std::vector, const std::vector<int>&> == true;
template <typename T, template <typename...> typename TemplatedType>
CPP_concept similarToInstantiation =
    isInstantiation<std::decay_t<T>, TemplatedType>;

/// @brief The concept is fulfilled if `T` is an instantiation of any
/// of the types passed in ...Ts
///
/// similarToAnyInstantiationOf<std::vector, std::vector<int>,
/// std::vector<char>> == true
template <typename T, template <typename...> typename... Ts>
CPP_concept similarToAnyInstantiationOf =
    (... || similarToInstantiation<T, Ts>);

/// isVector<T> is true if and only if T is an instantiation of std::vector
template <typename T>
constexpr static bool isVector = isInstantiation<T, std::vector>;

/// isTuple<T> is true if and only if T is an instantiation of std::tuple
template <typename T>
CPP_concept isTuple = isInstantiation<T, std::tuple>;

/// isVariant<T> is true if and only if T is an instantiation of std::variant
template <typename T>
constexpr static bool isVariant = isInstantiation<T, std::variant>;

/// isArray<T> is true if and only if `T` is an instantiation of `std::array`.
template <typename T>
constexpr static bool isArray = false;

template <typename T, size_t N>
constexpr static bool isArray<std::array<T, N>> = true;

// `SimilarToArray` is also true for `std::array<...>&`, etc.
template <typename T>
CPP_concept SimilarToArray = isArray<std::decay_t<T>>;

/// Two types are similar, if they are the same when we remove all cv (const or
/// volatile) qualifiers and all references
template <typename T, typename U>
constexpr static bool isSimilar =
    std::is_same_v<std::decay_t<T>, std::decay_t<U>>;

/// A concept for similarity
template <typename T, typename U>
CPP_concept SimilarTo = isSimilar<T, U>;

/// True iff `T` is similar (see above) to any of the `Ts...`.
template <typename T, typename... Ts>
CPP_concept SimilarToAny = (... || isSimilar<T, Ts>);

/// True iff `T` is the same as any of the `Ts...`.
template <typename T, typename... Ts>
CPP_concept SameAsAny = (... || ql::concepts::same_as<T, Ts>);

/*
The implementation for `SimilarToAnyTypeIn` and `SameAsAnyTypeIn` (see below
namespace detail).
*/
namespace detail {
template <typename T, typename Template>
struct SimilarToAnyTypeInImpl : std::false_type {};

template <typename T, template <typename...> typename Template, typename... Ts>
struct SimilarToAnyTypeInImpl<T, Template<Ts...>>
    : std::integral_constant<bool, SimilarToAny<T, Ts...>> {};

template <typename T, typename Template>
struct SameAsAnyTypeInImpl : std::false_type {};

template <typename T, template <typename...> typename Template, typename... Ts>
struct SameAsAnyTypeInImpl<T, Template<Ts...>>
    : std::integral_constant<bool, SameAsAny<T, Ts...>> {};

}  // namespace detail
/*
`SimilarToAnyTypeIn<T, U>` is true, iff type `U` is an instantiation of a
template that only has template type parameters (e.g. `std::pair`, `std::tuple`
or `std::variant`). and `T` is `isSimilar` (see above) to any of the type
parameters.
*/
template <typename T, typename Template>
CPP_concept SimilarToAnyTypeIn =
    detail::SimilarToAnyTypeInImpl<T, Template>::value;

/*
Equivalent to `SimilarToAnyTypeIn` (see above), but checks for exactly matching
types via `std::same_as`.
*/
template <typename T, typename Template>
CPP_concept SameAsAnyTypeIn = detail::SameAsAnyTypeInImpl<T, Template>::value;

/// A templated bool that is always false,
/// independent of the template parameter.
template <typename>
constexpr static bool alwaysFalse = false;

/// From the type Tuple (std::tuple<A, B, C....>) creates the type
/// std::tuple<TypeLifter<A>, TypeLifter<B>,...>
CPP_template(typename Tuple,
             template <typename>
             typename TypeLifter)(requires isTuple<Tuple>) using LiftedTuple =
    typename detail::LiftInnerTypes<
        std::tuple, TypeLifter>::template TypeToLift<Tuple>::LiftedType;

/// From the type Variant (std::variant<A, B, C....>) creates the type
/// std::variant<TypeLifter<A>, TypeLifter<B>,...>
CPP_template(typename Variant, template <typename> typename TypeLifter)(
    requires isVariant<Variant>) using LiftedVariant =
    typename detail::LiftInnerTypes<
        std::variant, TypeLifter>::template TypeToLift<Variant>::LiftedType;

/// From the type std::tuple<A, B, ...> makes the type std::variant<A, B, ...>
CPP_template(typename Tuple)(requires isTuple<Tuple>) using TupleToVariant =
    typename detail::TupleToVariantImpl<Tuple>::type;

/// From the types X = std::tuple<A, ... , B>, , Y = std::tuple<C, ..., D>...
/// makes the type TupleCat<X, Y> = std::tuple<A, ..., B, C, ..., D, ...> (works
/// for an arbitrary number of tuples as template parameters.
template <typename... Tuples>
using TupleCat =
    std::enable_if_t<(isTuple<Tuples> && ...),
                     decltype(std::tuple_cat(std::declval<Tuples&>()...))>;

/// A generalized version of std::visit that also supports non-variant
/// parameters. Each `parameterOrVariant` of type T that is not a std::variant
/// is converted to a `std::variant<T>`. Each `parameterOrVariant` that is a
/// std::variant is left as-is. Then std::visit is called on the passed
/// `Function` and the transformed parameters (which now are all variants). This
/// has the effect that `Function` is called on the values contained in the
/// passed variants and additionally with the non-variant parameters. Note that
/// this currently only supports variant types that are direct instantiations of
/// `std::variant`. If you want a different behavior, etc. also visit types that
/// inherit from std::variant, or treat a parameter that is a std::variant as an
/// "ordinary" parameter to the `Function`, you cannot use this function.
inline auto visitWithVariantsAndParameters =
    []<typename Function, typename... ParametersOrVariants>(
        Function&& f, ParametersOrVariants&&... parametersOrVariants) {
      auto liftToVariant = []<typename T>(T&& t) {
        if constexpr (isVariant<std::decay_t<T>>) {
          return AD_FWD(t);
        } else {
          return std::variant<std::decay_t<T>>{AD_FWD(t)};
        }
      };
      return std::visit(f, liftToVariant(std::forward<ParametersOrVariants>(
                               parametersOrVariants))...);
    };

/// Apply `Function f` to each element of tuple. Returns a tuple of the results.
/// Note: 1. The `Function` must not return void (otherwise this doesn't
/// compile)
///       2. The order in which the function is called is unspecified.
template <typename Function, typename Tuple>
auto applyFunctionToEachElementOfTuple(Function&& f, Tuple&& tuple) {
  auto transformer =
      [f = std::forward<Function>(f)]<typename... Args>(Args&&... args) {
        return std::tuple{f(std::forward<Args>(args))...};
      };
  return std::apply(transformer, std::forward<Tuple>(tuple));
}

// Return the last type of variadic template arguments.
template <typename... Ts>
using Last = typename std::enable_if_t<(sizeof...(Ts) > 0),
                                       typename detail::LastT<Ts...>::type>;

// Return the first type of variadic template arguments.
template <typename... Ts>
using First =
    typename std::enable_if_t<(sizeof...(Ts) > 0),
                              typename detail::FirstWrapper<Ts...>::type>;

/// Concept for `std::is_invocable_r_v`.
template <typename Func, typename R, typename... ArgTypes>
CPP_concept InvocableWithConvertibleReturnType =
    std::is_invocable_r_v<R, Func, ArgTypes...>;

// The implementation of `InvokeResultSfinaeFriendly`
namespace invokeResultSfinaeFriendly::detail {
template <typename T, typename... Args>
struct InvalidInvokeResult {
  InvalidInvokeResult() = delete;
  ~InvalidInvokeResult() = delete;
};
template <typename T, typename... Args>
constexpr auto getInvokeResultImpl() {
  if constexpr (std::is_invocable_v<T, Args...>) {
    return std::type_identity<std::invoke_result_t<T, Args...>>{};
  } else {
    return std::type_identity<InvalidInvokeResult<T, Args...>>{};
  }
}
}  // namespace invokeResultSfinaeFriendly::detail

// If `std::is_invocable_v<T, Args...>`, then this is `std::invoke_result_t<T ,
// Args...>` , otherwise it is an internal type that can be neither constructed
// nor destructed. This is useful in SFINAE-contexts, where
// `std::invoke_result_t` leads to hard compiler errors if it doesn't exist.
template <typename T, typename... Args>
using InvokeResultSfinaeFriendly =
    typename decltype(invokeResultSfinaeFriendly::detail::getInvokeResultImpl<
                      T, Args...>())::type;

/*
The following concepts are similar to `std::is_invocable_r_v` with the following
difference: `std::is_invocable_r_v` only checks that the return type of a
function is convertible to the specified type, but the following concepts check
for an exact match of the return type (`InvocableWithExactReturnType`) or a
similar return type (`invocablewithsimilarreturntype`), meaning that the types
are the same when ignoring `const`, `volatile`, and reference qualifiers.
*/

/*
@brief Require `Fn` to be invocable with `Args...` and the return type to be
`isSimilar` to `Ret`.
*/
template <typename Fn, typename Ret, typename... Args>
CPP_concept InvocableWithSimilarReturnType =
    std::invocable<Fn, Args...> &&
    isSimilar<InvokeResultSfinaeFriendly<Fn, Args...>, Ret>;

/*
@brief Require `Fn` to be invocable with `Args...` and the return type to be
 `Ret`.
*/
// TODO<joka921, gpcicciuca> turn back to `concept` and fix the remaining
// places.
template <typename Fn, typename Ret, typename... Args>
CPP_concept InvocableWithExactReturnType =
    std::invocable<Fn, Args...> &&
    std::same_as<InvokeResultSfinaeFriendly<Fn, Args...>, Ret>;

/*
@brief Require `Fn` to be regular invocable with `Args...` and the return type
to be `isSimilar` to `Ret`.

Note: Currently, the difference between invocable and regular invocable is
purely semantic. In other words, we can not, currently, actually check, if an
invocable type is regular invocable, or not.
For more information see: https://en.cppreference.com/w/cpp/concepts/invocable
*/
template <typename Fn, typename Ret, typename... Args>
CPP_concept RegularInvocableWithSimilarReturnType =
    std::regular_invocable<Fn, Args...> &&
    isSimilar<InvokeResultSfinaeFriendly<Fn, Args...>, Ret>;

/*
@brief Require `Fn` to be regular invocable with `Args...` and the return type
to be `Ret`.

Note: Currently, the difference between invocable and regular invocable is
purely semantic. In other words, we can not, currently, actually check, if an
invocable type is regular invocable, or not.
For more information see: https://en.cppreference.com/w/cpp/concepts/invocable
*/
template <typename Fn, typename Ret, typename... Args>
CPP_concept RegularInvocableWithExactReturnType =
    std::regular_invocable<Fn, Args...> &&
    std::same_as<InvokeResultSfinaeFriendly<Fn, Args...>, Ret>;

// True iff `T` is a value type or an rvalue reference. Can be used to force
// rvalue references for templated functions: For example: void f(auto&& x) //
// might be lvalue, because the && denotes a forwarding reference. void f(Rvalue
// auto&& x) // guaranteed rvalue reference, can safely be moved.
template <typename T>
CPP_concept Rvalue = std::is_rvalue_reference_v<T&&>;
}  // namespace ad_utility
