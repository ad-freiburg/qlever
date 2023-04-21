// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

// Type traits for template metaprogramming

#pragma once
#include <tuple>
#include <utility>
#include <variant>
#include <vector>

namespace ad_utility {

namespace detail {
// Check (via the compiler's template matching mechanism) whether a given type
// is an instantiation of a given template.
// Example 1: IsInstantiationOf<std::vector>::Instantiation<std::vector<int>>
// is true.
// Example 2: IsInstantiationOf<std::vector>::Instantiation<std:map<int, int>>
// is false
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
/// isInstantiation<std::vector, std::vector<int>> == true;
/// isInstantiation<std::vector, const std::vector<int>&> == false;
template <typename T, template <typename...> typename TemplatedType>
concept isInstantiation =
    detail::IsInstantiationOf<TemplatedType>::template Instantiation<T>::value;

/// The concept is fulfilled iff `T` is `ad_utility::SimilarTo` an
/// instantiation of `TemplatedType`. Examples:
///
/// similarToInstantiation<std::vector, std::vector<int>> == true;
/// similarToInstantiation<std::vector, const std::vector<int>&> == true;
template <typename T, template <typename...> typename TemplatedType>
concept similarToInstantiation =
    isInstantiation<std::decay_t<T>, TemplatedType>;

/// isVector<T> is true if and only if T is an instantiation of std::vector
template <typename T>
constexpr static bool isVector = isInstantiation<T, std::vector>;

/// isTuple<T> is true if and only if T is an instantiation of std::tuple
template <typename T>
constexpr static bool isTuple = isInstantiation<T, std::tuple>;

/// isVariant<T> is true if and only if T is an instantiation of std::variant
template <typename T>
constexpr static bool isVariant = isInstantiation<T, std::variant>;

/// Two types are similar, if they are the same when we remove all cv (const or
/// volatile) qualifiers and all references
template <typename T, typename U>
constexpr static bool isSimilar =
    std::is_same_v<std::decay_t<T>, std::decay_t<U>>;

/// A concept for similarity
template <typename T, typename U>
concept SimilarTo = isSimilar<T, U>;

/// isTypeContainedIn<T, U> It is true iff type U is a pair, tuple or variant
/// and T `isSimilar` (see above) to one of the types contained in the tuple,
/// pair or variant.
template <typename... Ts>
constexpr static bool isTypeContainedIn = false;

template <typename T, typename... Ts>
constexpr static bool isTypeContainedIn<T, std::tuple<Ts...>> =
    (... || isSimilar<T, Ts>);

template <typename T, typename... Ts>
constexpr static bool isTypeContainedIn<T, std::variant<Ts...>> =
    (... || isSimilar<T, Ts>);

template <typename T, typename... Ts>
constexpr static bool isTypeContainedIn<T, std::pair<Ts...>> =
    (... || isSimilar<T, Ts>);

/// A templated bool that is always false,
/// independent of the template parameter.
template <typename>
constexpr static bool alwaysFalse = false;

/// From the type Tuple (std::tuple<A, B, C....>) creates the type
/// std::tuple<TypeLifter<A>, TypeLifter<B>,...>
template <typename Tuple, template <typename> typename TypeLifter>
requires isTuple<Tuple> using LiftedTuple = typename detail::LiftInnerTypes<
    std::tuple, TypeLifter>::template TypeToLift<Tuple>::LiftedType;

/// From the type Variant (std::variant<A, B, C....>) creates the type
/// std::variant<TypeLifter<A>, TypeLifter<B>,...>
template <typename Variant, template <typename> typename TypeLifter>
requires isVariant<Variant>
using LiftedVariant = typename detail::LiftInnerTypes<
    std::variant, TypeLifter>::template TypeToLift<Variant>::LiftedType;

/// From the type std::tuple<A, B, ...> makes the type std::variant<A, B, ...>
template <typename Tuple>
requires isTuple<Tuple>
using TupleToVariant = typename detail::TupleToVariantImpl<Tuple>::type;

/// From the types X = std::tuple<A, ... , B>, , Y = std::tuple<C, ..., D>...
/// makes the type TupleCat<X, Y> = std::tuple<A, ..., B, C, ..., D, ...> (works
/// for an arbitrary number of tuples as template parameters.
template <typename... Tuples>
requires(... && isTuple<Tuples>)
using TupleCat = decltype(std::tuple_cat(std::declval<Tuples&>()...));

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
        if constexpr (isVariant<T>) {
          return std::forward<T>(t);
        } else {
          return std::variant<std::decay_t<T>>{std::forward<T>(t)};
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
requires(sizeof...(Ts) > 0) using Last = typename detail::LastT<Ts...>::type;

// Return the first type of variadic template arguments.
template <typename... Ts>
requires(sizeof...(Ts) > 0)
using First = typename detail::FirstWrapper<Ts...>::type;

}  // namespace ad_utility
