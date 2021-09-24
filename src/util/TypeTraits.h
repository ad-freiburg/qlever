// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

// Type traits for template metaprogramming

#pragma once
#include <tuple>
#include <utility>
#include <variant>

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

}  // namespace detail

/// isInstantiation<SomeTemplate, SomeType> is true iff `SomeType` is an
/// instantiation of the template `SomeTemplate`. This can be used to define
/// concepts, see below.
template <template <typename...> typename Template, typename T>
constexpr static bool isInstantiation =
    detail::IsInstantiationOf<Template>::template Instantiation<T>::value;

/// isVector<T> is true if and only if T is an instantiation of std::vector
template <typename T>
constexpr static bool isVector = isInstantiation<std::vector, T>;

/// isTuple<T> is true if and only if T is an instantiation of std::tuple
template <typename T>
constexpr static bool isTuple = isInstantiation<std::tuple, T>;

/// isVariant<T> is true if and only if T is an instantiation of std::variant
template <typename T>
constexpr static bool isVariant = isInstantiation<std::variant, T>;

/// Two types are similar, if they are the same when we remove all cv (const or
/// volatile) qualifiers and all references
template <typename T, typename U>
constexpr static bool isSimilar =
    std::is_same_v<std::decay_t<T>, std::decay_t<U>>;

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

/// A templated bool that is always false, independent of the template parameter
/// T.
template <typename T>
constexpr static bool alwaysFalse = false;

/// From the type Tuple (std::tuple<A, B, C....>) creates the type
/// std::tuple<TypeLifter<A>, TypeLifter<B>,...>
template <typename Tuple, template <typename> typename TypeLifter>
requires isTuple<Tuple> using LiftedTuple = typename detail::LiftInnerTypes<
    std::tuple, TypeLifter>::template TypeToLift<Tuple>::LiftedType;

/// From the type Variant (std::variant<A, B, C....>) creates the type
/// std::variant<TypeLifter<A>, TypeLifter<B>,...>
template <typename Variant, template <typename> typename TypeLifter>
requires isVariant<Variant> using LiftedVariant =
    typename detail::LiftInnerTypes<
        std::variant, TypeLifter>::template TypeToLift<Variant>::LiftedType;

/// From the type std::tuple<A, B, ...> makes the type std::variant<A, B, ...>
template <typename Tuple>
requires isTuple<Tuple> using TupleToVariant =
    typename detail::TupleToVariantImpl<Tuple>::type;

/// From the types X = std::tuple<A, ... , B>, , Y = std::tuple<C, ..., D>...
/// makes the type TupleCat<X, Y> = std::tuple<A, ..., B, C, ..., D, ...> (works
/// for an arbitrary number of tuples as template parameters.
template <typename... Tuples>
requires(...&& isTuple<Tuples>) using TupleCat =
    decltype(std::tuple_cat(std::declval<Tuples&>()...));
}  // namespace ad_utility
