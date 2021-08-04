// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

// Type traits for template metaprogramming

#pragma once
namespace ad_utility {

namespace detail {
template <template <typename...> typename Template>
struct IsInstantiationOfImpl {
  template <typename T>
  struct Inner : std::false_type {};

  template <typename... Ts>
  struct Inner<Template<Ts...>> : std::true_type {};
};
}  // namespace detail

/// IsInstantiation<someTemplate, someType>::value is true iff `someType` is an
/// instantiation of the template `someTemplate`. This can be used to define
/// concepts, see below
template <template <typename...> typename Template, typename T>
using IsInstantiation =
    typename detail::IsInstantiationOfImpl<Template>::template Inner<T>;

/// isVector<T> is true if and only if T is an instantiation of std::vector
template <typename T>
constexpr static bool isVector = IsInstantiation<std::vector, T>::value;
}  // namespace ad_utility
