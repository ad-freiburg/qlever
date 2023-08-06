// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (August of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <concepts>
#include <variant>

namespace ad_utility {
// A validator function is any invocable object, who takes the given parameter
// types, in the same order and transformed to `const type&`, and returns a
// bool.
template <typename Func, typename... ParameterTypes>
concept Validator =
    std::regular_invocable<Func, const std::decay_t<ParameterTypes>&...> &&
    std::same_as<
        std::invoke_result_t<Func, const std::decay_t<ParameterTypes>&...>,
        bool>;

/*
The validator has only a single parameter and this parameter is contained in
a given list.
*/
template <typename Func, typename... Ts>
concept ValidatorWithSingleParameterTypeOutOfList =
    ((Validator<Func, Ts>) || ...);

namespace detail {
template <typename Func, typename T>
struct isValidatorWithSingleParameterTypeOutOfVariantImpl : std::false_type {};

template <typename Func, typename... Ts>
requires ValidatorWithSingleParameterTypeOutOfList<Func, Ts...>
struct isValidatorWithSingleParameterTypeOutOfVariantImpl<Func,
                                                          std::variant<Ts...>>
    : std::true_type {};
}  // namespace detail

// The validator has only a single parameter and this parameter is contained in
// a given variant.
template <typename Func, typename VariantType>
concept isValidatorWithSingleParameterTypeOutOfVariant =
    detail::isValidatorWithSingleParameterTypeOutOfVariantImpl<
        Func, VariantType>::value;
}  // namespace ad_utility
