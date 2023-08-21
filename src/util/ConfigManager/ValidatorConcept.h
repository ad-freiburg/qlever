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
    std::regular_invocable<Func, const ParameterTypes&...> &&
    std::same_as<std::invoke_result_t<Func, const ParameterTypes&...>, bool>;
}  // namespace ad_utility
