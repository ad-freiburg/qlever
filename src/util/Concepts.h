// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <sstream>

#include "backports/concepts.h"

namespace ad_utility {

namespace detail {
template <typename T>
CPP_requires(is_streamable_, requires(T x, std::ostream& os)(os << x));
}

/**
 * A concept to ensure objects can be formatted by std::ostream.
 * @tparam T The Type to be formatted
 */
// TODO<joka921, gpcicciuca> turn back to `CPP_concept` and fix the remaining
// places.
template <typename T>
concept Streamable = CPP_requires_ref(detail::is_streamable_, T);
}  // namespace ad_utility
