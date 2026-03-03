// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_UTIL_CONCEPTS_H
#define QLEVER_SRC_UTIL_CONCEPTS_H

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
template <typename T>
CPP_concept Streamable = CPP_requires_ref(detail::is_streamable_, T);
}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_CONCEPTS_H
