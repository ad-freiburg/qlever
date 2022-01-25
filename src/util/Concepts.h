// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <sstream>

namespace ad_utility {
/**
 * A concept to ensure objects can be formatted by std::ostream.
 * @tparam T The Type to be formatted
 */
template <typename T>
concept Streamable = requires(T x, std::ostream& os) {
  os << x;
};
}  // namespace ad_utility
