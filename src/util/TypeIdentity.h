// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2024      Fabian Krause (fabian.krause@students.uni-freiburg.de)

#pragma once

#include <type_traits>

namespace ad_utility::use_type_identity {
template <typename T>
static constexpr auto TI = std::type_identity<T>{};
}
