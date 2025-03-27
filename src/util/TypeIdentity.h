// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2024 Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)
//        Fabian Krause (fabian.krause@students.uni-freiburg.de)
//
// The use case of the following aliases and constants is as follows:
// Locally (inside a function) put a `using namespace
// ad_utility::use_type_identity`, then use `TI<someType>` as a convenient short
// cut for `std::type_identity<someType>`. This can be used to make template
// arguments to local lambdas more readable, as C++ doesn't allow for local
// templates. For example:
//
// auto f1 = []<typename T>() {/* use T */};
// auto f2 = []<typename T>(TI<T>) { /* use T /*};
// f1.template operator()<int>(); // Works but is rather unreadable.
// f2(ti<int>); // Minimal visual overhead for specifying the template
// parameter.

#pragma once

#include <type_traits>

namespace ad_utility::use_type_identity {
template <typename T>
using TI = std::type_identity<T>;

template <typename T>
static constexpr auto ti = TI<T>{};
}  // namespace ad_utility::use_type_identity
