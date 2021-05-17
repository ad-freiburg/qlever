// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

// Type traits for template metaprogramming

#pragma once
namespace ad_utility {

/// isVector<T> is true if and only if T is an instantiation of std::vector
template <typename T, typename Alloc=void>
constexpr bool isVector = false;

template <typename T, typename Alloc>
constexpr bool isVector<std::vector<T, Alloc>> = true;

}
