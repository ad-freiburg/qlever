//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

// TODO<joka921> comment;

namespace ad_utility {
template <bool b>
struct CopyableIf;

template <>
struct CopyableIf<true> {};

template <>
struct CopyableIf<false> {
  CopyableIf() = default;
  CopyableIf(const CopyableIf&) = delete;
  CopyableIf& operator=(const CopyableIf&) = delete;
  CopyableIf(CopyableIf&&) = default;
  CopyableIf& operator=(CopyableIf&&) = default;
};

template <bool b>
struct MovableIf;

template <>
struct MovableIf<true> {};

template <>
struct MovableIf<false> {
  MovableIf() = default;
  MovableIf(const MovableIf&) = default;
  MovableIf& operator=(const MovableIf&) = default;
  MovableIf(MovableIf&&) = delete;
  MovableIf& operator=(MovableIf&&) = delete;
};

};  // namespace ad_utility
