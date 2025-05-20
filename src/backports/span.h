// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_BACKPORTS_SPAN_H
#define QLEVER_SRC_BACKPORTS_SPAN_H

// This header implements the following types and functions, all related to
// spans:
// - ql::span
// - ql::as_bytes
// - ql::as_writeable_bytes
// - ql::dynamic_extent
// In C++20 mode (by default) they are aliases for the corresponding types and
// functions from the standard library, in C++17 mode (when `QLEVER_CPP_17` is
// enabled) they are implemented using `range-v3`.
// Note that we use a fork of range-v3 that makes the span implementations very
// similar, but some small differences remain, In particular the deduction
// guides for the construction from `std::array` which leads to a static extent
// are missing.

#ifdef QLEVER_CPP_17
#include <cstddef>
#include <range/v3/view/span.hpp>
namespace ql {
using ::ranges::span;

template <typename T, ::ranges::detail::span_index_t N>
span<std::byte const, ::ranges::detail::byte_size<T>(N)> as_bytes(
    span<T, N> s) noexcept {
  return {reinterpret_cast<std::byte const*>(s.data()), s.size_bytes()};
}

template <typename T, ::ranges::detail::span_index_t N>
span<std::byte, ::ranges::detail::byte_size<T>(N)> as_writable_bytes(
    span<T, N> s) noexcept {
  return {reinterpret_cast<std::byte*>(s.data()), s.size_bytes()};
}

static constexpr auto dynamic_extent = ::ranges::dynamic_extent;
}  // namespace ql

#else
#include <span>
namespace ql {
using std::as_bytes;
using std::as_writable_bytes;
using std::span;
static constexpr auto dynamic_extent = std::dynamic_extent;
}  // namespace ql

#endif

#endif  // QLEVER_SRC_BACKPORTS_SPAN_H
