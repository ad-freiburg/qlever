// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_BACKPORTS_SPAN_H
#define QLEVER_SRC_BACKPORTS_SPAN_H

#include "backports/cppTemplate2.h"

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
span<std::byte, ::ranges::detail::byte_size<T>(N)> as_writeable_bytes(
    span<T, N> s) noexcept {
  return {reinterpret_cast<std::byte*>(s.data()), s.size_bytes()};
}

static constexpr auto dynamic_extent = ::ranges::dynamic_extent;
}  // namespace ql

#else
#include "backports/span.h"
namespace ql {
using std::as_bytes;
using std::as_writeable_bytes;
using std::span;
static constexpr auto dynamic_extent = std::dynamic_extent;
}  // namespace ql

#endif

#endif  // QLEVER_SRC_BACKPORTS_SPAN_H
