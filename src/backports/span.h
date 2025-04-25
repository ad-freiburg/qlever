// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_BACKPORTS_SPAN_H
#define QLEVER_SRC_BACKPORTS_SPAN_H

#include "backports/cppTemplate2.h"

#ifdef QLEVER_CPP_17
#include <range/v3/view/span.hpp>
namespace ql {
using ::ranges::span;
static constexpr auto dynamic_extent = ::ranges::dynamic_extent;
}  // namespace ql

#else
#include "backports/span.h"
namespace ql {
using std::span;
static constexpr auto dynamic_extent = std::dynamic_extent;
}  // namespace ql

#endif

#endif  // QLEVER_SRC_BACKPORTS_SPAN_H
