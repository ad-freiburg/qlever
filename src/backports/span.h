// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_BACKPORTS_SPAN
#define QLEVER_SRC_BACKPORTS_SPAN

#include "backports/cppTemplate2.h"

#ifndef QLEVER_CPP_17
// In C++20 mode, use std::span
#include <span>
namespace ql {
using std::span;
}  // namespace ql

#else
// In C++17 mode, use range-v3's span implementation
#include <range/v3/view/span.hpp>
namespace ql {
using ranges::span;
}  // namespace ql

#endif

#endif  // QLEVER_SRC_BACKPORTS_SPAN
