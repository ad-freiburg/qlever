// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_BACKPORTS_FILESYSTEM_H
#define QLEVER_SRC_BACKPORTS_FILESYSTEM_H

// This file defines the `ql::filesystem` namespace as a drop-in replacement for
// `std::filesystem`. By default (in C++20 mode) it is a simple alias for
// `std::filesystem`. If `QLEVER_CPP_17` is defined (the C++17 backports mode
// that also uses `range-v3`), it aliases `boost::filesystem` instead, because
// the compilers targeted in that mode (e.g. GCC 8) don't provide a usable
// `std::filesystem`. Note that the backported version requires linking against
// `Boost::filesystem`.
//
// The APIs of `std::filesystem` and `boost::filesystem` are almost identical
// for the subset of functionality used by QLever. The one notable difference is
// the `perms` enum, where `std::filesystem` uses the scoped enumerator
// `perms::none` while `boost::filesystem` uses the (unscoped)
// `perms::no_perms`. Use `ql::filesystem_perms_none` (defined below) instead of
// either.

#ifdef QLEVER_CPP_17
#include <boost/filesystem.hpp>
#else
#include <filesystem>
#endif

namespace ql {
#ifdef QLEVER_CPP_17
namespace filesystem = ::boost::filesystem;
#else
namespace filesystem = ::std::filesystem;
#endif

// The `perms` value that represents "no permissions", spelled `perms::none` in
// `std::filesystem` and `perms::no_perms` in `boost::filesystem`.
#ifdef QLEVER_CPP_17
static constexpr auto filesystem_perms_none =
    ::boost::filesystem::perms::no_perms;
#else
static constexpr auto filesystem_perms_none = ::std::filesystem::perms::none;
#endif
}  // namespace ql

#endif  // QLEVER_SRC_BACKPORTS_FILESYSTEM_H
