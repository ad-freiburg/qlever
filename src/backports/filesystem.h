// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_BACKPORTS_FILESYSTEM_H
#define QLEVER_SRC_BACKPORTS_FILESYSTEM_H

// This file defines the `ql::filesystem` namespace as a drop-in replacement for
// `std::filesystem`. By default (in C++20 mode) it is a simple alias for
// `std::filesystem`. If `QLEVER_CPP_17` is defined (the C++17 backports mode
// that also uses `range-v3`), it aliases `boost::filesystem` instead, because
// some toolchains targeted in that mode (e.g. QCC 8 used in QNX) don't provide
// a usable `std::filesystem`. Note that the backported version requires linking
// against `Boost::filesystem`.
//
// The APIs of `std::filesystem` and `boost::filesystem` are almost identical
// for the subset of functionality used by QLever. The one notable difference is
// the `perms` enum, where `std::filesystem` uses the scoped enumerator
// `perms::none` while `boost::filesystem` uses the (unscoped)
// `perms::no_perms`. Use `ql::filesystem_perms_none` (defined below) instead of
// either.

#ifdef QLEVER_CPP_17
#include <boost/filesystem.hpp>
#include <boost/system/error_code.hpp>
#else
#include <filesystem>
#include <system_error>
#endif

#include <utility>

namespace ql {
#ifdef QLEVER_CPP_17
namespace filesystem = ::boost::filesystem;
#else
namespace filesystem = ::std::filesystem;
#endif

// A drop-in replacement for `std::error_code`. The `error_code`-taking
// overloads of `boost::filesystem` expect a `boost::system::error_code`, not a
// `std::error_code`, so in the C++17 backports mode we alias the former. Both
// types provide the `operator bool` and `message()` members that QLever uses.
#ifdef QLEVER_CPP_17
using error_code = ::boost::system::error_code;
#else
using error_code = ::std::error_code;
#endif

// The `perms` value that represents "no permissions", spelled `perms::none` in
// `std::filesystem` and `perms::no_perms` in `boost::filesystem`.
#ifdef QLEVER_CPP_17
static constexpr auto filesystem_perms_none =
    ::boost::filesystem::perms::no_perms;
#else
static constexpr auto filesystem_perms_none = ::std::filesystem::perms::none;
#endif

// A range over the entries of a directory. Prefer this over using a
// `ql::filesystem::directory_iterator` directly as a range (for example in a
// `ql::ranges` algorithm). In the C++17 backports mode `directory_iterator` is
// `boost::filesystem::directory_iterator`, whose non-member `begin`/`end` take
// the iterator by `const&` and therefore lose in overload resolution against
// `range-v3`'s deleted poison-pill `begin`/`end` overloads. As a result the
// iterator is not recognized as a range by `range-v3`. This wrapper instead
// exposes member `begin()`/`end()`, which sidesteps the problem and works with
// both `std::filesystem` and `boost::filesystem`.
class DirectoryRange {
 private:
  filesystem::path directory_;

 public:
  explicit DirectoryRange(filesystem::path directory)
      : directory_{std::move(directory)} {}

  filesystem::directory_iterator begin() const {
    return filesystem::directory_iterator{directory_};
  }
  filesystem::directory_iterator end() const {
    return filesystem::directory_iterator{};
  }
};

// Return a `DirectoryRange` over the entries of `directory`.
inline DirectoryRange directoryRange(filesystem::path directory) {
  return DirectoryRange{std::move(directory)};
}

// Return the filename component of `path`, with `std::filesystem` semantics
// even in the C++17 backports mode. A path with a trailing directory separator
// has no filename component: `std::filesystem::path::filename()` returns an
// empty path for such a path, whereas `boost::filesystem::path::filename()`
// returns ".". Normalize to the `std::filesystem` behavior so that callers
// behave identically for both backends.
inline filesystem::path pathFilename(const filesystem::path& path) {
  const auto& native = path.native();
  if (!native.empty() &&
      native.back() == filesystem::path::preferred_separator) {
    return {};
  }
  return path.filename();
}
}  // namespace ql

#endif  // QLEVER_SRC_BACKPORTS_FILESYSTEM_H
