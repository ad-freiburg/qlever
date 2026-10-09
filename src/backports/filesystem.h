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
// for the subset of functionality used by QLever. One notable difference is
// the `perms` enum, where `std::filesystem` uses the scoped enumerator
// `perms::none` while `boost::filesystem` uses the (unscoped)
// `perms::no_perms`. Use `ql::filesystem_perms_none` (defined below) instead of
// either. The other differences are bridged by the helpers below; note that
// these also have to work with Boost 1.71, the oldest version that QLever is
// tested with (see the `CPP17 libQLever` CI workflow).

#ifdef QLEVER_CPP_17
#include <boost/filesystem.hpp>
#include <boost/system/error_code.hpp>
#include <boost/version.hpp>
#include <string_view>
#else
#include <filesystem>
#include <system_error>
#endif

#include <utility>

// `boost::filesystem::path` can only be constructed (and assigned, appended,
// and concatenated) from a `std::string_view` since Boost 1.81. For older Boost
// versions we opt in to that support by specializing the `is_pathable` trait,
// which is the customization point that all those member templates are
// constrained on. The corresponding `dispatch()` overload for arbitrary
// containers of characters already exists, so the specialization is all that is
// needed. Note that `is_pathable` was removed in Boost 1.81 (together with the
// introduction of the native `std::string_view` support), hence the version
// check.
#if defined(QLEVER_CPP_17) && BOOST_VERSION < 108100
namespace boost::filesystem::path_traits {
template <>
struct is_pathable<std::string_view> {
  static const bool value = true;
};
}  // namespace boost::filesystem::path_traits
#endif

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

// Return true if `entry` is a regular file resp. a directory, and false if the
// status of `entry` cannot be determined, in particular if it has been deleted
// since the directory was iterated. The member functions
// `directory_entry::is_regular_file()` and `directory_entry::is_directory()`
// only exist in `std::filesystem` and in recent versions of
// `boost::filesystem`, so the `status()` of the entry is used instead.
//
// NOTE: The `error_code` overload of `status()` is essential. Its throwing
// counterpart throws (instead of reporting a "not found" status) when the entry
// no longer exists, and `boost::filesystem` actually hits that case, because it
// stats the entry lazily when `status()` is called and not while iterating the
// directory. Callers typically iterate a directory that other processes write
// to, where an entry disappearing is normal and must not throw.
inline bool isRegularFile(const filesystem::directory_entry& entry) {
  error_code ignoredError;
  return filesystem::is_regular_file(entry.status(ignoredError));
}
inline bool isDirectory(const filesystem::directory_entry& entry) {
  error_code ignoredError;
  return filesystem::is_directory(entry.status(ignoredError));
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
