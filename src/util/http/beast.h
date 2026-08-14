//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_HTTP_BEAST_H
#define QLEVER_SRC_UTIL_HTTP_BEAST_H

// A convenience header that includes Boost::Asio and Boost::Beast, and defines
// the few constants that Boost::Asio doesn't deduce automatically (see below).
// In particular the constants for the coroutine support are NOT needed:
// sufficiently recent Boost versions define them themselves, and the old
// versions that we still support (down to Boost 1.71, see the `CPP17 libQLever`
// CI workflow) don't have coroutine support at all. Note that we must not force
// them for those old versions, because their `boost/asio/awaitable.hpp` then
// includes the Coroutines TS header `<experimental/coroutine>`, which is
// provided by neither libstdc++ nor recent libc++.

// Without explicitly including the `<utility>` header, an error occurs when
// compiling the `boost::asio` code included below with gcc 12. We hope and
// expect that this will go away with future version of `boost::asio`.
#include <utility>

// Needed for libc++ in C++20 mode, because std::result_of was removed.
#ifndef BOOST_ASIO_HAS_STD_INVOKE_RESULT
#define BOOST_ASIO_HAS_STD_INVOKE_RESULT
#endif

#include <boost/beast/version.hpp>

// Don't set header for boost beast 1.81 and forward, because it is noop there.
#if defined BOOST_BEAST_VERSION && BOOST_BEAST_VERSION < 345
#define BOOST_BEAST_USE_STD_STRING_VIEW
#endif

#include <boost/asio.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/beast.hpp>

// For boost versions prior to 1.81 this should be no-op
#if defined BOOST_BEAST_VERSION && BOOST_BEAST_VERSION < 345
constexpr std::string_view toStd(std::string_view view) { return view; }
#else
inline std::string_view toStd(boost::core::string_view view) { return view; }
#endif

#endif  // QLEVER_SRC_UTIL_HTTP_BEAST_H
