//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

// A convenience header that includes Boost::Asio and Boost::Beast,
// and defines several constants to make Boost::Asio compile
// with coroutine support on G++/libstdc++ and clang++/libc++
// (TODO<joka921> Figure out, why Boost currently is not able, to deduce
// these automatically.

// Without explicitly including the `<utility>` header, an error occurs when
// compiling the `boost::asio` code included below with gcc 12. We hope and
// expect that this will go away with future version of `boost::asio`.
#include <utility>

#include "util/Coroutines.h"

// libc++ needs <experimental/coroutine>, libstdc++ needs <coroutine>
#ifndef BOOST_ASIO_HAS_CO_AWAIT
#define BOOST_ASIO_HAS_CO_AWAIT
#endif
#ifndef BOOST_ASIO_HAS_STD_COROUTINE
#define BOOST_ASIO_HAS_STD_COROUTINE
#endif

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
