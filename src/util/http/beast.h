//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_HTTP_BEAST_H
#define QLEVER_SRC_UTIL_HTTP_BEAST_H

// A convenience header that includes Boost::Asio and Boost::Beast, and defines
// the few constants that Boost::Asio doesn't deduce automatically (see below).

// Without explicitly including the `<utility>` header, an error occurs when
// compiling the `boost::asio` code included below with gcc 12. We hope and
// expect that this will go away with future version of `boost::asio`.
#include <utility>

#include "util/CompilerWarnings.h"

// Needed for libc++ in C++20 mode, because std::result_of was removed.
#ifndef BOOST_ASIO_HAS_STD_INVOKE_RESULT
#define BOOST_ASIO_HAS_STD_INVOKE_RESULT
#endif

#include <boost/beast/version.hpp>

// Don't set header for boost beast 1.81 and forward, because it is noop there.
#if defined BOOST_BEAST_VERSION && BOOST_BEAST_VERSION < 345
#define BOOST_BEAST_USE_STD_STRING_VIEW
#endif

// GCC (since version 15) wrongly believes that
// `boost::asio::ip::basic_resolver_results::create` copies a `tcp::endpoint`
// out of bounds. GCC 15 reports this as `-Warray-bounds` and GCC 16 as
// `-Wstringop-overflow`, so both have to be disabled; for why the suppression
// has to wrap the include, see `util/CompilerWarnings.h`. The two `DISABLE_...`
// macros each open their own diagnostic scope, hence the two
// `GCC_REENABLE_WARNINGS` below.
//
// IMPORTANT: This only works because `<boost/asio.hpp>` is included here and
// nowhere else in QLever; it is the only Boost header that pulls in
// `basic_resolver_results.hpp`. Because of the include guards, a translation
// unit that included it before this header would parse the offending definition
// outside the diagnostic scope, and the suppression would silently have no
// effect. Do not add another include of `<boost/asio.hpp>`.
DISABLE_ARRAY_BOUNDS_WARNINGS
DISABLE_STRINGOP_OVERFLOW_WARNINGS
#include <boost/asio.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/beast.hpp>
GCC_REENABLE_WARNINGS
GCC_REENABLE_WARNINGS

// For boost versions prior to 1.81 this should be no-op
#if defined BOOST_BEAST_VERSION && BOOST_BEAST_VERSION < 345
constexpr std::string_view toStd(std::string_view view) { return view; }
#else
inline std::string_view toStd(boost::core::string_view view) { return view; }
#endif

#endif  // QLEVER_SRC_UTIL_HTTP_BEAST_H
