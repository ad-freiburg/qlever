//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_BEAST_H
#define QLEVER_BEAST_H

// A convenience header that includes Boost::Asio and Boost::Beast,
// and defines several constants to make Boost::Asio compile
// with coroutine support on G++/libstdc++ and clang++/libc++
// (TODO<joka921> Figure out, why Boost currently is not able, to deduce
// these automatically.

#include "../../util/Coroutines.h"

// libc++ needs <experimental/coroutine>, libstdc++ needs <coroutine>
#define BOOST_ASIO_HAS_CO_AWAIT
#define BOOST_ASIO_HAS_STD_COROUTINE

// Needed for libc++ in C++20 mode, because std::result_of was removed.
#define BOOST_ASIO_HAS_STD_INVOKE_RESULT
#define BOOST_BEAST_USE_STD_STRING_VIEW

#include <boost/asio.hpp>
#include <boost/beast.hpp>

#endif  // QLEVER_BEAST_H
