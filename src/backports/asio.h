// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_BACKPORTS_ASIO_H
#define QLEVER_SRC_BACKPORTS_ASIO_H

// This file bridges the differences between the `boost::asio` versions that
// QLever supports. The oldest of them is the one from Boost 1.71 (see the
// `CPP17 libQLever` CI workflow), which is only supported together with
// `REDUCED_FEATURE_SET_FOR_CPP17`.

#include <boost/version.hpp>

#if BOOST_VERSION >= 107400
#include <boost/asio/any_io_executor.hpp>
#else
#include <boost/asio/executor.hpp>
#endif

namespace ql {
// A drop-in replacement for `boost::asio::any_io_executor`, the polymorphic
// wrapper for executors. That type only exists since Boost 1.74; for older
// versions we use its predecessor `boost::asio::executor` from the Networking
// TS, which supports everything that QLever does with it (in particular being
// the executor type of a `boost::asio::strand`).
#if BOOST_VERSION >= 107400
using any_io_executor = ::boost::asio::any_io_executor;
#else
using any_io_executor = ::boost::asio::executor;
#endif
}  // namespace ql

#endif  // QLEVER_SRC_BACKPORTS_ASIO_H
