// Copyright 2021 - 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_BACKPORTS_MEMORY_RESOURCE_H
#define QLEVER_SRC_BACKPORTS_MEMORY_RESOURCE_H

#ifdef QLEVER_CPP_17
#include <boost/container/pmr/memory_resource.hpp>
#include <boost/container/pmr/monotonic_buffer_resource.hpp>
#include <boost/container/pmr/polymorphic_allocator.hpp>
#else
#include <memory_resource>
#endif

namespace ql {
#ifdef QLEVER_CPP_17
namespace pmr = ::boost::container::pmr;
#else
namespace pmr = ::std::pmr;
#endif
}  // namespace ql

#endif  // QLEVER_SRC_BACKPORTS_MEMORY_RESOURCE_H
