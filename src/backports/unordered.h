// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_BACKPORTS_UNORDERED_H
#define QLEVER_SRC_BACKPORTS_UNORDERED_H

// This file detects whether the open-addressing hash containers from
// `boost::unordered` (`boost::unordered_flat_map`, `boost::unordered_flat_set`,
// `boost::unordered_node_map`, and `boost::unordered_node_set`) are available,
// and if so, includes them. The flat containers exist since Boost 1.81, the
// node containers since Boost 1.82. The hash map and hash set aliases in
// `util/HashMap.h`, `util/HashSet.h`, and `util/ContainersWithAllocator.h` use
// these containers if `QLEVER_HAS_BOOST_UNORDERED` is defined, and fall back to
// `absl` or the standard library otherwise (in particular for the Boost 1.71
// that is used by the `CPP17 libQLever` CI workflow).

#include <boost/version.hpp>

#if BOOST_VERSION >= 108200
#define QLEVER_HAS_BOOST_UNORDERED
#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_flat_set.hpp>
#include <boost/unordered/unordered_node_map.hpp>
#include <boost/unordered/unordered_node_set.hpp>
#endif

#endif  // QLEVER_SRC_BACKPORTS_UNORDERED_H
