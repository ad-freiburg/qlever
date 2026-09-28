// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_ALLOCATIONCOUNTER_H
#define QLEVER_SRC_UTIL_ALLOCATIONCOUNTER_H

#include <cstdint>

// Counters of the heap allocations of the whole process, for measurements.
// They only count if QLever is built with the CMake option
// `QLEVER_COUNT_ALLOCATIONS`, which replaces the global `operator new`
// (`AllocationCounter.cpp`). Without it, `enabled` is false and `current()`
// returns zeros, so the calls compile to nothing.
namespace ad_utility::allocationCounter {

// The number of calls to `operator new` and the number of bytes requested by
// them since the start of the process.
struct Counts {
  uint64_t numAllocations_ = 0;
  uint64_t numBytes_ = 0;

  Counts operator-(const Counts& other) const {
    return {numAllocations_ - other.numAllocations_,
            numBytes_ - other.numBytes_};
  }
};

#ifdef QLEVER_COUNT_ALLOCATIONS
inline constexpr bool enabled = true;
Counts current();
#else
inline constexpr bool enabled = false;
inline Counts current() { return {}; }
#endif

}  // namespace ad_utility::allocationCounter

#endif  // QLEVER_SRC_UTIL_ALLOCATIONCOUNTER_H
