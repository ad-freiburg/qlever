// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_UTIL_DANGLINGVIEWTESTHELPERS_H
#define QLEVER_TEST_UTIL_DANGLINGVIEWTESTHELPERS_H

#include <atomic>
#include <cstddef>

// _____________________________________________________________________________
// STRICTLY TEST-LOCAL BEST-EFFORT TRIPWIRE — NOT A CORRECTNESS MECHANISM.
// Overwrite `NumBytes` of stack directly below the caller's frame with
// sentinel bytes. This is the region where the frames of functions that the
// caller has already returned from lived, so stale stack contents there (e.g.
// a destroyed local object that a dangling view still points into) become
// implausible to survive. Every call starts at the same depth, so repeated
// calls overwrite the same region; to reach further down, use a larger
// `NumBytes`. Returns the last byte of the buffer, loaded from memory (a
// `volatile` read) before the function returns, so callers can check that the
// writes were not optimized away. Whether the stack still holds the sentinel
// after the function has returned cannot be checked without undefined
// behavior.
template <size_t NumBytes = 4096>
[[gnu::noinline]] char clobberStack(char sentinel = '#') {
  // `volatile` prevents the compiler from optimizing the stack writes away.
  static_assert(NumBytes > 0, "clobberStack requires a non-empty buffer");
  static_assert(NumBytes <= 65536,
                "clobberStack buffer is limited to 64KB to prevent stack "
                "overflow from excessively large template arguments");
  volatile char buffer[NumBytes];
  for (volatile char& byte : buffer) {
    byte = sentinel;
  }
  // Compiler barrier: prevents the optimizer from eliding the stack writes or
  // reordering them past the return. A signal fence compiles to no
  // instructions (unlike a thread fence it is not a hardware memory fence),
  // and unlike inline assembly it also compiles under Emscripten, which
  // builds this helper as part of `GTestHelpersTest`.
  std::atomic_signal_fence(std::memory_order_seq_cst);
  return buffer[NumBytes - 1];
}

#endif  // QLEVER_TEST_UTIL_DANGLINGVIEWTESTHELPERS_H
