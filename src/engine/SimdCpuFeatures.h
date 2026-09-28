// Copyright 2026, The QLever Authors, in particular:
//
// 2026        Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_SIMDCPUFEATURES_H
#define QLEVER_SRC_ENGINE_SIMDCPUFEATURES_H

namespace ad_utility::simd {

#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || \
    defined(_M_IX86)
// Runtime AVX2 detection for the `target("avx2")` kernels used by the SIMD
// export formatters. The attribute makes the kernels *compile* on any x86
// machine, but executing them on a CPU without AVX2 raises SIGILL, so every
// AVX2 call site must dispatch through this check with the scalar
// implementation as the fallback. Result is cached process-wide; the check
// itself costs one predictable branch per call.
[[nodiscard]] inline bool cpuSupportsAvx2() noexcept {
#if defined(__AVX2__)
  // AVX2 enabled globally: the compilation baseline guarantees support.
  return true;
#elif defined(__GNUC__) || defined(__clang__)
  static const bool supported = [] {
    __builtin_cpu_init();
    return __builtin_cpu_supports("avx2") != 0;
  }();
  return supported;
#else
  // Unknown x86 compiler: no portable feature query, stay scalar.
  return false;
#endif
}
#endif

}  // namespace ad_utility::simd

#endif  // QLEVER_SRC_ENGINE_SIMDCPUFEATURES_H
