// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Marvin Stoetzel <marvin.stoetzel@mailbox.org>

#ifndef QLEVER_SRC_UTIL_SIMDUTILS_H
#define QLEVER_SRC_UTIL_SIMDUTILS_H

#include <cstddef>
#include <cstring>
#include <string_view>

// The x86 code below uses GCC/clang-only facilities (`__attribute__((target))`,
// `__builtin_cpu_supports`), which MSVC does not provide, so it is enabled only
// for those compilers. Everything else uses the scalar fallback.
#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))
#define QLEVER_SIMD_X86 1
#include <immintrin.h>
#endif

namespace ad_utility::simd {

namespace detail {

// Scalar fallback: return true iff `data[0 .. size)` contains any byte from
// the compile-time set `SpecialChars`. This is the reference implementation
// that all SIMD variants must agree with. It delegates to `find_first_of`,
// which the standard libraries implement with a 256-bit lookup bitmap plus a
// vectorized scan, and is therefore much faster than a naive nested loop for
// the short inputs that dominate the non-SIMD paths.
template <char... SpecialChars>
bool containsAnyByteScalar(std::string_view data) {
  static constexpr char specialChars[] = {SpecialChars...};
  static constexpr std::string_view specialCharsView{specialChars,
                                                     sizeof...(SpecialChars)};
  return data.find_first_of(specialCharsView) != std::string_view::npos;
}

#ifdef QLEVER_SIMD_X86
// True iff the CPU running this binary supports AVX2. Deliberately a
// non-template function so that there is a single guarded static for the whole
// program instead of one per instantiation of `containsAnyByte`.
inline bool hasAvx2() {
  static const bool kHasAvx2 = [] {
    __builtin_cpu_init();
    return __builtin_cpu_supports("avx2");
  }();
  return kHasAvx2;
}

// SSE2 (baseline on x86-64): scan 16 bytes at a time. After the loop, one
// overlapping load of the last 16 bytes covers the tail; since that window
// ends exactly at `data[size]`, it never reads past the end of the buffer.
template <char... SpecialChars>
bool containsAnyByteSSE2(const char* data, size_t size) {
  constexpr size_t chunkSize = 16;
  if (size < chunkSize) {
    return containsAnyByteScalar<SpecialChars...>(std::string_view{data, size});
  }
  auto chunkContainsSpecial = [](const char* p) {
    __m128i chunk;
    std::memcpy(&chunk, p, sizeof(chunk));
    __m128i mask = _mm_setzero_si128();
    ((mask = _mm_or_si128(mask,
                          _mm_cmpeq_epi8(chunk, _mm_set1_epi8(SpecialChars)))),
     ...);
    return _mm_movemask_epi8(mask) != 0;
  };
  for (size_t i = 0; i + chunkSize <= size; i += chunkSize) {
    if (chunkContainsSpecial(data + i)) {
      return true;
    }
  }
  // If `size` is an exact multiple of the chunk size, the loop above already
  // covered every byte; an overlapping tail load would rescan the last chunk
  // for no gain. Only load the tail when a partial chunk remains.
  if (size % chunkSize != 0) {
    return chunkContainsSpecial(data + size - chunkSize);
  }
  return false;
}

// AVX2: scan 32 bytes at a time, otherwise identical to the SSE2 variant.
// Compiled with the `target` attribute so that only these functions require
// AVX2 support; they are only called after a runtime check via
// `__builtin_cpu_supports("avx2")`.
//
// NOTE: the actual chunk predicate must be a free function carrying its own
// `target` attribute rather than a lambda inside the AVX2 function. Clang
// does not propagate the enclosing function's `target` attribute to lambdas
// defined inside it, which made the always_inline `_mm256_*` intrinsics fail
// with "requires target feature 'avx', but would be inlined into function
// 'operator()' compiled without support" (GCC accepts the lambda form, clang
// does not). See PR #3211.
template <char... SpecialChars>
__attribute__((target("avx2"))) bool avx2ChunkContainsSpecial(const char* p) {
  __m256i chunk;
  std::memcpy(&chunk, p, sizeof(chunk));
  __m256i mask = _mm256_setzero_si256();
  ((mask = _mm256_or_si256(
        mask, _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8(SpecialChars)))),
   ...);
  return _mm256_movemask_epi8(mask) != 0;
}

template <char... SpecialChars>
__attribute__((target("avx2"))) bool containsAnyByteAVX2(const char* data,
                                                         size_t size) {
  constexpr size_t chunkSize = 32;
  if (size < chunkSize) {
    return containsAnyByteSSE2<SpecialChars...>(data, size);
  }
  for (size_t i = 0; i + chunkSize <= size; i += chunkSize) {
    if (avx2ChunkContainsSpecial<SpecialChars...>(data + i)) {
      return true;
    }
  }
  // See the SSE2 variant: an exact multiple needs no overlapping tail load.
  if (size % chunkSize != 0) {
    return avx2ChunkContainsSpecial<SpecialChars...>(data + size - chunkSize);
  }
  return false;
}
#endif  // QLEVER_SIMD_X86
}  // namespace detail

namespace detail {

// A compile-time set of characters, used to give the special-character lists
// of the individual output formats a name (see `RdfEscaping.cpp`).
template <char... SpecialChars>
struct CharacterSet {};

}  // namespace detail

// Return true iff `sv` contains any of the bytes in the compile-time set
// `SpecialChars`. Uses a single vectorized sweep (AVX2 if the CPU supports it,
// else SSE2, which is baseline on x86-64) and falls back to the scalar
// implementation on other architectures. All variants are guaranteed to return
// the same result, so this function is safe to use as a fast replacement for
// `std::string_view::find_first_of` (or a regex character-class search) in
// hot paths. The set must contain at least one byte.
template <char... SpecialChars>
bool containsAnyByte(std::string_view sv) {
  static_assert(sizeof...(SpecialChars) > 0);
  const char* data = sv.data();
  const size_t size = sv.size();
#ifdef QLEVER_SIMD_X86
  if (detail::hasAvx2()) {
    return detail::containsAnyByteAVX2<SpecialChars...>(data, size);
  }
  return detail::containsAnyByteSSE2<SpecialChars...>(data, size);
#else
  return detail::containsAnyByteScalar<SpecialChars...>(
      std::string_view{data, size});
#endif
}

// Overload of `containsAnyByte` that takes a named `CharacterSet` instead of
// the raw template arguments.
template <char... SpecialChars>
bool containsAnyByte(detail::CharacterSet<SpecialChars...>,
                     std::string_view sv) {
  return containsAnyByte<SpecialChars...>(sv);
}

}  // namespace ad_utility::simd

#endif  // QLEVER_SRC_UTIL_SIMDUTILS_H
