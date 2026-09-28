// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_SIMDVALIDITYBITMASK_H
#define QLEVER_SRC_ENGINE_SIMDVALIDITYBITMASK_H

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>
#include <vector>

#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || \
    defined(_M_IX86)
#include <immintrin.h>
#define QLEVER_SIMD_X86 1
#endif

#include "engine/SimdCpuFeatures.h"

#if defined(__GNUC__) || defined(__clang__)
#define QLEVER_AVX2_TARGET __attribute__((target("avx2")))
#define QLEVER_SSE2_TARGET __attribute__((target("sse2")))
#else
#define QLEVER_AVX2_TARGET
#define QLEVER_SSE2_TARGET
#endif

#include "backports/span.h"
#include "global/Id.h"
#include "global/ValueId.h"
#include "util/Exception.h"

namespace ad_utility::simd {

// _____________________________________________________________________________
// ValidityBitmask64: Invariant-bearing 64-bit column validity tracker.
// Semantic convention (matching DuckDB validity_t):
// - Bit value 1 represents a VALID (bound / defined) row value.
// - Bit value 0 represents an UNBOUND (undefined / null) row value.
class ValidityBitmask64 {
 private:
  uint64_t mask_ = 0;

 public:
  // Default constructor: creates an all-unbound bitmask (mask = 0).
  constexpr explicit ValidityBitmask64(uint64_t mask = 0) noexcept
      : mask_{mask} {}

  // Factory methods for clear intent
  [[nodiscard]] static constexpr ValidityBitmask64 allValidMask() noexcept {
    return ValidityBitmask64{~0ULL};
  }

  [[nodiscard]] static constexpr ValidityBitmask64 allUnboundMask() noexcept {
    return ValidityBitmask64{0ULL};
  }

  // Row-level query and manipulation
  [[nodiscard]] bool isRowValid(size_t index) const noexcept {
    AD_EXPENSIVE_CHECK(index < 64);
    return (mask_ & (1ULL << index)) != 0;
  }

  [[nodiscard]] bool isRowUnbound(size_t index) const noexcept {
    AD_EXPENSIVE_CHECK(index < 64);
    return (mask_ & (1ULL << index)) == 0;
  }

  void setRowValid(size_t index) noexcept {
    AD_CONTRACT_CHECK(index < 64);
    mask_ |= (1ULL << index);
  }

  void setRowUnbound(size_t index) noexcept {
    AD_CONTRACT_CHECK(index < 64);
    mask_ &= ~(1ULL << index);
  }

  void setRow(size_t index, bool isValid) noexcept {
    AD_CONTRACT_CHECK(index < 64);
    if (isValid) {
      mask_ |= (1ULL << index);
    } else {
      mask_ &= ~(1ULL << index);
    }
  }

  // Aggregate queries
  [[nodiscard]] constexpr bool allValid() const noexcept {
    return mask_ == ~0ULL;
  }

  [[nodiscard]] constexpr bool allUnbound() const noexcept {
    return mask_ == 0ULL;
  }

  [[nodiscard]] constexpr bool hasUnbound() const noexcept {
    return mask_ != ~0ULL;
  }

  [[nodiscard]] constexpr bool hasValid() const noexcept {
    return mask_ != 0ULL;
  }

  [[nodiscard]] constexpr size_t countValid() const noexcept {
    return static_cast<size_t>(std::popcount(mask_));
  }

  [[nodiscard]] constexpr size_t countUnbound() const noexcept {
    return 64 - static_cast<size_t>(std::popcount(mask_));
  }

  [[nodiscard]] constexpr uint64_t rawMask() const noexcept { return mask_; }

  // Returns the index of the first unbound row (0..63), or 64 if all are valid.
  [[nodiscard]] constexpr size_t firstUnboundIndex() const noexcept {
    return static_cast<size_t>(std::countr_one(mask_));
  }

  // Returns the index of the first valid row (0..63), or 64 if all are unbound.
  [[nodiscard]] constexpr size_t firstValidIndex() const noexcept {
    return mask_ == 0ULL ? 64 : static_cast<size_t>(std::countr_zero(mask_));
  }

  // Iteration helpers over set/unset bits
  template <typename Func>
  void forEachValid(Func&& func) const {
    uint64_t remaining = mask_;
    while (remaining != 0) {
      size_t idx = static_cast<size_t>(std::countr_zero(remaining));
      func(idx);
      remaining &= (remaining - 1);  // Clear lowest set bit
    }
  }

  template <typename Func>
  void forEachUnbound(Func&& func) const {
    uint64_t remaining = ~mask_;
    while (remaining != 0) {
      size_t idx = static_cast<size_t>(std::countr_zero(remaining));
      func(idx);
      remaining &= (remaining - 1);  // Clear lowest set bit
    }
  }

  // Bitwise operators
  [[nodiscard]] friend constexpr ValidityBitmask64 operator&(
      ValidityBitmask64 a, ValidityBitmask64 b) noexcept {
    return ValidityBitmask64{a.mask_ & b.mask_};
  }

  [[nodiscard]] friend constexpr ValidityBitmask64 operator|(
      ValidityBitmask64 a, ValidityBitmask64 b) noexcept {
    return ValidityBitmask64{a.mask_ | b.mask_};
  }

  [[nodiscard]] friend constexpr ValidityBitmask64 operator^(
      ValidityBitmask64 a, ValidityBitmask64 b) noexcept {
    return ValidityBitmask64{a.mask_ ^ b.mask_};
  }

  [[nodiscard]] friend constexpr ValidityBitmask64 operator~(
      ValidityBitmask64 a) noexcept {
    return ValidityBitmask64{~a.mask_};
  }

  constexpr ValidityBitmask64& operator&=(ValidityBitmask64 other) noexcept {
    mask_ &= other.mask_;
    return *this;
  }

  constexpr ValidityBitmask64& operator|=(ValidityBitmask64 other) noexcept {
    mask_ |= other.mask_;
    return *this;
  }

  constexpr ValidityBitmask64& operator^=(ValidityBitmask64 other) noexcept {
    mask_ ^= other.mask_;
    return *this;
  }

  friend constexpr bool operator==(ValidityBitmask64 a,
                                   ValidityBitmask64 b) noexcept {
    return a.mask_ == b.mask_;
  }

  friend constexpr bool operator!=(ValidityBitmask64 a,
                                   ValidityBitmask64 b) noexcept {
    return a.mask_ != b.mask_;
  }
};

namespace detail {

#if defined(QLEVER_SIMD_X86)

// AVX2 implementation: Scans 64 64-bit values (512 bytes) using 16 __m256i
// vectors. For each 4-element vector, _mm256_cmpeq_epi64 checks against 0
// (undefined ValueId), and _mm256_movemask_pd extracts the 4-bit comparison
// mask.
[[nodiscard]] QLEVER_AVX2_TARGET inline uint64_t scanBatch64Avx2(
    const uint64_t* data) noexcept {
  const __m256i zero = _mm256_setzero_si256();
  const auto* ptr = reinterpret_cast<const __m256i*>(data);
  uint64_t resultMask = 0;

  for (size_t i = 0; i < 16; ++i) {
    __m256i v = _mm256_loadu_si256(ptr + i);
    __m256i cmp = _mm256_cmpeq_epi64(v, zero);
    uint64_t undef4 =
        static_cast<uint32_t>(_mm256_movemask_pd(_mm256_castsi256_pd(cmp)));
    uint64_t valid4 = (~undef4) & 0x0FULL;
    resultMask |= (valid4 << (i * 4));
  }
  return resultMask;
}

// AVX2 fast test for all-unbound (all 64 values == 0) via bitwise OR reduction.
[[nodiscard]] QLEVER_AVX2_TARGET inline bool isAllUnbound64Avx2(
    const uint64_t* data) noexcept {
  const auto* ptr = reinterpret_cast<const __m256i*>(data);
  __m256i or0 =
      _mm256_or_si256(_mm256_loadu_si256(ptr + 0), _mm256_loadu_si256(ptr + 1));
  __m256i or1 =
      _mm256_or_si256(_mm256_loadu_si256(ptr + 2), _mm256_loadu_si256(ptr + 3));
  __m256i or2 =
      _mm256_or_si256(_mm256_loadu_si256(ptr + 4), _mm256_loadu_si256(ptr + 5));
  __m256i or3 =
      _mm256_or_si256(_mm256_loadu_si256(ptr + 6), _mm256_loadu_si256(ptr + 7));
  __m256i or4 =
      _mm256_or_si256(_mm256_loadu_si256(ptr + 8), _mm256_loadu_si256(ptr + 9));
  __m256i or5 = _mm256_or_si256(_mm256_loadu_si256(ptr + 10),
                                _mm256_loadu_si256(ptr + 11));
  __m256i or6 = _mm256_or_si256(_mm256_loadu_si256(ptr + 12),
                                _mm256_loadu_si256(ptr + 13));
  __m256i or7 = _mm256_or_si256(_mm256_loadu_si256(ptr + 14),
                                _mm256_loadu_si256(ptr + 15));

  __m256i acc0 =
      _mm256_or_si256(_mm256_or_si256(or0, or1), _mm256_or_si256(or2, or3));
  __m256i acc1 =
      _mm256_or_si256(_mm256_or_si256(or4, or5), _mm256_or_si256(or6, or7));
  __m256i total = _mm256_or_si256(acc0, acc1);
  return _mm256_testz_si256(total, total) != 0;
}

// AVX2 vectorized store for 64 single-byte delimiters (2 x 32-byte vector
// stores).
QLEVER_AVX2_TARGET inline char* write64DelimitersAvx2(char* dest,
                                                      char delimiter) noexcept {
  __m256i delims = _mm256_set1_epi8(delimiter);
  _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest), delims);
  _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + 32), delims);
  return dest + 64;
}

// AVX2 vectorized store for 64 2-byte (delimiter, separator) pairs (4 x 32-byte
// vector stores = 128 bytes).
QLEVER_AVX2_TARGET inline char* write64DelimiterPairsAvx2(
    char* dest, char delimiter, char separator) noexcept {
  uint16_t pair = static_cast<uint8_t>(delimiter) |
                  (static_cast<uint16_t>(static_cast<uint8_t>(separator)) << 8);
  __m256i vec = _mm256_set1_epi16(static_cast<short>(pair));
  _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest), vec);
  _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + 32), vec);
  _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + 64), vec);
  _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + 96), vec);
  return dest + 128;
}

#endif  // QLEVER_SIMD_X86

// Portable scalar fallback for scanning 64 64-bit values.
[[nodiscard]] inline uint64_t scanBatch64Scalar(const uint64_t* data) noexcept {
  uint64_t mask = 0;
  for (size_t i = 0; i < 64; ++i) {
    if (data[i] != 0) {
      mask |= (1ULL << i);
    }
  }
  return mask;
}

// Portable fallback for writing 64 delimiter characters using 64-bit stores.
inline char* write64DelimitersScalar(char* dest, char delimiter) noexcept {
  uint64_t word8 = 0x0101010101010101ULL * static_cast<uint8_t>(delimiter);
  for (size_t i = 0; i < 8; ++i) {
    std::memcpy(dest + i * 8, &word8, 8);
  }
  return dest + 64;
}

// Portable fallback for writing 64 pairs using 64-bit stores.
inline char* write64DelimiterPairsScalar(char* dest, char delimiter,
                                         char separator) noexcept {
  uint16_t pair = static_cast<uint8_t>(delimiter) |
                  (static_cast<uint16_t>(static_cast<uint8_t>(separator)) << 8);
  uint64_t quad =
      static_cast<uint64_t>(pair) | (static_cast<uint64_t>(pair) << 16) |
      (static_cast<uint64_t>(pair) << 32) | (static_cast<uint64_t>(pair) << 48);
  for (size_t i = 0; i < 16; ++i) {
    std::memcpy(dest + i * 8, &quad, 8);
  }
  return dest + 128;
}

}  // namespace detail

// _____________________________________________________________________________
// SimdValidityScanner: Deep module providing vectorized validity scanning,
// column-level batch classification, and fast-path vectorized unbound
// serialization.
class SimdValidityScanner {
 public:
  // ___________________________________________________________________________
  // Scan a batch of exactly 64 ValueIds (512 bytes) and construct a
  // ValidityBitmask64.
  [[nodiscard]] static inline ValidityBitmask64 scanBatch64(
      const ValueId* data) noexcept {
    AD_CONTRACT_CHECK(data != nullptr);
    // `ValueId` is a standard-layout class whose first (and only) member is
    // the underlying `uint64_t`, so it is pointer-interconvertible with it
    // and this access is well-defined.
    const auto* raw = reinterpret_cast<const uint64_t*>(data);
#if defined(QLEVER_SIMD_X86)
    if (cpuSupportsAvx2()) {
      return ValidityBitmask64{detail::scanBatch64Avx2(raw)};
    }
#endif
    return ValidityBitmask64{detail::scanBatch64Scalar(raw)};
  }

  // ___________________________________________________________________________
  // Scan a batch of exactly 64 uint64_t raw values.
  [[nodiscard]] static inline ValidityBitmask64 scanBatch64(
      const uint64_t* data) noexcept {
    AD_CONTRACT_CHECK(data != nullptr);
#if defined(QLEVER_SIMD_X86)
    if (cpuSupportsAvx2()) {
      return ValidityBitmask64{detail::scanBatch64Avx2(data)};
    }
#endif
    return ValidityBitmask64{detail::scanBatch64Scalar(data)};
  }

  // ___________________________________________________________________________
  // Fast check whether all 64 ValueIds in the batch are unbound (all zero).
  [[nodiscard]] static inline bool isAllUnbound64(
      const ValueId* data) noexcept {
    AD_CONTRACT_CHECK(data != nullptr);
    const auto* raw = reinterpret_cast<const uint64_t*>(data);
#if defined(QLEVER_SIMD_X86)
    if (cpuSupportsAvx2()) {
      return detail::isAllUnbound64Avx2(raw);
    }
#endif
    for (size_t i = 0; i < 64; ++i) {
      if (raw[i] != 0) {
        return false;
      }
    }
    return true;
  }

  // ___________________________________________________________________________
  // Fast check whether all 64 ValueIds in the batch are valid (none are zero).
  [[nodiscard]] static inline bool isAllValid64(const ValueId* data) noexcept {
    return scanBatch64(data).allValid();
  }

  // ___________________________________________________________________________
  // Scan an arbitrary span of ValueIds (up to 64 elements).
  // Bits at index >= data.size() are set to 0 (unbound).
  [[nodiscard]] static inline ValidityBitmask64 scanBatch(
      ql::span<const ValueId> data) noexcept {
    AD_CONTRACT_CHECK(data.size() <= 64);
    if (data.size() == 64) {
      return scanBatch64(data.data());
    }
    uint64_t mask = 0;
    for (size_t i = 0; i < data.size(); ++i) {
      if (data[i] != ValueId::makeUndefined()) {
        mask |= (1ULL << i);
      }
    }
    return ValidityBitmask64{mask};
  }

  // ___________________________________________________________________________
  // Scan an entire column of ValueIds into a destination span of
  // ValidityBitmask64. Returns the number of 64-row bitmask blocks written.
  static inline size_t scanColumn(
      ql::span<const ValueId> column,
      ql::span<ValidityBitmask64> outBitmasks) noexcept {
    const size_t numRows = column.size();
    const size_t numFullBatches = numRows / 64;
    const size_t totalBatches = (numRows + 63) / 64;
    AD_CONTRACT_CHECK(outBitmasks.size() >= totalBatches);

    const ValueId* ptr = column.data();
    for (size_t b = 0; b < numFullBatches; ++b) {
      outBitmasks[b] = scanBatch64(ptr + b * 64);
    }

    size_t tailCount = numRows % 64;
    if (tailCount > 0) {
      outBitmasks[numFullBatches] =
          scanBatch(column.subspan(numFullBatches * 64, tailCount));
    }
    return totalBatches;
  }

  // ___________________________________________________________________________
  // Scan an entire column of ValueIds and return a vector of ValidityBitmask64.
  [[nodiscard]] static inline std::vector<ValidityBitmask64> scanColumn(
      ql::span<const ValueId> column) {
    const size_t totalBatches = (column.size() + 63) / 64;
    std::vector<ValidityBitmask64> bitmasks(totalBatches);
    scanColumn(column,
               ql::span<ValidityBitmask64>{bitmasks.data(), bitmasks.size()});
    return bitmasks;
  }

  // ___________________________________________________________________________
  // Vectorized store writing 64 CSV delimiter tokens (e.g. ',') with zero cell
  // checks. Returns the pointer past the last written byte (dest + 64).
  static inline char* writeUnboundBatchCsv(char* dest,
                                           char delimiter = ',') noexcept {
    AD_CONTRACT_CHECK(dest != nullptr);
#if defined(QLEVER_SIMD_X86)
    if (cpuSupportsAvx2()) {
      return detail::write64DelimitersAvx2(dest, delimiter);
    }
#endif
    return detail::write64DelimitersScalar(dest, delimiter);
  }

  // ___________________________________________________________________________
  // Vectorized store writing 64 TSV delimiter tokens (e.g. '\t') with zero cell
  // checks. Returns the pointer past the last written byte (dest + 64).
  static inline char* writeUnboundBatchTsv(char* dest,
                                           char delimiter = '\t') noexcept {
    AD_CONTRACT_CHECK(dest != nullptr);
#if defined(QLEVER_SIMD_X86)
    if (cpuSupportsAvx2()) {
      return detail::write64DelimitersAvx2(dest, delimiter);
    }
#endif
    return detail::write64DelimitersScalar(dest, delimiter);
  }

  // ___________________________________________________________________________
  // Vectorized store writing 64 pairs of (delimiter, rowSeparator) = 128 bytes
  // for CSV export with newline terminators (e.g. ',\n').
  static inline char* writeUnboundRowsCsv(char* dest, char delimiter = ',',
                                          char rowSeparator = '\n') noexcept {
    AD_CONTRACT_CHECK(dest != nullptr);
#if defined(QLEVER_SIMD_X86)
    if (cpuSupportsAvx2()) {
      return detail::write64DelimiterPairsAvx2(dest, delimiter, rowSeparator);
    }
#endif
    return detail::write64DelimiterPairsScalar(dest, delimiter, rowSeparator);
  }

  // ___________________________________________________________________________
  // Vectorized store writing 64 pairs of (delimiter, rowSeparator) = 128 bytes
  // for TSV export with newline terminators (e.g. '\t\n').
  static inline char* writeUnboundRowsTsv(char* dest, char delimiter = '\t',
                                          char rowSeparator = '\n') noexcept {
    AD_CONTRACT_CHECK(dest != nullptr);
#if defined(QLEVER_SIMD_X86)
    if (cpuSupportsAvx2()) {
      return detail::write64DelimiterPairsAvx2(dest, delimiter, rowSeparator);
    }
#endif
    return detail::write64DelimiterPairsScalar(dest, delimiter, rowSeparator);
  }
};

// _____________________________________________________________________________
// Free convenience wrapper functions
inline char* writeUnboundBatchCsv(char* dest, char delimiter = ',') noexcept {
  return SimdValidityScanner::writeUnboundBatchCsv(dest, delimiter);
}

inline char* writeUnboundBatchTsv(char* dest, char delimiter = '\t') noexcept {
  return SimdValidityScanner::writeUnboundBatchTsv(dest, delimiter);
}

inline char* writeUnboundRowsCsv(char* dest, char delimiter = ',',
                                 char rowSeparator = '\n') noexcept {
  return SimdValidityScanner::writeUnboundRowsCsv(dest, delimiter,
                                                  rowSeparator);
}

inline char* writeUnboundRowsTsv(char* dest, char delimiter = '\t',
                                 char rowSeparator = '\n') noexcept {
  return SimdValidityScanner::writeUnboundRowsTsv(dest, delimiter,
                                                  rowSeparator);
}

}  // namespace ad_utility::simd

#endif  // QLEVER_SRC_ENGINE_SIMDVALIDITYBITMASK_H
