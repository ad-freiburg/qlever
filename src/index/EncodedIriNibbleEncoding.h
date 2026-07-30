// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_ENCODEDIRINIBBLEENCODING_H
#define QLEVER_SRC_INDEX_ENCODEDIRINIBBLEENCODING_H

#include <absl/numeric/bits.h>

#include <algorithm>
#include <cstdint>
#include <string>
#include <string_view>

#include "backports/StartsWithAndEndsWith.h"
#include "util/Exception.h"

namespace encodedIris {
// The encoding of a sequence of decimal digits into `NumBitsEncoding` bits, for
// the IRIs that are encoded directly into an `Id` (see `EncodedIriManager.h`).
//
// Each decimal digit is encoded as a 4-bit nibble, where digit `i` is encoded
// as `i + 1` and converted to a hexadecimal number. The nibbles are stored
// left-aligned (not right-aligned) and filled on the right with zeroes. This
// non-standard encoding makes sure that the order of the encoded values
// corresponds to the lexical order of the original digit sequences, and that
// leading zeros are preserved.
//
// For example, with `NumBitsEncoding = 32`:
//
// "1"    ->  20 00 00 00
// "10"   ->  21 00 00 00
// "100"  ->  21 10 00 00
// "2"    ->  30 00 00 00
// "20"   ->  31 00 00 00
//
// NOTE: The price for these properties is that at most `NumDigits` digits can
// be encoded. A prefix with constraints (see `EncodedIriBitConstraint.h`) uses
// a different, binary encoding without that limit, but also without the order
// and leading-zero properties.
template <size_t NumBitsEncoding>
class NibbleEncoder {
 public:
  // We use 4-bit nibbles per digit in the encoding.
  static constexpr size_t NibbleSize = 4;
  // The maximal number of digits that can be encoded.
  static constexpr size_t NumDigits = NumBitsEncoding / NibbleSize;

  static_assert(NumBitsEncoding % NibbleSize == 0);
  static_assert(NumDigits > 0);

  // Encode `numberStr` (which may only consist of digits, and of at most
  // `NumDigits` of them) into a 64-bit number.
  static constexpr uint64_t encode(std::string_view numberStr) {
    AD_CORRECTNESS_CHECK(numberStr.size() <= NumDigits);

    uint64_t result = 0;

    // Compute the starting shift (for the first digit).
    uint64_t shift = NumBitsEncoding - NibbleSize;

    for (const char digitChar : numberStr) {
      // Deliberately encode [0, ..., 9] as [1, ..., A], so that the padding
      // nibble `0`is smaller than any valid digit encoding.
      uint8_t digit = (digitChar - '0') + 1;
      result |= static_cast<uint64_t>(digit) << shift;
      shift -= NibbleSize;
    }
    return result;
  }

  // The inverse of `encode`. The digits are appended to the `result` string.
  static void decodeToString(std::string& result, uint64_t encoded) {
    forEachDigit(
        [&result](auto digit) {
          result.push_back(static_cast<char>(digit + '0'));
        },
        encoded);
  }

  // Overload of `decodeToString` that returns the result as a `uint64_t`. Note
  // that this loses leading zeros.
  static uint64_t decode(uint64_t encoded) {
    uint64_t result = 0;
    forEachDigit(
        [&result](auto digit) {
          result *= 10;
          result += digit;
        },
        encoded);
    return result;
  }

  // Combine the integer encoding of the payload and the prefix string into a
  // result string that represents an IRI.
  //
  // NOTE: This function expects that the prefix starts with `<`.
  static std::string toStringWithGivenPrefix(uint64_t digitEncoding,
                                             std::string_view prefix) {
    AD_EXPENSIVE_CHECK(ql::starts_with(prefix, '<'));
    std::string result;
    result.reserve(prefix.size() + NumDigits + 1);
    result = prefix;
    decodeToString(result, digitEncoding);
    result.push_back('>');
    return result;
  }

  // Helper for decoding numbers. Call `processDigit` for every digit (from high
  // to low) in the decoded representation of `encoded`.
  template <typename F>
  static void forEachDigit(F processDigit, uint64_t encoded) {
    size_t shift = NumBitsEncoding - NibbleSize;
    auto numTrailingZeros = absl::countr_zero(encoded);
    size_t numTrailingZeroNibbles = numTrailingZeros / NibbleSize;
    // NOTE: `encoded == 0` gives `numTrailingZeroNibbles > NumDigits`, so the
    // length has to be clamped, else it would underflow. A payload of `0` never
    // comes from this encoding (which always sets the highest nibble), but a
    // prefix with constraints can produce it, and the payload of a corrupted
    // index can be anything.
    size_t len = NumDigits - std::min(numTrailingZeroNibbles, NumDigits);
    for (size_t i = 0; i < len; ++i) {
      processDigit(((encoded >> shift) & 0xF) - 1);
      shift -= NibbleSize;
    }
  }
};
}  // namespace encodedIris

#endif  // QLEVER_SRC_INDEX_ENCODEDIRINIBBLEENCODING_H
