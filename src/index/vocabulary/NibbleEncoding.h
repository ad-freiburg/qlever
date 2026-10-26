// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_NIBBLEENCODING_H
#define QLEVER_SRC_INDEX_VOCABULARY_NIBBLEENCODING_H

#include <absl/numeric/bits.h>

#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>

#include "backports/concepts.h"
#include "util/BitUtils.h"
#include "util/Exception.h"

// The nibble encoding of a sequence of decimal digits, which is the encoding
// that the `EncodedIriManager` (see `EncodedIriManager.h`) uses for the digits
// that follow the prefix of an encoded IRI. It has the following properties:
//
// 1. It preserves the lexicographic order of the digit sequences, that is, the
//    order of the encoded numbers is the same as the order of the original
//    IRIs.
// 2. It preserves leading zeros, so `007` and `7` have different encodings.
//
// This is achieved as follows: Each decimal digit is stored in a nibble (four
// bits), where the digit `d` is stored as `d + 1`, such that the nibble `0`
// never occurs in an encoded digit. The nibbles are stored left-aligned in the
// `numBits` bits of the encoding and padded on the right with `0` nibbles.
// Because the padding nibble `0` is smaller than every digit, a digit sequence
// is ordered before all of its extensions, as required by the lexicographic
// order.
//
// For example, with `numBits = 32`, the encodings (as hexadecimal numbers) are
// as follows.
//
// 1    ->  0x20000000
// 10   ->  0x21000000
// 100  ->  0x21100000
// 2    ->  0x30000000
// 20   ->  0x31000000
//
// Note that the digit sequences on the left are in lexicographic order, and so
// are the encodings on the right.
namespace encodedIri {

// The number of bits per digit.
static constexpr size_t NibbleSize = 4;

// Encode the `digits` (which may only consist of decimal digits, at most
// `numBits / NibbleSize` many) into the lowest `numBits` bits of the result,
// using the nibble encoding described at the top of this file.
//
// NOTE: The precondition is reported via `throw` and not via
// `AD_CORRECTNESS_CHECK`, because the latter is not `constexpr`. An
// unconditional call to it would make this a function that can never yield a
// constant expression, which is ill-formed (no diagnostic required); GCC 11 and
// 12 as well as Clang reject it outright. A `throw` on a branch that is not
// taken is fine in a constant expression. See
// `ad_utility::bitMaskForLowerBits` in `util/BitUtils.h` for the same pattern.
constexpr uint64_t encodeDigitsAsNibbles(std::string_view digits,
                                         size_t numBits) {
  if (digits.size() * NibbleSize > numBits) {
    throw std::out_of_range{"too many digits for the nibble encoding"};
  }
  uint64_t result = 0;
  size_t shift = numBits - NibbleSize;
  for (const char digitChar : digits) {
    result |= static_cast<uint64_t>((digitChar - '0') + 1) << shift;
    shift -= NibbleSize;
  }
  return result;
}

// The inverse of `encodeDigitsAsNibbles`: Call `processDigit` with each digit
// (as a `uint64_t` in `[0, 9]`, from the most significant one to the least
// significant one) that `encodeDigitsAsNibbles` has encoded into the lowest
// `numBits` bits of `encoded`. The `encoded` value must be the result of such
// a call to `encodeDigitsAsNibbles`, in particular it must fit into `numBits`
// bits.
CPP_template(typename F)(
    requires ql::concepts::invocable<
        F, uint64_t>) void forEachNibbleEncodedDigit(F processDigit,
                                                     uint64_t encoded,
                                                     size_t numBits) {
  AD_CORRECTNESS_CHECK(numBits >= NibbleSize && numBits <= 64);
  AD_CORRECTNESS_CHECK(encoded <= ad_utility::bitMaskForLowerBits(numBits));
  size_t shift = numBits - NibbleSize;
  size_t numDigits = numBits / NibbleSize;
  // The `0` nibbles at the right are the padding and not digits.
  size_t numTrailingZeroNibbles =
      encoded == 0
          ? numDigits
          : static_cast<size_t>(absl::countr_zero(encoded)) / NibbleSize;
  for (size_t i = 0; i < numDigits - numTrailingZeroNibbles; ++i) {
    uint64_t nibble = (encoded >> shift) & 0xF;
    // The digit `d` was stored as `d + 1`, see `encodeDigitsAsNibbles`.
    AD_CORRECTNESS_CHECK(nibble >= 1 && nibble <= 10);
    processDigit(nibble - 1);
    shift -= NibbleSize;
  }
}

// The inverse of `encodeDigitsAsNibbles`: Append the decoded digits (including
// leading zeros) to `result`.
inline void decodeNibblesToDigits(std::string& result, uint64_t encoded,
                                  size_t numBits) {
  forEachNibbleEncodedDigit(
      [&result](uint64_t digit) {
        result.push_back(static_cast<char>(digit + '0'));
      },
      encoded, numBits);
}

// Variant of `decodeNibblesToDigits` that returns the decoded digits as a
// number. Note that leading zeros are lost by this variant.
inline uint64_t decodeNibblesToNumber(uint64_t encoded, size_t numBits) {
  uint64_t result = 0;
  forEachNibbleEncodedDigit(
      [&result](uint64_t digit) {
        result *= 10;
        result += digit;
      },
      encoded, numBits);
  return result;
}

}  // namespace encodedIri

#endif  // QLEVER_SRC_INDEX_VOCABULARY_NIBBLEENCODING_H
