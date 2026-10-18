// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_ENCODEDIRIPATTERN_H
#define QLEVER_SRC_INDEX_VOCABULARY_ENCODEDIRIPATTERN_H

#include <cstdint>
#include <functional>
#include <optional>
#include <range/v3/numeric/accumulate.hpp>
#include <string>
#include <string_view>
#include <vector>

#include "backports/three_way_comparison.h"
#include "index/vocabulary/NibbleEncoding.h"
#include "util/BitUtils.h"
#include "util/Exception.h"
#include "util/json.h"

// The declarative description of the IRIs that the `EncodedIriManager` (see
// `EncodedIriManager.h`) stores directly in an `Id` instead of in the
// vocabulary. It is stored in the index, such that an index can be loaded
// without knowing how it was built.
//
// NOTE: The constructors of the structs below validate their arguments and
// throw a `std::runtime_error` if they are invalid, so every object that
// exists fulfills the documented constraints. This also holds for objects that
// are read from JSON. The data members are public for the convenient read
// access, they must not be modified in a way that violates the constraints.
namespace encodedIri {

// A range of bits `[begin_, end_)` of a number that is known to always have the
// fixed value `value_`. The bits are numbered starting from the least
// significant one and `value_` is the value of the range as a number, so
// `FixedBitRange{29, 32, 1}` means that bit 29 is one and that the bits 30 and
// 31 are zero. Such bits carry no information and are therefore not stored in
// the `Id`. The range is never empty, it ends at bit 64 at the latest, and
// `value_` always fits into the range, that is, `value_ < 2 ^ (end_ - begin_)`.
struct FixedBitRange {
  uint8_t begin_;
  uint8_t end_;
  uint64_t value_;

  // Throw a `std::runtime_error` if the range `[begin, end)` is empty, if it
  // extends beyond 64 bits, or if the `value` doesn't fit into the range.
  // NOTE: The arguments are deliberately wider than the members, such that
  // values that are too large are rejected instead of being silently
  // truncated, in particular when reading them from JSON.
  FixedBitRange(uint64_t begin, uint64_t end, uint64_t value);

  // The number of bits in the range.
  size_t numBits() const { return end_ - begin_; }

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(FixedBitRange, begin_, end_,
                                              value_)

  template <typename H>
  friend H AbslHashValue(H h, const FixedBitRange& range) {
    return H::combine(std::move(h), range.begin_, range.end_, range.value_);
  }
};

// The way in which the decimal number of a `Part` (see below) is encoded.
enum class NumberEncoding {
  // Encode the value of the number in binary, leaving out the
  // `fixedBitRanges_`. Numbers with a leading zero (except for the number `0`
  // itself) are not encodable in this mode, because the encoding would
  // otherwise not be invertible.
  Binary,
  // Encode each decimal digit in a nibble (four bits), which preserves the
  // lexicographic order of the digit strings and also encodes leading zeros
  // (see `NibbleEncoding.h` for the details). For this encoding, `numBits_`
  // must be a multiple of `NibbleSize` and `fixedBitRanges_` must be empty.
  Nibbles
};

// One part of a `Pattern` (see below): a decimal number, followed by a fixed
// string.
struct Part {
  // The number can only be encoded if it is smaller than `2 ^ numBits_`. It is
  // always between 1 and 64.
  uint8_t numBits_;
  // The bit ranges of the number that always have a fixed value and that are
  // therefore not stored. They are sorted, non-overlapping, and contained in
  // `[0, numBits_)`.
  std::vector<FixedBitRange> fixedBitRanges_;
  // The fixed string that directly follows the number in the IRI. It may only
  // be empty for the last part of a pattern (see `Pattern`), it never contains
  // an angle bracket, and it never starts with a digit, because the digits of
  // the number are matched greedily.
  std::string suffix_;
  NumberEncoding encoding_;

  // Throw a `std::runtime_error` if one of the constraints that are documented
  // at the members is violated. NOTE: The `numBits` are deliberately wider than
  // the member, see `FixedBitRange`.
  Part(uint64_t numBits, std::vector<FixedBitRange> fixedBitRanges,
       std::string suffix, NumberEncoding encoding = NumberEncoding::Binary);

  // The number of bits that are actually stored in the `Id` for this part.
  size_t numBitsStored() const {
    return numBits_ - ::ranges::accumulate(fixedBitRanges_, size_t{0},
                                           std::plus<>{},
                                           &FixedBitRange::numBits);
  }

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(Part, numBits_, fixedBitRanges_,
                                              suffix_, encoding_)

  template <typename H>
  friend H AbslHashValue(H h, const Part& part) {
    return H::combine(std::move(h), part.numBits_, part.fixedBitRanges_,
                      part.suffix_, part.encoding_);
  }
};

// The pattern of a family of IRIs that are all encoded with the same tag: a
// fixed prefix, followed by an alternating sequence of decimal numbers and
// fixed strings, followed by the closing `>`. For example, the IRIs
// `<http://example.org/range_536870912_50_25P>`, where the first number always
// has the three highest bits of its 32 bits set to `001` and the other two
// numbers are smaller than `2 ^ 8`, are described by
//
//   Pattern{"http://example.org/range_",
//           {Part{32, {{29, 32, 1}}, "_"}, Part{8, {}, "_"}, Part{8, {}, "P"}}}
struct Pattern {
  // The prefix of the IRIs. In the public configuration it is specified
  // without the leading `<`, which the `EncodedIriManager` then adds. It never
  // contains a `>`.
  std::string prefix_;
  // The numbers of the IRIs, together with the fixed strings that separate
  // them. Never empty, and only the `suffix_` of the last part may be empty,
  // because two consecutive numbers could otherwise not be told apart.
  std::vector<Part> parts_;

  // Throw a `std::runtime_error` if one of the constraints that are documented
  // at the members is violated. NOTE: Whether the `prefix` has a leading `<`
  // is deliberately not checked here, because the `EncodedIriManager` is the
  // one that adds it, and it also validates the prefixes that a user
  // specifies.
  Pattern(std::string prefix, std::vector<Part> parts);

  // The total number of bits that are stored in the `Id` for this pattern.
  size_t numBitsStored() const {
    return ::ranges::accumulate(parts_, size_t{0}, std::plus<>{},
                                &Part::numBitsStored);
  }

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(Pattern, prefix_, parts_)

  template <typename H>
  friend H AbslHashValue(H h, const Pattern& pattern) {
    return H::combine(std::move(h), pattern.prefix_, pattern.parts_);
  }
};

// The pattern for the simple case of a fixed prefix that is followed by up to
// `numBits / NibbleSize` decimal digits that use the nibble encoding (see
// `NibbleEncoding.h`), which is what the `--encode-as-id` option of the index
// builder specifies.
Pattern plainPrefixPattern(std::string prefix, size_t numBits);

// Return true if the `pattern` was created by `plainPrefixPattern` with the
// given `numBits`. Only such patterns can be stored in the legacy format of
// the index metadata, which consists of the prefixes alone (see
// `EncodedIriManager::to_json`).
bool isPlainPrefixPattern(const Pattern& pattern, size_t numBits);

// Throw a `std::runtime_error` if the `pattern` needs more than
// `numBitsAvailable` bits. This is the only constraint of a pattern that
// depends on the `EncodedIriManager` and that the constructors of the structs
// above therefore cannot check.
void validatePattern(const Pattern& pattern, size_t numBitsAvailable);

// Remove the `fixedBitRanges_` of the `part` from the `value`, such that only
// the bits that actually have to be stored remain. Return `std::nullopt` if
// the `value` doesn't fulfill the constraints of the `part`, meaning that it
// is too large, or that one of the fixed bit ranges has a different value.
std::optional<uint64_t> compressNumber(const Part& part, uint64_t value);

// The inverse of `compressNumber`: reinsert the `fixedBitRanges_` of the
// `part`. The `compressedValue` must be smaller than
// `2 ^ part.numBitsStored()`.
uint64_t decompressNumber(const Part& part, uint64_t compressedValue);

// Overload of `decompressNumber` that appends the decimal representation of
// the decompressed number to `result`.
void decompressNumber(std::string& result, const Part& part,
                      uint64_t compressedValue);

// The longest prefix of `input` that consists of decimal digits only.
std::string_view leadingDigits(std::string_view input);

// Parse `input` (which may only consist of decimal digits) as a `uint64_t`.
// Return `std::nullopt` if the number doesn't fit into a `uint64_t`, or if it
// has a leading zero (see `NumberEncoding::Binary`).
std::optional<uint64_t> parseDecimal(std::string_view input);

// Try to encode the `rest` of an IRI (the part that follows the `prefix_` of
// the `pattern`, including the closing `>`) into the `pattern.numBitsStored()`
// payload bits. The first number of the pattern is stored in the most
// significant bits of the payload, the last one in the least significant bits.
// Return `std::nullopt` if the `rest` doesn't match the `pattern`, or if one
// of its numbers violates the constraints of the corresponding `Part`.
//
// NOTE: The `pattern` must store fewer than 64 bits (which `validatePattern`
// guarantees for the patterns of an `EncodedIriManager`), because the payload
// is shifted by that number of bits. This precondition is checked.
std::optional<uint64_t> encodePayload(const Pattern& pattern,
                                      std::string_view rest);

// The inverse of `encodePayload`: Reconstruct the complete IRI (the `prefix_`
// of the `pattern`, the numbers with their suffixes, and the closing `>`)
// from the `payload`, which has to be the result of a call to `encodePayload`
// with the same `pattern`. The same precondition as for `encodePayload` holds.
std::string decodeToIri(const Pattern& pattern, uint64_t payload);

}  // namespace encodedIri

// Conversion to and from JSON, which is how the patterns are stored in the
// index. As the structs are not default-constructible (they validate their
// arguments in the constructors), the `from_json` functions return the objects
// by value.
namespace nlohmann {
template <>
struct adl_serializer<encodedIri::FixedBitRange> {
  static void to_json(json& j, const encodedIri::FixedBitRange& range);
  static encodedIri::FixedBitRange from_json(const json& j);
};

template <>
struct adl_serializer<encodedIri::Part> {
  static void to_json(json& j, const encodedIri::Part& part);
  static encodedIri::Part from_json(const json& j);
};

template <>
struct adl_serializer<encodedIri::Pattern> {
  static void to_json(json& j, const encodedIri::Pattern& pattern);
  static encodedIri::Pattern from_json(const json& j);
};
}  // namespace nlohmann

#endif  // QLEVER_SRC_INDEX_VOCABULARY_ENCODEDIRIPATTERN_H
