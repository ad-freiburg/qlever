// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_ENCODEDIRIS_ENCODEDIRIPATTERN_H
#define QLEVER_SRC_INDEX_VOCABULARY_ENCODEDIRIS_ENCODEDIRIPATTERN_H

#include <cstdint>
#include <optional>
#include <range/v3/numeric/accumulate.hpp>
#include <string>
#include <string_view>
#include <vector>

#include "backports/three_way_comparison.h"
#include "index/vocabulary/encodedIris/NibbleEncoding.h"
#include "util/BitUtils.h"
#include "util/Exception.h"
#include "util/json.h"

// The declarative description of the IRIs that the `EncodedIriManager` (see
// `EncodedIriManager.h`) stores directly in an `Id` instead of in the
// vocabulary. It is stored in the index, such that an index can be loaded
// without knowing how it was built.
namespace encodedIri {

// A range of bits `[begin_, end_)` of a number that is known to always have the
// fixed value `value_`. The bits are numbered starting from the least
// significant one and `value_` is the value of the range as a number, so
// `FixedBitRange{29, 32, 1}` means that bit 29 is one and that the bits 30 and
// 31 are zero. Such bits carry no information and are therefore not stored in
// the `Id`.
struct FixedBitRange {
  size_t begin_ = 0;
  size_t end_ = 0;
  uint64_t value_ = 0;

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
  // The number can only be encoded if it is smaller than `2 ^ numBits_`.
  size_t numBits_ = 64;
  // The bit ranges of the number that always have a fixed value and that are
  // therefore not stored. They have to be sorted, non-overlapping, and
  // contained in `[0, numBits_)`.
  std::vector<FixedBitRange> fixedBitRanges_{};
  // The fixed string that directly follows the number in the IRI. It may only
  // be empty for the last part of a pattern, and it must not start with a
  // digit, because the digits of the number are matched greedily.
  std::string separator_{};
  NumberEncoding encoding_ = NumberEncoding::Binary;

  // The number of bits that are actually stored in the `Id` for this part.
  size_t numBitsStored() const {
    return numBits_ - ::ranges::accumulate(fixedBitRanges_, size_t{0},
                                           [](size_t sum, const auto& range) {
                                             return sum + range.numBits();
                                           });
  }

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(Part, numBits_, fixedBitRanges_,
                                              separator_, encoding_)

  template <typename H>
  friend H AbslHashValue(H h, const Part& part) {
    return H::combine(std::move(h), part.numBits_, part.fixedBitRanges_,
                      part.separator_, part.encoding_);
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
  // without the leading `<`, which the `EncodedIriManager` then adds.
  std::string prefix_{};
  // The numbers of the IRIs, together with the fixed strings that separate
  // them. Must not be empty.
  std::vector<Part> parts_{};

  // The total number of bits that are stored in the `Id` for this pattern.
  size_t numBitsStored() const {
    return ::ranges::accumulate(parts_, size_t{0},
                                [](size_t sum, const auto& part) {
                                  return sum + part.numBitsStored();
                                });
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

// Throw a `std::runtime_error` if the `pattern` is not valid, or if it needs
// more than `numBitsAvailable` bits. NOTE: Whether the `prefix_` has a leading
// `<` is deliberately not checked here, because the `EncodedIriManager` is the
// one that adds it, and it also validates the prefixes that a user specifies.
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

// Conversion to and from JSON, which is how the patterns are stored in the
// index.
void to_json(nlohmann::json& j, const FixedBitRange& range);
void from_json(const nlohmann::json& j, FixedBitRange& range);
void to_json(nlohmann::json& j, const Part& part);
void from_json(const nlohmann::json& j, Part& part);
void to_json(nlohmann::json& j, const Pattern& pattern);
void from_json(const nlohmann::json& j, Pattern& pattern);

}  // namespace encodedIri

#endif  // QLEVER_SRC_INDEX_VOCABULARY_ENCODEDIRIS_ENCODEDIRIPATTERN_H
