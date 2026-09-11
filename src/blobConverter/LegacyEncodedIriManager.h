// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_BLOBCONVERTER_LEGACYENCODEDIRIMANAGER_H
#define QLEVER_SRC_BLOBCONVERTER_LEGACYENCODEDIRIMANAGER_H

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "index/vocabulary/EncodedIriManager.h"
#include "index/vocabulary/EncodedIriPattern.h"
#include "util/json.h"

namespace qlever::blobConverter {

// The hardcoded, BMW-specific encoding schemes of the legacy
// `EncodedIriManager` (see `LegacyEncodedIriManager` below). The integer values
// are the ones that are stored in the `"specialEncoding"` key of the legacy
// index metadata.
enum struct LegacySpecialEncoding : int {
  None = 0,
  // `range_<num1>_<num2>_<num3>P`: `num1` is a 32-bit number whose highest
  // three bits are `001`, `num2` and `num3` are smaller than `2 ^ 8`.
  RangePattern = 1,
  // `valRange_<num1>_<num2>_<num3>M`: `num1` as for `RangePattern`, `num2` and
  // `num3` are smaller than `2 ^ 11`.
  ValRangePattern = 2,
  // `laneRef_<num1>_<num2>`, `roadRef_<num1>_<num2>`,
  // `speedprofile_<num1>_<num2>`: `num1` is a 64-bit number whose highest three
  // bits are `001` and whose bits `[17, 32)` are zero, `num2` is smaller than
  // `2 ^ 4`.
  LaneRef = 3,
  RoadRef = 4,
  SpeedProfile = 5,
  // `stopLoc_<num1>_<num2>`, in one of two variants: the same shape as
  // `LaneRef`, or `num1` smaller than `2 ^ 32` and `num2` smaller than
  // `2 ^ 18`. The two enum values share the same implementation.
  StopLoc = 6,
  StopLoc32 = 7
};

// The configuration of one legacy prefix. Exactly one of the following modes
// applies: `specialEncoding_ != None` (a hardcoded scheme), a non-empty
// `bitRangeConstraints_` (a single 64-bit number with bit ranges of a known
// value), a set `zeroBitRange_` (a single 64-bit number with one bit range that
// is known to be zero), or none of these (a plain sequence of decimal digits
// that is encoded digit by digit).
struct LegacyPrefixConfig {
  // The prefix of the IRIs, including the leading `<`.
  std::string prefix_;
  std::optional<std::pair<size_t, size_t>> zeroBitRange_;
  // Sorted by `begin_` and non-overlapping.
  std::vector<encodedIri::FixedBitRange> bitRangeConstraints_;
  LegacySpecialEncoding specialEncoding_ = LegacySpecialEncoding::None;

  bool isSpecialEncoding() const {
    return specialEncoding_ != LegacySpecialEncoding::None;
  }
  bool isMultiConstraintMode() const { return !bitRangeConstraints_.empty(); }
  bool isPlain() const {
    return !isSpecialEncoding() && !isMultiConstraintMode() &&
           !zeroBitRange_.has_value();
  }
};

// A reimplementation of the decoding (and, for testing purposes, the encoding)
// of the `EncodedIriManager` of the legacy format (the `demo-v1-c++17_unimodel`
// branch of the `qlever-bmw` fork). The bit layout of an encoded IRI is the
// same as in the current format: of the 60 data bits of an `Id`, the highest
// 8 bits are the tag (the index of the `LegacyPrefixConfig`), and the lowest
// 52 bits are the payload. Only the way in which the payload is computed
// differs, which is what this class implements. It can also express each of
// its configurations as one or two `encodedIri::Pattern`s of the current
// format (see `toPatterns`), such that the IRIs of a legacy blob can be
// re-encoded for the current format.
//
// NOTE: The legacy bit-range modes accepted numbers with leading zeros (e.g.
// `lane_0123`) and normalized them to `123`, whereas the `Binary` encoding of
// the current format rejects leading zeros. The conversion of a blob is exact
// nevertheless, because decoded IRIs never have leading zeros, but a query
// constant with leading zeros behaves differently against a converted blob
// (it is not encoded, and therefore matches nothing).
class LegacyEncodedIriManager {
 public:
  static constexpr size_t NumBitsTags = 8;
  static constexpr size_t NumBitsEncoding = EncodedIriManager::NumBitsEncoding;
  static_assert(NumBitsEncoding == 52);
  static constexpr size_t NumDigits = NumBitsEncoding / encodedIri::NibbleSize;

 private:
  // The index of a config in this vector is its tag.
  std::vector<LegacyPrefixConfig> configs_;

 public:
  LegacyEncodedIriManager() = default;
  explicit LegacyEncodedIriManager(std::vector<LegacyPrefixConfig> configs);

  // Construct from the value of the `"encoded-iri-prefixes"` key of the legacy
  // index metadata. Supports both the `"prefix-configs"` format and the plain
  // `"prefixes-with-leading-angle-brackets"` list. Throw if the JSON has none
  // of the two keys, or if a config is invalid.
  static LegacyEncodedIriManager fromJson(const nlohmann::json& json);

  const std::vector<LegacyPrefixConfig>& configs() const { return configs_; }

  // Decode the 60 data bits of a legacy `Id` with datatype `EncodedVal` to the
  // IRI that they encode (including the angle brackets).
  std::string toString(uint64_t encodedVal) const;

  // Decode the complete bits of a legacy `Id`. Throw if the legacy datatype of
  // these bits is not `EncodedVal`.
  std::string toStringFromLegacyBits(uint64_t legacyBits) const;

  // Encode the `iri` (including the angle brackets) exactly like the legacy
  // `EncodedIriManager`, and return the resulting 60 data bits of the legacy
  // `Id` (the tag shifted by `NumBitsEncoding`, combined with the payload).
  // Return `std::nullopt` if the legacy manager did not encode the `iri`.
  std::optional<uint64_t> encode(std::string_view iri) const;

  // Express the configurations as `encodedIri::Pattern`s of the current
  // format, in the order of the tags. A `StopLoc`/`StopLoc32` config becomes
  // two patterns with the same prefix (the 64-bit variant first, exactly as
  // the legacy encoder tries them). The `prefix_` of the patterns is returned
  // without the leading `<` (as expected by the constructor of the current
  // `EncodedIriManager`). For the modes with a single 64-bit number, the
  // legacy limit of the compressed value to `NumBitsEncoding` bits is
  // expressed by additional fixed zero bits where necessary.
  std::vector<encodedIri::Pattern> toPatterns() const;

  // Construct the `EncodedIriManager` of the current format that encodes
  // exactly the IRIs that this legacy manager encodes (via `toPatterns`). Like
  // every current manager, it additionally encodes the always-on prefix for
  // new graphs (`QLEVER_NEW_GRAPH_PREFIX`), which the index code relies on, so
  // its `patterns_` has one entry more than `toPatterns()`.
  EncodedIriManager makeCurrentManager() const;

 private:
  // Append the decoded payload (the part of the IRI after the prefix, without
  // the closing `>`) for the `config` to `result`.
  static void decodePayload(std::string& result,
                            const LegacyPrefixConfig& config, uint64_t payload);

  // Encode the `suffix` (the part of the IRI after the prefix, including the
  // closing `>`) for the `config`, and return the payload.
  static std::optional<uint64_t> encodePayload(const LegacyPrefixConfig& config,
                                               std::string_view suffix);
};

}  // namespace qlever::blobConverter

#endif  // QLEVER_SRC_BLOBCONVERTER_LEGACYENCODEDIRIMANAGER_H
