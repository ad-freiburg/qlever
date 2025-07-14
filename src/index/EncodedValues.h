// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_ENCODEDVALUES_H
#define QLEVER_SRC_INDEX_ENCODEDVALUES_H

#include "global/Id.h"
#include "parser/LiteralOrIri.h"
#include "util/BitUtils.h"
#include "util/CtreHelpers.h"
#include "util/Log.h"

struct EncodedValues {
  static constexpr std::tuple prefixes{
      ctll::fixed_string{"<http://www.wikidata.org/entity/Q"},
      ctll::fixed_string{"<https://www.openstreetmap.org/way/"},
      ctll::fixed_string{"<https://www.openstreetmap.org/relation/"},
      ctll::fixed_string{"<https://www.openstreetmap.org/node/"},
      ctll::fixed_string{
          "<https://osm2rdf.cs.uni-freiburg.de/rdf/geom#osm_node_"},
      ctll::fixed_string{
          "<https://osm2rdf.cs.uni-freiburg.de/rdf/geom#osm_relarea_"},
      ctll::fixed_string{
          "<https://osm2rdf.cs.uni-freiburg.de/rdf/geom#osm_wayarea_"},
  };
  // TODO<joka921> Make sure to keep those two in sync by automating this
  // process.
  static constexpr std::tuple prefixesRt{
      std::string_view{"<http://www.wikidata.org/entity/Q"},
      std::string_view{"<https://www.openstreetmap.org/way/"},
      std::string_view{"<https://www.openstreetmap.org/relation/"},
      std::string_view{"<https://www.openstreetmap.org/node/"},
      std::string_view{
          "<https://osm2rdf.cs.uni-freiburg.de/rdf/geom#osm_node_"},
      std::string_view{
          "<https://osm2rdf.cs.uni-freiburg.de/rdf/geom#osm_relarea_"},
      std::string_view{
          "<https://osm2rdf.cs.uni-freiburg.de/rdf/geom#osm_wayarea_"},
  };
  using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;

  template <uint64_t N>
  static constexpr uint64_t encodeDecimalToNBit(std::string_view numberStr) {
    constexpr uint64_t maxDigits = N / 4;  // 4 bits per digits.
    auto len = numberStr.size();
    AD_CORRECTNESS_CHECK(len <= maxDigits);

    uint64_t result = 0;

    // Compute the starting shift (for the first digit)
    uint64_t shift = N - 4;

    for (uint64_t i = 0; i < len; ++i) {
      // Deliberately encode "0" through "9" as '1' through 'A' to make the
      // padding nibble `0`.
      uint8_t digit = numberStr[i] - '0' + 1;
      result |= static_cast<uint64_t>(digit) << shift;
      shift -= 4;
    }
    return result;
  }

  template <uint64_t N>
  static void decodeDecimalFrom64Bit(std::string& result, uint64_t encoded) {
    // AD_LOG_WARN << "encoding is " << std::hex << encoded << std::endl;
    size_t shift = N - 4;
    size_t len = (N - std::countr_zero(encoded)) / 4;
    // AD_LOG_WARN << "len of encoded is " << len << std::endl;
    for (size_t i = 0; i < len; ++i) {
      result.push_back(static_cast<char>(((encoded >> shift) & 0xF) + '0' - 1));
      shift -= 4;
    }
  }

  template <size_t I>
  static std::optional<Id> encodeSingle(std::string_view repr) {
    static constexpr size_t numBitsEncoding = 48;
    // Note: The `12` in the following regex is `48/4` and has to be adjusted
    // should we ever change the characteristics.
    static constexpr auto regex =
        std::get<I>(prefixes) + "(?<digits>[0-9]{1,12})>";
    auto match = ctre::match<regex>(repr);
    if (!match) {
      return std::nullopt;
    }
    const auto& numString = match.template get<"digits">().to_view();
    // TODO<joka921> Compute this constant.
    static_assert(I < (2ull << 10));
    return Id::makeFromEncodedVal(
        encodeDecimalToNBit<numBitsEncoding>(numString) |
        (I << numBitsEncoding));
  }

  static std::optional<Id> encode(std::string_view repr) {
    // TODO
    auto res = encodeSingle<0>(repr);
    if (!res.has_value()) {
      res = encodeSingle<1>(repr);
    }
    if (!res.has_value()) {
      res = encodeSingle<2>(repr);
    }
    if (!res.has_value()) {
      res = encodeSingle<3>(repr);
    }
    if (!res.has_value()) {
      res = encodeSingle<4>(repr);
    }
    if (!res.has_value()) {
      res = encodeSingle<5>(repr);
    }
    if (!res.has_value()) {
      res = encodeSingle<6>(repr);
    }
    return res;
  }
  template <size_t I>
  static auto toLiteralOrIriSingle(uint64_t idx) {
    std::string repr;
    const auto& prefix = std::get<I>(prefixesRt);
    // TODO<joka921> Magic constant.
    repr.reserve(prefix.size() + 13);
    repr = prefix;
    decodeDecimalFrom64Bit<48>(repr, idx);
    repr.push_back('>');
    return LiteralOrIri::fromStringRepresentation(std::move(repr));
  }

  static auto toLiteralOrIri(Id id) {
    AD_CORRECTNESS_CHECK(id.getDatatype() == Datatype::EncodedVal);
    static constexpr auto mask = ad_utility::bitMaskForLowerBits(50);
    auto idx = id.getEncodedVal() & mask;
    auto encoderIdx = id.getEncodedVal() >> 50;

    if (encoderIdx == 0) {
      return toLiteralOrIriSingle<0>(idx);
    } else if (encoderIdx == 1) {
      return toLiteralOrIriSingle<1>(idx);
    } else if (encoderIdx == 2) {
      return toLiteralOrIriSingle<2>(idx);
    } else if (encoderIdx == 3) {
      return toLiteralOrIriSingle<3>(idx);
    } else if (encoderIdx == 4) {
      return toLiteralOrIriSingle<4>(idx);
    } else if (encoderIdx == 5) {
      return toLiteralOrIriSingle<5>(idx);
    } else if (encoderIdx == 6) {
      return toLiteralOrIriSingle<6>(idx);
    }
    AD_FAIL();
  }
};

#endif  // QLEVER_SRC_INDEX_ENCODEDVALUES_H
