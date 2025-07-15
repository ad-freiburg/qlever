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

template <size_t NumBitsTotal, size_t NumBitsTags>
// TODO<joka921> Add several assertions.
struct EncodedValuesImpl {
  static constexpr size_t NumBitsEncoding = NumBitsTotal - NumBitsTags;
  std::vector<std::string> prefixes{
      "<http://www.wikidata.org/entity/Q",
      "<https://www.openstreetmap.org/way/",
      "<https://www.openstreetmap.org/relation/",
      "<https://www.openstreetmap.org/node/",
      "<https://osm2rdf.cs.uni-freiburg.de/rdf/geom#osm_node_",
      "<https://osm2rdf.cs.uni-freiburg.de/rdf/geom#osm_relarea_",
      "<https://osm2rdf.cs.uni-freiburg.de/rdf/geom#osm_wayarea_",
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
    auto numZeros = std::countr_zero(encoded);
    size_t len = (N / 4) - (numZeros / 4);
    // AD_LOG_WARN << "num zeros:" << std::dec << numZeros << " len of encoded
    // is " << len << std::endl;
    for (size_t i = 0; i < len; ++i) {
      result.push_back(static_cast<char>(((encoded >> shift) & 0xF) + '0' - 1));
      shift -= 4;
    }
  }

  std::optional<Id> encode(std::string_view repr) const {
    auto it = ql::ranges::find_if(prefixes, [&repr](std::string_view prefix) {
      return repr.starts_with(prefix);
    });
    if (it == prefixes.end()) {
      return std::nullopt;
    }
    repr.remove_prefix(it->size());
    static constexpr auto regex = ctll::fixed_string{"(?<digits>[0-9]+)>"};
    auto match = ctre::match<regex>(repr);
    if (!match) {
      return std::nullopt;
    }
    const auto& numString = match.template get<"digits">().to_view();

    // TODO<joka921> magic constant.
    if (numString.size() > NumBitsEncoding / 4) {
      return std::nullopt;
    }
    auto prefixIndex = static_cast<size_t>(it - prefixes.begin());
    // TODO<joka921> Compute this magic constant and assert during construction.
    AD_CORRECTNESS_CHECK(prefixIndex < (1ull << 10));

    return Id::makeFromEncodedVal(
        encodeDecimalToNBit<NumBitsEncoding>(numString) |
        (prefixIndex << NumBitsEncoding));
  }

  std::string toString(Id id) const {
    AD_CORRECTNESS_CHECK(id.getDatatype() == Datatype::EncodedVal);
    static constexpr auto mask =
        ad_utility::bitMaskForLowerBits(NumBitsEncoding);
    auto idx = id.getEncodedVal() & mask;
    auto encoderIdx = id.getEncodedVal() >> NumBitsEncoding;
    std::string repr;
    const auto& prefix = prefixes.at(encoderIdx);
    // TODO<joka921> Magic constant.
    repr.reserve(prefix.size() + 13);
    repr = prefix;
    decodeDecimalFrom64Bit<48>(repr, idx);
    repr.push_back('>');
    return repr;
  }

  LiteralOrIri toLiteralOrIri(Id id) const {
    return LiteralOrIri::fromStringRepresentation(toString(id));
  }
};

using EncodedValues = EncodedValuesImpl<Id::numDataBits, 10>;

#endif  // QLEVER_SRC_INDEX_ENCODEDVALUES_H
