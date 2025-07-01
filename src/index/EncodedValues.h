//
// Created by kalmbacj on 7/1/25.
//

#ifndef QLEVER_SRC_INDEX_ENCODEDVALUES_H
#define QLEVER_SRC_INDEX_ENCODEDVALUES_H

#include "global/Id.h"
#include "parser/LiteralOrIri.h"
#include "util/BitUtils.h"
#include "util/CtreHelpers.h"

struct EncodedValues {
  static constexpr std::tuple prefixes{
      ctll::fixed_string{"<http://www.wikidata.org/entity/Q"},
      ctll::fixed_string{"<https://www.openstreetmap.org/way/"},
      ctll::fixed_string{"<https://www.openstreetmap.org/relation/"},
      ctll::fixed_string{"<https://www.openstreetmap.org/node/"},
  };
  // TODO<joka921> Make sure to keep those two in sync by automating this
  // process.
  static constexpr std::tuple prefixesRt{
      std::string_view{"<http://www.wikidata.org/entity/Q"},
      std::string_view{"<https://www.openstreetmap.org/way/"},
      std::string_view{"<https://www.openstreetmap.org/relation/"},
      std::string_view{"<https://www.openstreetmap.org/node/"},
  };
  using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;

  template <size_t I>
  static std::optional<Id> encodeSingle(std::string_view repr) {
    static constexpr auto regex =
        std::get<I>(prefixes) + "(?<digits>[0-9]{1,15})>";
    auto match = ctre::match<regex>(repr);
    if (!match) {
      return std::nullopt;
    }
    auto idx = match.template get<"digits">().template to_number<uint64_t>();
    // TODO<joka921> The numbers are a little arbitrary here currently, test and
    // document the behavior.
    static_assert(I < 255);
    return Id::makeFromEncodedVal(idx | (I << 50));
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
    return res;
  }
  template <size_t I>
  static auto toLiteralOrIriSingle(uint64_t idx) {
    return LiteralOrIri::fromStringRepresentation(
        absl::StrCat(std::get<I>(prefixesRt), idx, ">"));
  }

  static auto toLiteralOrIri(Id id) {
    AD_CORRECTNESS_CHECK(id.getDatatype() == Datatype::EncodedVal);
    static constexpr auto mask = ad_utility::bitMaskForLowerBits(50);
    auto idx = id.getEncodedVal() & mask;
    auto encoderIdx = id.getEncodedVal() >> 50;

    if (encoderIdx == 0) {
      return toLiteralOrIriSingle<0>(idx);
    }
    AD_FAIL();
  }
};

#endif  // QLEVER_SRC_INDEX_ENCODEDVALUES_H
