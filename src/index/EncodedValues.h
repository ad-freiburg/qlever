//
// Created by kalmbacj on 7/1/25.
//

#ifndef QLEVER_SRC_INDEX_ENCODEDVALUES_H
#define QLEVER_SRC_INDEX_ENCODEDVALUES_H

#include "global/Id.h"
#include "parser/LiteralOrIri.h"

struct EncodedValues {
  using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;
  static std::optional<Id> encode(std::string_view repr) {
    auto match =
        ctre::match<"<http://www.wikidata.org/entity/Q(?<digits>[0-9]{1,15})>">(
            repr);
    if (!match) {
      return std::nullopt;
    }
    uint64_t idx = match.get<"digits">().to_number<uint64_t>();
    return Id::makeFromEncodedVal(idx);
  }

  static auto toLiteralOrIri(Id id) {
    AD_CORRECTNESS_CHECK(id.getDatatype() == Datatype::EncodedVal);
    return LiteralOrIri::fromStringRepresentation(absl::StrCat(
        "<http://www.wikidata.org/entity/Q", id.getEncodedVal(), ">"));
  }
};

#endif  // QLEVER_SRC_INDEX_ENCODEDVALUES_H
