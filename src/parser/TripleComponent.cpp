// Copyright 2018 - 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <johannes.kalmbach@gmail.com>
//          Hannah Bast <bast@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include "parser/TripleComponent.h"

#include <sstream>

#include "rdfTypes/GeoPoint.h"

// ____________________________________________________________________________
std::ostream& operator<<(std::ostream& stream, const TripleComponent& obj) {
  std::visit(
      [&stream](const auto& value) -> void {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, Variable>) {
          stream << value.name();
        } else if constexpr (std::is_same_v<T, TripleComponent::UNDEF>) {
          stream << "UNDEF";
        } else if constexpr (std::is_same_v<T, TripleComponent::Literal>) {
          stream << value.toStringRepresentation();
        } else if constexpr (std::is_same_v<T, TripleComponent::Iri>) {
          stream << value.toStringRepresentation();
        } else if constexpr (std::is_same_v<T, DateYearOrDuration>) {
          stream << "DATE: " << value.toStringAndType().first;
        } else if constexpr (std::is_same_v<T, bool>) {
          stream << (value ? "true" : "false");
        } else if constexpr (std::is_same_v<T, GeoPoint>) {
          stream << Id::makeFromGeoPoint(value);
        } else {
          static_assert(
              ad_utility::SameAsAny<T, Id, double, int64_t, std::string>);
          stream << value;
        }
      },
      obj._variant);
  return stream;
}

// ____________________________________________________________________________
[[nodiscard]] std::string TripleComponent::toString() const {
  std::stringstream stream;
  stream << *this;
  return std::move(stream).str();
}
