// Copyright 2018 - 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <johannes.kalmbach@gmail.com>
//          Hannah Bast <bast@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include "parser/TripleComponent.h"

#include <charconv>
#include <sstream>

#include "rdfTypes/GeoPoint.h"
#include "util/TypeTraits.h"

// ____________________________________________________________________________
std::ostream& operator<<(std::ostream& stream, const TripleComponent& obj) {
  ad_utility::visitIf(
      obj._variant, [&stream](const Variable& v) { stream << v.name(); },
      [&stream](const TripleComponent::UNDEF&) { stream << "UNDEF"; },
      [&stream](const TripleComponent::Literal& v) {
        stream << v.toStringRepresentation();
      },
      [&stream](const TripleComponent::Iri& v) {
        stream << v.toStringRepresentation();
      },
      [&stream](const DateYearOrDuration& v) {
        stream << "DATE: " << v.toStringAndType().first;
      },
      [&stream](bool v) { stream << (v ? "true" : "false"); },
      [&stream](const GeoPoint& v) { stream << Id::makeFromGeoPoint(v); },
      [&stream](double v) {
        // Print the shortest representation that is parsed back to `v`, with a
        // `.0` for an integral value (to distinguish it from an `int64_t`).
        std::array<char, 32> buffer;
        auto end = std::to_chars(buffer.begin(), buffer.end(), v).ptr;
        std::string_view string{buffer.begin(), end};
        stream << string;
        if (string.find_first_of(".en") == std::string_view::npos) {
          stream << ".0";
        }
      },
      [&stream](const auto& v) {
        static_assert(ad_utility::SameAsAny<std::decay_t<decltype(v)>, Id,
                                            int64_t, std::string>);
        stream << v;
      });
  return stream;
}

// ____________________________________________________________________________
[[nodiscard]] std::string TripleComponent::toString() const {
  std::stringstream stream;
  stream << *this;
  return std::move(stream).str();
}
