// Copyright 2018 - 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <johannes.kalmbach@gmail.com>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include "parser/TripleComponent.h"

#include "absl/strings/str_cat.h"
#include "engine/ExportQueryExecutionTrees.h"

// ____________________________________________________________________________
std::ostream& operator<<(std::ostream& stream, const TripleComponent& obj) {
  std::visit(
      [&stream]<typename T>(const T& value) -> void {
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
        } else {
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

// ____________________________________________________________________________
std::optional<Id> TripleComponent::toValueIdIfNotString() const {
  auto visitor = []<typename T>(const T& value) -> std::optional<Id> {
    if constexpr (std::is_same_v<T, std::string> ||
                  std::is_same_v<T, Literal> || std::is_same_v<T, Iri>) {
      return std::nullopt;
    } else if constexpr (std::is_same_v<T, int64_t>) {
      return Id::makeFromInt(value);
    } else if constexpr (std::is_same_v<T, Id>) {
      return value;
    } else if constexpr (std::is_same_v<T, double>) {
      return Id::makeFromDouble(value);
    } else if constexpr (std::is_same_v<T, bool>) {
      return Id::makeFromBool(value);
    } else if constexpr (std::is_same_v<T, UNDEF>) {
      return Id::makeUndefined();
    } else if constexpr (std::is_same_v<T, DateYearOrDuration>) {
      return Id::makeFromDate(value);
    } else if constexpr (std::is_same_v<T, Variable>) {
      // Cannot turn a variable into a ValueId.
      AD_FAIL();
    } else {
      static_assert(ad_utility::alwaysFalse<T>);
    }
  };
  return std::visit(visitor, _variant);
}

// ____________________________________________________________________________
std::string TripleComponent::toRdfLiteral() const {
  if (isVariable()) {
    return getVariable().name();
  } else if (isString()) {
    return getString();
  } else if (isLiteral()) {
    return getLiteral().toStringRepresentation();
  } else if (isIri()) {
    return getIri().toStringRepresentation();
  } else {
    auto [value, type] =
        ExportQueryExecutionTrees::idToStringAndTypeForEncodedValue(
            toValueIdIfNotString().value())
            .value();
    return absl::StrCat("\"", value, "\"^^<", type, ">");
  }
}
