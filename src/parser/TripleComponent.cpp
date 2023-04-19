// Copyright 2018 - 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <johannes.kalmbach@gmail.com>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include "parser/TripleComponent.h"

#include "absl/strings/str_cat.h"

// ____________________________________________________________________________
std::ostream& operator<<(std::ostream& stream, const TripleComponent& obj) {
  std::visit(
      [&stream]<typename T>(const T& value) -> void {
        if constexpr (std::is_same_v<T, Variable>) {
          stream << value.name();
        } else if constexpr (std::is_same_v<T, TripleComponent::UNDEF>) {
          stream << "UNDEF";
        } else if constexpr (std::is_same_v<T, TripleComponent::Literal>) {
          stream << value.rawContent();
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
TripleComponent::Literal::Literal(
    const RdfEscaping::NormalizedRDFString& literal,
    std::string_view langtagOrDatatype) {
  const std::string& l = literal.get();
  AD_CORRECTNESS_CHECK(l.starts_with('"') && l.ends_with('"') && l.size() >= 2);
  // TODO<joka921> there also should be a strong type for the
  // `langtagOrDatatype`.
  AD_CONTRACT_CHECK(langtagOrDatatype.empty() ||
                    langtagOrDatatype.starts_with('@') ||
                    langtagOrDatatype.starts_with("^^"));
  content_ = absl::StrCat(l, langtagOrDatatype);
  startOfDatatype_ = l.size();
}

// ____________________________________________________________________________
std::optional<Id> TripleComponent::toValueIdIfNotString() const {
  auto visitor = []<typename T>(const T& value) -> std::optional<Id> {
    if constexpr (std::is_same_v<T, std::string> ||
                  std::is_same_v<T, Literal>) {
      return std::nullopt;
    } else if constexpr (std::is_same_v<T, int64_t>) {
      return Id::makeFromInt(value);
    } else if constexpr (std::is_same_v<T, double>) {
      return Id::makeFromDouble(value);
    } else if constexpr (std::is_same_v<T, UNDEF>) {
      return Id::makeUndefined();
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
  if (isString()) {
    return getString();
  } else if (isLiteral()) {
    return getLiteral().rawContent();
  } else if (isDouble()) {
    return absl::StrCat("\"", getDouble(), "\"^^<", XSD_DOUBLE_TYPE, ">");
  } else {
    AD_CORRECTNESS_CHECK(isInt());
    return absl::StrCat("\"", getInt(), "\"^^<", XSD_INTEGER_TYPE, ">");
  }
}
