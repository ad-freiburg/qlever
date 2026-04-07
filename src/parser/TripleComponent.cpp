// Copyright 2018 - 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <johannes.kalmbach@gmail.com>
//          Hannah Bast <bast@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include "parser/TripleComponent.h"

#include <absl/strings/str_cat.h>

#include "engine/ExportQueryExecutionTrees.h"
#include "index/ExportIds.h"
#include "index/IndexImpl.h"
#include "rdfTypes/GeoPoint.h"
#include "util/GeoSparqlHelpers.h"

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

// ____________________________________________________________________________
std::optional<Id> TripleComponent::toValueIdIfNotString(
    const EncodedIriManager* evManager) const {
  auto visitor = [evManager](const auto& value) -> std::optional<Id> {
    using T = std::decay_t<decltype(value)>;
    if constexpr (std::is_same_v<T, Iri>) {
      return evManager->encode(value.toStringRepresentation());

    } else if constexpr (ad_utility::SameAsAny<T, std::string, Literal>) {
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
    } else if constexpr (std::is_same_v<T, GeoPoint>) {
      return Id::makeFromGeoPoint(value);
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
    EncodedIriManager ev;
    auto [value, type] = ql::exportIds::idToStringAndTypeForEncodedValue(
                             toValueIdIfNotString(&ev).value())
                             .value();
    return absl::StrCat("\"", value, "\"^^<", type, ">");
  }
}

// _____________________________________________________________________________
std::variant<Id, std::pair<VocabIndex, VocabIndex>>
TripleComponent::toValueIdOrBounds(const IndexImpl& index) const {
  AD_CONTRACT_CHECK(!isString());
  std::optional<Id> vid = toValueIdIfNotString(&index.encodedIriManager());
  if (vid != std::nullopt) {
    return vid.value();
  }
  AD_CORRECTNESS_CHECK(isLiteral() || isIri());
  const std::string& content = isLiteral()
                                   ? getLiteral().toStringRepresentation()
                                   : getIri().toStringRepresentation();
  auto [lower, upper] = index.getVocab().getPositionOfWord(content);
  if (lower != upper) {
    return Id::makeFromVocabIndex(lower);
  }
  return std::pair(lower, upper);
}

// _____________________________________________________________________________
std::optional<Id> TripleComponent::toValueId(const IndexImpl& index) const {
  auto idOrBounds = toValueIdOrBounds(index);
  if (auto* id = std::get_if<Id>(&idOrBounds)) {
    return *id;
  }
  return std::nullopt;
}

// _____________________________________________________________________________
Id TripleComponent::toValueId(const IndexImpl& index,
                              LocalVocab& localVocab) && {
  auto idOrBounds = toValueIdOrBounds(index);
  if (auto* id = std::get_if<Id>(&idOrBounds)) {
    return *id;
  }
  using Bounds = std::pair<VocabIndex, VocabIndex>;
  AD_CORRECTNESS_CHECK(std::holds_alternative<Bounds>(idOrBounds));
  auto [lower, upper] = std::get<Bounds>(idOrBounds);
  // If `toValueId` could not convert to `Id`, we have a Literal or Iri,
  // which we look up in (and potentially add to) our local vocabulary.
  AD_CORRECTNESS_CHECK(isLiteral() || isIri());
  using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;
  auto moveWord = [&]() -> LiteralOrIri {
    if (isLiteral()) {
      return LiteralOrIri{std::move(getLiteral())};
    } else {
      return LiteralOrIri{std::move(getIri())};
    }
  };
  return Id::makeFromLocalVocabIndex(localVocab.getIndexAndAddIfNotContained(
      LocalVocabEntry(moveWord(), Id::makeFromVocabIndex(lower),
                      Id::makeFromVocabIndex(upper))));
}
