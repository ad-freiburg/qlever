// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/TripleComponentConversions.h"

#include <absl/strings/str_cat.h>

#include "index/ExportIds.h"
#include "index/IndexImpl.h"
#include "index/LocalVocabContext.h"
#include "rdfTypes/GeoPoint.h"

// ____________________________________________________________________________
std::optional<Id> toValueIdIfNotString(
    const TripleComponent& tripleComponent,
    const EncodedIriManager* encodedIriManager) {
  auto visitor = [encodedIriManager](const auto& value) -> std::optional<Id> {
    using T = std::decay_t<decltype(value)>;
    using Literal = TripleComponent::Literal;
    using Iri = TripleComponent::Iri;
    if constexpr (std::is_same_v<T, Iri>) {
      return encodedIriManager->encode(value.toStringRepresentation());

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
    } else if constexpr (std::is_same_v<T, TripleComponent::UNDEF>) {
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
  return tripleComponent.visit(visitor);
}

// ____________________________________________________________________________
std::string toRdfLiteral(const TripleComponent& tripleComponent) {
  if (tripleComponent.isVariable()) {
    return tripleComponent.getVariable().name();
  } else if (tripleComponent.isString()) {
    return tripleComponent.getString();
  } else if (tripleComponent.isLiteral()) {
    return tripleComponent.getLiteral().toStringRepresentation();
  } else if (tripleComponent.isIri()) {
    return tripleComponent.getIri().toStringRepresentation();
  } else {
    EncodedIriManager ev;
    auto [value, type] = ql::exportIds::idToStringAndTypeForEncodedValue(
                             toValueIdIfNotString(tripleComponent, &ev).value())
                             .value();
    return absl::StrCat("\"", value, "\"^^<", type, ">");
  }
}

// _____________________________________________________________________________
std::variant<Id, std::pair<VocabIndex, VocabIndex>> toValueIdOrBounds(
    const TripleComponent& tripleComponent, const IndexImpl& index) {
  AD_CONTRACT_CHECK(!tripleComponent.isString());
  std::optional<Id> vid =
      toValueIdIfNotString(tripleComponent, &index.encodedIriManager());
  if (vid != std::nullopt) {
    return vid.value();
  }
  AD_CORRECTNESS_CHECK(tripleComponent.isLiteral() || tripleComponent.isIri());
  const std::string& content =
      tripleComponent.isLiteral()
          ? tripleComponent.getLiteral().toStringRepresentation()
          : tripleComponent.getIri().toStringRepresentation();
  // Look up the word in the vocabularies of the index. NOTE: This is exactly
  // the lookup that a `LocalVocabEntry` performs, which is required because
  // `toValueId` below passes the result to the constructor of that class that
  // takes the position in the vocabularies, see
  // `LocalVocabContext::lookupWordInVocabularies`.
  auto idOrBounds =
      index.getLocalVocabContext().lookupWordInVocabularies(content);
  if (const auto* id = std::get_if<Id>(&idOrBounds)) {
    return *id;
  }
  return std::get<LocalVocabContext::VocabBounds>(idOrBounds);
}

// _____________________________________________________________________________
std::optional<Id> toValueId(const TripleComponent& tripleComponent,
                            const IndexImpl& index) {
  auto idOrBounds = toValueIdOrBounds(tripleComponent, index);
  if (auto* id = std::get_if<Id>(&idOrBounds)) {
    return *id;
  }
  return std::nullopt;
}

// _____________________________________________________________________________
Id toValueId(TripleComponent&& tripleComponent, const IndexImpl& index,
             LocalVocab& localVocab) {
  auto idOrBounds = toValueIdOrBounds(tripleComponent, index);
  if (const auto* id = std::get_if<Id>(&idOrBounds)) {
    return *id;
  }
  using Bounds = std::pair<VocabIndex, VocabIndex>;
  AD_CORRECTNESS_CHECK(std::holds_alternative<Bounds>(idOrBounds));
  auto [lower, upper] = std::get<Bounds>(idOrBounds);
  // If `toValueIdOrBounds` could not convert to `Id`, we have a Literal or Iri,
  // which we look up in (and potentially add to) our local vocabulary. NOTE:
  // The bounds are the position of a word that is contained in none of the
  // vocabularies of the index, which is exactly the position that the
  // `LocalVocabEntry` below requires, see `positionInVocab()` there.
  AD_CORRECTNESS_CHECK(tripleComponent.isLiteral() || tripleComponent.isIri());
  using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;
  auto moveWord = [&tripleComponent]() {
    if (tripleComponent.isLiteral()) {
      return LiteralOrIri{std::move(tripleComponent.getLiteral())};
    } else {
      return LiteralOrIri{std::move(tripleComponent.getIri())};
    }
  };
  return Id::makeFromLocalVocabIndex(
      localVocab.getIndexAndAddIfNotContained(LocalVocabEntry(
          moveWord(), Id::makeFromVocabIndex(lower),
          Id::makeFromVocabIndex(upper), index.getLocalVocabContext())));
}
