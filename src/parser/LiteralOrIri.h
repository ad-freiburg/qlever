// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#pragma once

#include <absl/strings/str_cat.h>

#include <variant>

#include "parser/Iri.h"
#include "parser/Literal.h"
#include "util/Exception.h"

namespace ad_utility::triple_component {
static constexpr char literalPrefixChar = '"';
static constexpr char iriPrefixChar = '<';
static constexpr std::string_view iriPrefix{&iriPrefixChar, 1};
static constexpr std::string_view literalPrefix{&literalPrefixChar, 1};
// A wrapper class that can contain either an Iri or a Literal object.
class alignas(16) LiteralOrIri {
 private:
  using LiteralOrIriVariant = std::variant<Literal, Iri>;
  LiteralOrIriVariant data_;

 public:
  // Return contained Iri object if available, throw exception otherwise
  const Iri& getIri() const;

  // Return contained Literal object if available, throw exception
  // otherwise
  const Literal& getLiteral() const;

  // Create a new LiteralOrIri based on a Literal object
  explicit LiteralOrIri(Literal literal);

  // Create a new LiteralOrIri based on an Iri object
  explicit LiteralOrIri(Iri iri);

  const std::string& toStringRepresentation() const {
    auto impl = [](const auto& val) -> decltype(auto) {
      return val.toStringRepresentation();
    };
    return std::visit(impl, data_);
  }

  static LiteralOrIri fromStringRepresentation(std::string internal) {
    char tag = internal.front();
    if (tag == iriPrefixChar) {
      return LiteralOrIri{Iri::fromStringRepresentation(std::move(internal))};
    } else {
      AD_CORRECTNESS_CHECK(tag == literalPrefixChar);
      return LiteralOrIri{
          Literal::fromStringRepresentation(std::move(internal))};
    }
  }
  template <typename H>
  friend H AbslHashValue(H h,
                         const std::same_as<LiteralOrIri> auto& literalOrIri) {
    return H::combine(std::move(h), literalOrIri.data_);
  }
  bool operator==(const LiteralOrIri&) const = default;

  auto operator<=>(const LiteralOrIri& rhs) const {
    // TODO<joka921> Use something unicode-based for this.
    return toStringRepresentation() <=> rhs.toStringRepresentation();
  }

  // Return true if object contains an Iri object
  bool isIri() const;

  // Return iri content of contained Iri object without any leading or trailing
  // angled brackets. Throws Exception if object does not contain an Iri object.
  NormalizedStringView getIriContent() const;

  // Return true if object contains a Literal object
  bool isLiteral() const;

  // Return true if contained Literal object has a language tag, throw
  // exception if no Literal object is contained
  bool hasLanguageTag() const;

  // Return true if contained Literal object has a datatype, throw
  // exception if no Literal object is contained
  bool hasDatatype() const;

  // Return content of contained Literal as string without leading or training
  // quotation marks. Throw exception if no Literal object is contained
  NormalizedStringView getLiteralContent() const;

  // Return the language tag of the contained Literal without leading @
  // character. Throw exception if no Literal object is contained or object has
  // no language tag.
  NormalizedStringView getLanguageTag() const;

  // Return the datatype of the contained Literal without "^^" prefix.
  // Throw exception if no Literal object is contained or object has no
  // datatype.
  NormalizedStringView getDatatype() const;

  // Return the content of the contained Iri, or the contained Literal
  NormalizedStringView getContent() const;

  // Create a new Literal with optional datatype or language tag.
  //   The rdfContent is expected to be a valid string according to SPARQL 1.1
  //   Query Language, 19.8 Grammar, Rule [145], and to be surrounded by
  //   quotation marks (", """, ', or '''). If the second argument is set and of
  //   type IRI, it is stored as the datatype of the given literal. If the
  //   second argument is set and of type string, it is interpreted as the
  //   language tag of the given literal. The language tag string can optionally
  //   start with an @ character, which is removed during the automatic
  //   normalization. If no second argument is set, the literal is stored
  //   without any descriptor.
  static LiteralOrIri literalWithQuotes(
      std::string_view rdfContentWithQuotes,
      std::optional<std::variant<Iri, std::string>> descriptor = std::nullopt);

  // Similar to `fromEscapedRdfLiteral`, except the rdfContent is expected to
  // NOT BE surrounded by quotation marks.
  static LiteralOrIri literalWithoutQuotes(
      std::string_view rdfContentWithoutQuotes,
      std::optional<std::variant<Iri, std::string>> descriptor = std::nullopt);

  // Create a new iri given an iri with surrounding brackets
  static LiteralOrIri iriref(const std::string& stringWithBrackets);

  // Create a new iri given a prefix iri and its suffix
  static LiteralOrIri prefixedIri(const Iri& prefix, std::string_view suffix);

  // Printing for GTest
  friend void PrintTo(const LiteralOrIri& literalOrIri, std::ostream* os) {
    auto& s = *os;
    s << literalOrIri.toStringRepresentation();
  }
};

}  // namespace ad_utility::triple_component
