// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#ifndef QLEVER_SRC_PARSER_LITERALORIRI_H
#define QLEVER_SRC_PARSER_LITERALORIRI_H

#include <variant>

#include "backports/three_way_comparison.h"
#include "rdfTypes/Iri.h"
#include "rdfTypes/Literal.h"

// Forward declaration
class IndexImpl;

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
  // Pointer to the IndexImpl for accessing the comparator
  const IndexImpl* index_ = nullptr;

 public:
  // Return contained Iri object if available, throw exception otherwise
  const Iri& getIri() const;

  // Return a modifiable reference to the contained Iri object if available,
  // throw exception otherwise. Allows the caller to modify the Iri object
  // e.g. for SubStr in StringExpressions.cpp
  Iri& getIri();

  // Return contained Literal object if available, throw exception
  // otherwise
  const Literal& getLiteral() const;

  // Return a modifiable reference to the contained Literal object if available,
  // throw exception otherwise. Allows the caller to modify the Literal object
  // e.g. for SubStr in StringExpressions.cpp
  Literal& getLiteral();

  // Create a new LiteralOrIri based on a Literal object
  // The index parameter is required for proper comparison operations.
  explicit LiteralOrIri(Literal literal, const IndexImpl* index);

  // Create a new LiteralOrIri based on an Iri object
  // The index parameter is required for proper comparison operations.
  explicit LiteralOrIri(Iri iri, const IndexImpl* index);

  // Get the associated IndexImpl pointer
  const IndexImpl* getIndexImpl() const { return index_; }

  // Set the associated IndexImpl pointer (used internally for comparison)
  void setIndexImpl(const IndexImpl* index) { index_ = index; }

  const std::string& toStringRepresentation() const {
    auto impl = [](const auto& val) -> decltype(auto) {
      return val.toStringRepresentation();
    };
    return std::visit(impl, data_);
  }

  static LiteralOrIri fromStringRepresentation(std::string internal,
                                               const IndexImpl* index) {
    AD_CONTRACT_CHECK(index != nullptr,
                      "LiteralOrIri requires a non-null IndexImpl pointer");
    char tag = internal.front();
    if (tag == literalPrefixChar) {
      return LiteralOrIri{Literal::fromStringRepresentation(std::move(internal)),
                          index};
    } else {
      return LiteralOrIri{Iri::fromStringRepresentation(std::move(internal)),
                          index};
    }
  }
  CPP_template(typename H, typename L)(
      requires ql::concepts::same_as<L, LiteralOrIri>) friend H
      AbslHashValue(H h, const L& literalOrIri) {
    // Only hash the data, not the index pointer
    return H::combine(std::move(h), literalOrIri.data_);
  }

  // Equality only depends on data_, not on index_
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(LiteralOrIri, data_)

  ql::strong_ordering compareThreeWay(const LiteralOrIri& rhs) const;
  QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL(LiteralOrIri)

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

  // Return content of contained Literal as string without leading or trailing
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
      const IndexImpl* index,
      std::optional<std::variant<Iri, std::string>> descriptor = std::nullopt);

  // Similar to `fromEscapedRdfLiteral`, except the rdfContent is expected to
  // NOT BE surrounded by quotation marks.
  static LiteralOrIri literalWithoutQuotes(
      std::string_view rdfContentWithoutQuotes,
      const IndexImpl* index,
      std::optional<std::variant<Iri, std::string>> descriptor = std::nullopt);

  // Create a new iri given an iri with surrounding brackets
  static LiteralOrIri iriref(const std::string& stringWithBrackets,
                             const IndexImpl* index);

  // Create a new iri given a prefix iri and its suffix
  static LiteralOrIri prefixedIri(const Iri& prefix, std::string_view suffix,
                                  const IndexImpl* index);

  // Printing for GTest
  friend void PrintTo(const LiteralOrIri& literalOrIri, std::ostream* os) {
    auto& s = *os;
    s << literalOrIri.toStringRepresentation();
  }
};

}  // namespace ad_utility::triple_component

#endif  // QLEVER_SRC_PARSER_LITERALORIRI_H
