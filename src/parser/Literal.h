// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#pragma once

#include "parser/Iri.h"
#include "parser/NormalizedString.h"

namespace ad_utility::triple_component {
// A class to hold literal values.
class Literal {
 private:
  // Store the string value of the literal without the surrounding quotation
  // marks or trailing descriptor.
  //  "Hello World"@en -> Hello World
  NormalizedString content_;

  using LiteralDescriptorVariant =
      std::variant<std::monostate, NormalizedString, Iri>;

  // Store the optional language tag or the optional datatype if applicable
  // without their prefixes.
  //  "Hello World"@en -> en
  //  "Hello World"^^test:type -> test:type
  LiteralDescriptorVariant descriptor_;

  // Create a new literal without any descriptor
  explicit Literal(NormalizedString content);

  // Create a new literal with a datatype
  Literal(NormalizedString content, Iri datatype);

  // Create a new literal with a language tag
  Literal(NormalizedString content, NormalizedString languageTag);

  // Similar to `literalWithQuotes`, except the rdfContent is expected to
  // already be normalized
  static Literal literalWithNormalizedContent(
      NormalizedString normalizedRdfContent,
      std::optional<std::variant<Iri, string>> descriptor = std::nullopt);

 public:
  template <typename H>
  friend H AbslHashValue(H h, const std::same_as<Literal> auto& literal) {
    return H::combine(std::move(h), asStringViewUnsafe(literal.getContent()));
  }
  bool operator==(const Literal&) const = default;

  std::string toInternalRepresentation() const {
    char c = 0;
    auto first = std::string_view{&c, 1};
    uint64_t sz = getContent().size();
    if (hasLanguageTag()) {
      sz |= (1ul << 63);
    }
    std::string_view metaData{reinterpret_cast<char*>(&sz), sizeof(sz)};
    if (hasLanguageTag()) {
      return absl::StrCat(first, metaData, asStringViewUnsafe(getContent()),
                          asStringViewUnsafe(getLanguageTag()));
    } else if (hasDatatype()) {
      return absl::StrCat(first, metaData, asStringViewUnsafe(getContent()),
                          asStringViewUnsafe(getDatatype().getContent()));
    } else {
      return absl::StrCat(first, metaData, asStringViewUnsafe(getContent()));
    }
  }

  static Literal fromInternalRepresentation(std::string_view internal) {
    // TODO<joka921> checkSizes.
    // TODO<joka921> Check that it is indeed a literal.
    internal.remove_prefix(1);
    uint64_t sz;
    std::memcpy(&sz, internal.data(), sizeof(sz));
    internal.remove_prefix(sizeof(sz));
    bool hasLanguageTag = sz &= (1ul << 63);
    sz ^= (1ul << 63);
    bool hasDatatype = !hasLanguageTag && internal.size() > sz;

    auto content = internal.substr(0, sz);
    auto remainder = internal.substr(sz);
    if (hasLanguageTag) {
      return Literal{NormalizedString{asNormalizedStringViewUnsafe(content)},
                     NormalizedString{asNormalizedStringViewUnsafe(remainder)}};
    } else if (hasDatatype) {
      return Literal{NormalizedString{asNormalizedStringViewUnsafe(content)},
                     Iri::fromInternalRepresentation(remainder)};
    } else {
      return Literal{NormalizedString{asNormalizedStringViewUnsafe(content)}};
    }
  }
  // Return true if the literal has an assigned language tag
  bool hasLanguageTag() const;

  // Return true if the literal has an assigned datatype
  bool hasDatatype() const;

  // Return the value of the literal without quotation marks and  without any
  // datatype or language tag
  NormalizedStringView getContent() const;

  // Return the language tag of the literal, if available, without leading @
  // character. Throws an exception if the literal has no language tag.
  NormalizedStringView getLanguageTag() const;

  // Return the datatype of the literal, if available, without leading ^^
  // prefix. Throws an exception if the literal has no datatype.
  Iri getDatatype() const;

  // For documentation, see documentation of function
  // LiteralORIri::literalWithQuotes
  static Literal literalWithQuotes(
      std::string_view rdfContentWithQuotes,
      std::optional<std::variant<Iri, string>> descriptor = std::nullopt);

  void addLanguageTag(std::string_view languageTag);
  void addDatatype(Iri datatype);

  // For documentation, see documentation of function
  // LiteralORIri::literalWithoutQuotes
  static Literal literalWithoutQuotes(
      std::string_view rdfContentWithoutQuotes,
      std::optional<std::variant<Iri, string>> descriptor = std::nullopt);
};
}  // namespace ad_utility::triple_component
