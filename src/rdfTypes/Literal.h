// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#ifndef QLEVER_SRC_PARSER_LITERAL_H
#define QLEVER_SRC_PARSER_LITERAL_H

#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <variant>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/concepts.h"
#include "backports/three_way_comparison.h"
#include "parser/NormalizedString.h"
#include "rdfTypes/Iri.h"

namespace ad_utility::triple_component {

// A class template to hold literal values. When `isOwning = true` (the
// default), storage is `std::string`. When `isOwning = false`, storage is
// `std::string_view` and all mutating/allocating functions are disabled. Use
// the `Literal` and `LiteralView` wrapper classes for the concrete owning and
// non-owning variants.
template <bool isOwning = true>
class BasicLiteral {
 protected:
  using StorageType =
      std::conditional_t<isOwning, std::string, std::string_view>;

  // Store the normalized version of the literal, including possible datatypes
  // and descriptors.
  StorageType content_;
  // The position after the closing `"`, so either the size of the string, or
  // the position of the `@` or `^^` for literals with language tags or
  // datatypes.
  std::size_t beginOfSuffix_;

  // Create a new literal without any descriptor.
  explicit BasicLiteral(StorageType content, size_t beginOfSuffix);

  // Internal helper function. Return either the empty string (for a plain
  // literal), `@langtag` or `^^<datatypeIri>`.
  std::string_view getSuffix() const {
    std::string_view result = content_;
    result.remove_prefix(beginOfSuffix_);
    return result;
  }

  NormalizedStringView content() const {
    return asNormalizedStringViewUnsafe(content_);
  }

 public:
  CPP_template(typename H, typename L)(
      requires ql::concepts::same_as<L, BasicLiteral>) friend H
      AbslHashValue(H h, const L& literal) {
    return H::combine(std::move(h), literal.content_);
  }
  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(BasicLiteral, content_,
                                              beginOfSuffix_)

  std::conditional_t<isOwning, const std::string&, std::string_view>
  toStringRepresentation() const& {
    return content_;
  }

  std::string toStringRepresentation() && requires(isOwning) {
    return std::move(content_);
  }

  static BasicLiteral fromStringRepresentation(StorageType internal);

  // Return true if the literal has an assigned language tag.
  bool hasLanguageTag() const;

  // Return true if the literal has an assigned datatype.
  bool hasDatatype() const;

  // Return the value of the literal without quotation marks and without any
  // datatype or language tag.
  NormalizedStringView getContent() const;

  // Return the language tag of the literal, if available, without leading @
  // character. Throws an exception if the literal has no language tag.
  NormalizedStringView getLanguageTag() const;

  // Return the datatype of the literal, if available, without leading ^^
  // prefix. Throws an exception if the literal has no datatype.
  NormalizedStringView getDatatype() const;

  // Returns true if the literal has no language tag or datatype suffix.
  bool isPlain() const;
};

// Owning Literal type (stores its own `std::string`).
class Literal : public BasicLiteral<true> {
 public:
  using BasicLiteral<true>::BasicLiteral;
  Literal(BasicLiteral<true>&& base) : BasicLiteral<true>(std::move(base)) {}
  Literal(const BasicLiteral<true>& base) : BasicLiteral<true>(base) {}

  using BasicLiteral<true>::toStringRepresentation;

  template <typename H>
  friend H AbslHashValue(H h, const Literal& lit) {
    return AbslHashValue(std::move(h),
                         static_cast<const BasicLiteral<true>&>(lit));
  }

  static Literal fromStringRepresentation(std::string internal);

  // Create a Literal from an escaped RDF literal string with quotes.
  static Literal fromEscapedRdfLiteral(
      std::string_view rdfContentWithQuotes,
      std::optional<std::variant<Iri, std::string>> descriptor = std::nullopt);

  // Similar to `fromEscapedRdfLiteral`, except the rdfContent is expected to
  // already be normalized.
  static Literal literalWithNormalizedContent(
      NormalizedStringView normalizedRdfContent,
      std::optional<std::variant<Iri, std::string>> descriptor = std::nullopt);

  // Create a Literal without surrounding quotes.
  static Literal literalWithoutQuotes(
      std::string_view rdfContentWithoutQuotes,
      std::optional<std::variant<Iri, std::string>> descriptor = std::nullopt);

  void addLanguageTag(std::string_view languageTag);
  void addDatatype(const Iri& datatype);

  // Erase everything but the substring in the range ['start', 'start'+'length')
  // from the inner content.
  void setSubstr(std::size_t start, std::size_t length);

  // Remove the datatype suffix from the Literal.
  void removeDatatypeOrLanguageTag();

  // Replace the content of the Literal object with `newContent`.
  void replaceContent(std::string_view newContent);

  // Concatenates the content of the current literal with another literal.
  void concat(const BasicLiteral<true>& other);
};

// Non-owning Literal view type (stores a `std::string_view`).
class LiteralView : public BasicLiteral<false> {
 public:
  using BasicLiteral<false>::BasicLiteral;
  LiteralView(BasicLiteral<false>&& base)
      : BasicLiteral<false>(std::move(base)) {}
  LiteralView(const BasicLiteral<false>& base) : BasicLiteral<false>(base) {}

  template <typename H>
  friend H AbslHashValue(H h, const LiteralView& lit) {
    return AbslHashValue(std::move(h),
                         static_cast<const BasicLiteral<false>&>(lit));
  }

  static LiteralView fromStringRepresentation(std::string_view internal) {
    return BasicLiteral<false>::fromStringRepresentation(internal);
  }
};

}  // namespace ad_utility::triple_component

#endif  // QLEVER_SRC_PARSER_LITERAL_H
