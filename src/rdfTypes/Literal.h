// Copyright 2023 - 2026 The QLever Authors, in particular:
//
// 2023 Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

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
  //  For example `"Hello World"@en`  or `"With"Quote"^^<someDatatype>` (note
  //  that the quote in the middle is unescaped because this is the normalized
  //  form that QLever stores.
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

  // Return true if the literal has an assigned datatype. `XSD_STRING` is not
  // considered a datatype for this function, so literals with the `XSD_STRING`
  // datatype are considered to have no datatype.
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

  // Remove the datatype suffix from the Literal.
  void removeDatatypeOrLanguageTag();

  // Returns true if the literal has no language tag or datatype suffix.
  // `XSD_STRING` is not considered a datatype for this function, so literals
  // with the `XSD_STRING` datatype are considered plain.
  bool isPlain() const;
};

// Owning Literal type (stores its own `std::string`).
class Literal : public BasicLiteral<true> {
 public:
  using BasicLiteral<true>::BasicLiteral;

 private:
  Literal(BasicLiteral<true>&& base) : BasicLiteral<true>(std::move(base)) {}

 public:
  using BasicLiteral<true>::toStringRepresentation;

  template <typename H>
  friend H AbslHashValue(H h, const Literal& lit) {
    return H::combine(std::move(h),
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

  void addLanguageTag(std::string_view languageTag);

  // Add a datatype to the literal. If the literal already has a datatype or a
  // language tag, this function will throw an error. The `XSD_STRING` datatype
  // is ignored.
  void addDatatype(const Iri& datatype);

  // For documentation, see documentation of function
  // LiteralORIri::literalWithoutQuotes
  static Literal literalWithoutQuotes(
      std::string_view rdfContentWithoutQuotes,
      std::optional<std::variant<Iri, std::string>> descriptor = std::nullopt);

  // Erase everything but the substring in the range ['start', 'start'+'length')
  // from the inner content. Note that the start position does not count the
  // leading quotes, so the first character after the quote has index 0.
  // Throws if either 'start' or 'start' + 'length' is out of bounds.
  // from the inner content.
  void setSubstr(std::size_t start, std::size_t length);

  // Replace the content of the Literal object with `newContent`.
  // It truncates or extends the content based on the length of newContent
  // Used in UCASE/LCASE functions in StringExpressions.cpp.
  void replaceContent(std::string_view newContent);

  // Concatenates the content of the current literal with another literal.
  // If the language tag or datatype of the literals differ, the existing
  // language tag or datatype is removed from the current literal. Used in the
  // CONCAT function in StringExpressions.cpp.
  void concat(const Literal& other);
};

// Non-owning Literal view type (stores a `std::string_view`).
class LiteralView : public BasicLiteral<false> {
 public:
  using BasicLiteral<false>::BasicLiteral;

 private:
  LiteralView(BasicLiteral<false>&& base)
      : BasicLiteral<false>(std::move(base)) {}

 public:
  template <typename H>
  friend H AbslHashValue(H h, const LiteralView& lit) {
    return H::combine(std::move(h),
                      static_cast<const BasicLiteral<false>&>(lit));
  }

  static LiteralView fromStringRepresentation(std::string_view internal) {
    return BasicLiteral<false>::fromStringRepresentation(internal);
  }
};

}  // namespace ad_utility::triple_component

#endif  // QLEVER_SRC_PARSER_LITERAL_H
