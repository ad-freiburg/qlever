// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#ifndef QLEVER_SRC_PARSER_LITERAL_H
#define QLEVER_SRC_PARSER_LITERAL_H

#include <absl/strings/str_cat.h>

#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <variant>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "backports/shift.h"
#include "backports/three_way_comparison.h"
#include "parser/NormalizedString.h"
#include "rdfTypes/Iri.h"
#include "rdfTypes/RdfEscaping.h"
#include "util/Exception.h"
#include "util/OverloadCallOperator.h"

namespace ad_utility::triple_component {

// A class template to hold literal values. When `isOwning = true` (the
// default), storage is `std::string`. When `isOwning = false`, storage is
// `std::string_view` and all mutating/allocating functions are disabled. Use
// the `Literal` and `LiteralView` wrapper classes for the concrete owning and
// non-owning variants.
template <bool isOwning = true>
class BasicLiteral {
 private:
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

  CPP_template_2(typename = void)(requires(isOwning)) std::string
      toStringRepresentation() && {
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

  // For documentation, see documentation of function
  // LiteralOrIri::fromEscapedRdfLiteral.
  CPP_template_2(typename = void)(requires(isOwning)) static BasicLiteral
      fromEscapedRdfLiteral(std::string_view rdfContentWithQuotes,
                            std::optional<std::variant<Iri, std::string>>
                                descriptor = std::nullopt) {
    NormalizedString content =
        RdfEscaping::normalizeLiteralWithQuotes(rdfContentWithQuotes);
    return literalWithNormalizedContent(content, std::move(descriptor));
  }

  // Similar to `fromEscapedRdfLiteral`, except the rdfContent is expected to
  // already be normalized.
  CPP_template_2(typename = void)(requires(isOwning)) static BasicLiteral
      literalWithNormalizedContent(NormalizedStringView normalizedRdfContent,
                                   std::optional<std::variant<Iri, std::string>>
                                       descriptor = std::nullopt) {
    using namespace std::string_view_literals;
    auto quotes = "\""sv;
    auto actualContent =
        absl::StrCat(quotes, asStringViewUnsafe(normalizedRdfContent), quotes);
    auto sz = actualContent.size();
    auto literal = BasicLiteral{std::move(actualContent), sz};
    if (!descriptor.has_value()) {
      return literal;
    }

    using namespace RdfEscaping;
    auto visitLanguageTag = [&literal](std::string_view languageTag) {
      literal.addLanguageTag(languageTag);
    };

    auto visitDatatype = [&literal](const Iri& datatype) {
      literal.addDatatype(datatype);
    };

    std::visit(
        ad_utility::OverloadCallOperator{visitDatatype, visitLanguageTag},
        std::move(descriptor.value()));
    return literal;
  }

  CPP_template_2(typename = void)(requires(isOwning)) void addLanguageTag(
      std::string_view languageTag) {
    AD_CORRECTNESS_CHECK(!hasDatatype() && !hasLanguageTag());
    using namespace std::string_view_literals;
    if (ql::starts_with(languageTag, '@')) {
      absl::StrAppend(&content_, languageTag);
    } else {
      absl::StrAppend(&content_, "@"sv, languageTag);
    }
  }

  CPP_template_2(typename = void)(requires(isOwning)) void addDatatype(
      const Iri& datatype) {
    AD_CORRECTNESS_CHECK(!hasDatatype() && !hasLanguageTag());
    using namespace std::string_view_literals;
    absl::StrAppend(&content_, "^^"sv, datatype.toStringRepresentation());
  }

  // For documentation, see documentation of function
  // LiteralOrIri::literalWithoutQuotes.
  CPP_template_2(typename = void)(requires(isOwning)) static BasicLiteral
      literalWithoutQuotes(std::string_view rdfContentWithoutQuotes,
                           std::optional<std::variant<Iri, std::string>>
                               descriptor = std::nullopt) {
    NormalizedString content =
        RdfEscaping::normalizeLiteralWithoutQuotes(rdfContentWithoutQuotes);
    return literalWithNormalizedContent(content, std::move(descriptor));
  }

  // Returns true if the literal has no language tag or datatype suffix.
  bool isPlain() const;

  // Erase everything but the substring in the range ['start', 'start'+'length')
  // from the inner content. Note that the start position does not count the
  // leading quotes, so the first character after the quote has index 0.
  // Throws if either 'start' or 'start' + 'length' is out of bounds.
  CPP_template_2(typename = void)(requires(isOwning)) void setSubstr(
      std::size_t start, std::size_t length) {
    std::size_t contentLength =
        beginOfSuffix_ - 2;  // Ignore the two quotation marks
    AD_CONTRACT_CHECK(start <= contentLength &&
                      start + length <= contentLength);
    auto contentBegin = content_.begin() + 1;  // Ignore the leading quote
    ql::shift_left(contentBegin, contentBegin + start + length, start);
    content_.erase(length + 1, contentLength - length);
    beginOfSuffix_ = beginOfSuffix_ - (contentLength - length);
  }

  // Remove the datatype suffix from the Literal.
  CPP_template_2(typename = void)(
      requires(isOwning)) void removeDatatypeOrLanguageTag() {
    content_.erase(beginOfSuffix_);
  }

  // Replace the content of the Literal object with `newContent`.
  // It truncates or extends the content based on the length of newContent.
  // Used in UCASE/LCASE functions in StringExpressions.cpp.
  CPP_template_2(typename = void)(requires(isOwning)) void replaceContent(
      std::string_view newContent) {
    std::size_t originalContentLength = beginOfSuffix_ - 2;
    std::size_t minLength = std::min(originalContentLength, newContent.size());
    ql::ranges::copy(newContent.substr(0, minLength), content_.begin() + 1);
    if (newContent.size() <= originalContentLength) {
      content_.erase(newContent.size() + 1,
                     originalContentLength - newContent.size());
    } else {
      content_.insert(beginOfSuffix_ - 1,
                      newContent.substr(originalContentLength));
    }
    beginOfSuffix_ = newContent.size() + 2;
  }

  // Concatenates the content of the current literal with another literal.
  // If the language tag or datatype of the literals differ, the existing
  // language tag or datatype is removed from the current literal. Used in the
  // CONCAT function in StringExpressions.cpp.
  CPP_template_2(typename = void)(requires(isOwning)) void concat(
      const BasicLiteral& other) {
    if (!((hasLanguageTag() && other.hasLanguageTag() &&
           getLanguageTag() == other.getLanguageTag()) ||
          (hasDatatype() && other.hasDatatype() &&
           getDatatype() == other.getDatatype()))) {
      removeDatatypeOrLanguageTag();
    }
    const auto& otherContent = asStringViewUnsafe(other.getContent());
    content_.insert(beginOfSuffix_ - 1, otherContent);
    beginOfSuffix_ += otherContent.size();
  }
};

// Owning Literal type (stores its own `std::string`).
class Literal : public BasicLiteral<true> {
 public:
  using BasicLiteral<true>::BasicLiteral;
  Literal(BasicLiteral<true>&& base) : BasicLiteral<true>(std::move(base)) {}
  Literal(const BasicLiteral<true>& base) : BasicLiteral<true>(base) {}

  static Literal fromStringRepresentation(std::string internal) {
    return BasicLiteral<true>::fromStringRepresentation(std::move(internal));
  }
  static Literal fromEscapedRdfLiteral(
      std::string_view rdfContentWithQuotes,
      std::optional<std::variant<Iri, std::string>> descriptor = std::nullopt) {
    return BasicLiteral<true>::fromEscapedRdfLiteral(rdfContentWithQuotes,
                                                     std::move(descriptor));
  }
  static Literal literalWithNormalizedContent(
      NormalizedStringView normalizedRdfContent,
      std::optional<std::variant<Iri, std::string>> descriptor = std::nullopt) {
    return BasicLiteral<true>::literalWithNormalizedContent(
        normalizedRdfContent, std::move(descriptor));
  }
  static Literal literalWithoutQuotes(
      std::string_view rdfContentWithoutQuotes,
      std::optional<std::variant<Iri, std::string>> descriptor = std::nullopt) {
    return BasicLiteral<true>::literalWithoutQuotes(rdfContentWithoutQuotes,
                                                    std::move(descriptor));
  }
};

// Non-owning Literal view type (stores a `std::string_view`).
class LiteralView : public BasicLiteral<false> {
 public:
  using BasicLiteral<false>::BasicLiteral;
  LiteralView(BasicLiteral<false>&& base)
      : BasicLiteral<false>(std::move(base)) {}
  LiteralView(const BasicLiteral<false>& base) : BasicLiteral<false>(base) {}

  static LiteralView fromStringRepresentation(std::string_view internal) {
    return BasicLiteral<false>::fromStringRepresentation(internal);
  }
};

}  // namespace ad_utility::triple_component

#endif  // QLEVER_SRC_PARSER_LITERAL_H
