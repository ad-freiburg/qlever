// Copyright 2023 - 2026 The QLever Authors, in particular:
//
// 2023 Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "rdfTypes/Literal.h"

#include <absl/strings/str_cat.h>

#include <utility>

#include "backports/algorithm.h"
#include "backports/shift.h"
#include "global/Constants.h"
#include "rdfTypes/RdfEscaping.h"
#include "util/OverloadCallOperator.h"

static constexpr char quote{'"'};
static constexpr char at{'@'};
static constexpr char hat{'^'};

namespace ad_utility::triple_component {

// __________________________________________
template <bool isOwning>
BasicLiteral<isOwning>::BasicLiteral(StorageType content, size_t beginOfSuffix)
    : content_{std::move(content)}, beginOfSuffix_{beginOfSuffix} {
  AD_CORRECTNESS_CHECK(ql::starts_with(content_, quote));
  AD_CORRECTNESS_CHECK(beginOfSuffix_ >= 2);
  AD_CORRECTNESS_CHECK(content_[beginOfSuffix_ - 1] == quote);
  AD_CORRECTNESS_CHECK(beginOfSuffix_ == content_.size() ||
                       content_[beginOfSuffix] == at ||
                       content_[beginOfSuffix] == hat);
}

// __________________________________________
template <bool isOwning>
bool BasicLiteral<isOwning>::hasLanguageTag() const {
  return ql::starts_with(getSuffix(), at);
}

// __________________________________________
template <bool isOwning>
bool BasicLiteral<isOwning>::hasDatatype() const {
  return ql::starts_with(getSuffix(), hat);
}

// __________________________________________
template <bool isOwning>
NormalizedStringView BasicLiteral<isOwning>::getContent() const {
  return content().substr(1, beginOfSuffix_ - 2);
}

// __________________________________________
template <bool isOwning>
NormalizedStringView BasicLiteral<isOwning>::getDatatype() const {
  if (!hasDatatype()) {
    AD_THROW("The literal does not have an explicit datatype.");
  }
  // We don't return the enclosing <angle brackets>
  NormalizedStringView result = content();
  result.remove_prefix(beginOfSuffix_ + 3);
  result.remove_suffix(1);
  return result;
}

// __________________________________________
template <bool isOwning>
NormalizedStringView BasicLiteral<isOwning>::getLanguageTag() const {
  if (!hasLanguageTag()) {
    AD_THROW("The literal does not have an explicit language tag.");
  }
  return content().substr(beginOfSuffix_ + 1);
}

// __________________________________________
template <bool isOwning>
BasicLiteral<isOwning> BasicLiteral<isOwning>::fromStringRepresentation(
    StorageType internal) {
  // TODO<joka921> This is a little dangerous as there might be quotes in the
  // IRI which might lead to unexpected results here.
  AD_CORRECTNESS_CHECK(ql::starts_with(internal, '"'));
  auto endIdx = internal.rfind('"');
  AD_CORRECTNESS_CHECK(endIdx > 0);
  BasicLiteral literal{std::move(internal), endIdx + 1};
  // Remove redundant `XSD_STRING` datatype. If vocabularies normalize strings
  // like this in the future, this could get replaced by an assertion if all the
  // other callers are updated accordingly.
  if (literal.hasDatatype() &&
      asStringViewUnsafe(literal.getDatatype()) == XSD_STRING) {
    literal.removeDatatypeOrLanguageTag();
  }
  return literal;
}

// __________________________________________
template <bool isOwning>
bool BasicLiteral<isOwning>::isPlain() const {
  return beginOfSuffix_ == content_.size();
}

// ____________________________________________________________________________
template <bool isOwning>
void BasicLiteral<isOwning>::removeDatatypeOrLanguageTag() {
  if constexpr (isOwning) {
    content_.erase(beginOfSuffix_);
  } else {
    content_ = content_.substr(0, beginOfSuffix_);
  }
}

template class BasicLiteral<true>;
template class BasicLiteral<false>;

// ____________________________________________________________________________
// Literal (owning) method implementations.
// ____________________________________________________________________________

Literal Literal::fromStringRepresentation(std::string internal) {
  return BasicLiteral<true>::fromStringRepresentation(std::move(internal));
}

// ____________________________________________________________________________
Literal Literal::fromEscapedRdfLiteral(
    std::string_view rdfContentWithQuotes,
    std::optional<std::variant<Iri, std::string>> descriptor) {
  NormalizedString content =
      RdfEscaping::normalizeLiteralWithQuotes(rdfContentWithQuotes);
  return literalWithNormalizedContent(content, std::move(descriptor));
}

// ____________________________________________________________________________
Literal Literal::literalWithNormalizedContent(
    NormalizedStringView normalizedRdfContent,
    std::optional<std::variant<Iri, std::string>> descriptor) {
  using namespace std::string_view_literals;
  auto quotes = "\""sv;
  auto actualContent =
      absl::StrCat(quotes, asStringViewUnsafe(normalizedRdfContent), quotes);
  auto sz = actualContent.size();
  auto literal = Literal{std::move(actualContent), sz};
  if (!descriptor.has_value()) {
    return literal;
  }

  auto visitLanguageTag = [&literal](std::string_view languageTag) {
    literal.addLanguageTag(languageTag);
  };

  auto visitDatatype = [&literal](const Iri& datatype) {
    literal.addDatatype(datatype);
  };

  std::visit(ad_utility::OverloadCallOperator{visitDatatype, visitLanguageTag},
             std::move(descriptor.value()));
  return literal;
}

// ____________________________________________________________________________
Literal Literal::literalWithoutQuotes(
    std::string_view rdfContentWithoutQuotes,
    std::optional<std::variant<Iri, std::string>> descriptor) {
  NormalizedString content =
      RdfEscaping::normalizeLiteralWithoutQuotes(rdfContentWithoutQuotes);
  return literalWithNormalizedContent(content, std::move(descriptor));
}

// ____________________________________________________________________________
void Literal::addLanguageTag(std::string_view languageTag) {
  AD_CORRECTNESS_CHECK(!hasDatatype() && !hasLanguageTag());
  using namespace std::string_view_literals;
  if (ql::starts_with(languageTag, '@')) {
    absl::StrAppend(&content_, languageTag);
  } else {
    absl::StrAppend(&content_, "@"sv, languageTag);
  }
}

// ____________________________________________________________________________
void Literal::addDatatype(const Iri& datatype) {
  AD_CORRECTNESS_CHECK(!hasDatatype() && !hasLanguageTag());
  // Trim the default string datatype.
  using namespace std::string_view_literals;
  if (asStringViewUnsafe(datatype.getContent()) != XSD_STRING) {
    absl::StrAppend(&content_, "^^"sv, datatype.toStringRepresentation());
  }
}

// __________________________________________
void Literal::setSubstr(std::size_t start, std::size_t length) {
  std::size_t contentLength =
      beginOfSuffix_ - 2;  // Ignore the two quotation marks
  AD_CONTRACT_CHECK(start <= contentLength && start + length <= contentLength);
  auto contentBegin = content_.begin() + 1;  // Ignore the leading quote
  ql::shift_left(contentBegin, contentBegin + start + length, start);
  content_.erase(length + 1, contentLength - length);
  beginOfSuffix_ = beginOfSuffix_ - (contentLength - length);
}

// ____________________________________________________________________________
void Literal::replaceContent(std::string_view newContent) {
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

// ____________________________________________________________________________
void Literal::concat(const BasicLiteral<true>& other) {
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

}  // namespace ad_utility::triple_component
