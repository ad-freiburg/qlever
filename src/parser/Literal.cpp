// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#include "parser/Literal.h"

#include <utility>
#include <variant>

#include "parser/LiteralOrIri.h"

static constexpr char quote{'"'};
static constexpr char at{'@'};
static constexpr char hat{'^'};

namespace ad_utility::triple_component {
// __________________________________________
Literal::Literal(std::string content, size_t beginOfSuffix)
    : content_{std::move(content)}, beginOfSuffix_{beginOfSuffix} {
  AD_CORRECTNESS_CHECK(content_.starts_with(quote));
  AD_CORRECTNESS_CHECK(beginOfSuffix_ >= 2);
  AD_CORRECTNESS_CHECK(content_[beginOfSuffix_ - 1] == quote);
  AD_CORRECTNESS_CHECK(beginOfSuffix_ == content_.size() ||
                       content_[beginOfSuffix] == at ||
                       content_[beginOfSuffix] == hat);
}

// __________________________________________
bool Literal::hasLanguageTag() const { return getSuffix().starts_with(at); }

// __________________________________________
bool Literal::hasDatatype() const { return getSuffix().starts_with(hat); }

// __________________________________________
NormalizedStringView Literal::getContent() const {
  return content().substr(1, beginOfSuffix_ - 2);
}

// __________________________________________
NormalizedStringView Literal::getDatatype() const {
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
NormalizedStringView Literal::getLanguageTag() const {
  if (!hasLanguageTag()) {
    AD_THROW("The literal does not have an explicit language tag.");
  }
  return content().substr(beginOfSuffix_ + 1);
}

// __________________________________________
Literal Literal::fromEscapedRdfLiteral(
    std::string_view rdfContentWithQuotes,
    std::optional<std::variant<Iri, std::string>> descriptor) {
  NormalizedString content =
      RdfEscaping::normalizeLiteralWithQuotes(rdfContentWithQuotes);

  return literalWithNormalizedContent(content, std::move(descriptor));
}

// __________________________________________
Literal Literal::literalWithoutQuotes(
    std::string_view rdfContentWithoutQuotes,
    std::optional<std::variant<Iri, string>> descriptor) {
  NormalizedString content =
      RdfEscaping::normalizeLiteralWithoutQuotes(rdfContentWithoutQuotes);

  return literalWithNormalizedContent(content, std::move(descriptor));
}

// __________________________________________
Literal Literal::literalWithNormalizedContent(
    NormalizedStringView normalizedRdfContent,
    std::optional<std::variant<Iri, string>> descriptor) {
  auto quotes = "\""sv;
  auto actualContent =
      absl::StrCat(quotes, asStringViewUnsafe(normalizedRdfContent), quotes);
  auto sz = actualContent.size();
  auto literal = Literal{std::move(actualContent), sz};
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

  std::visit(ad_utility::OverloadCallOperator{visitDatatype, visitLanguageTag},
             std::move(descriptor.value()));
  return literal;
}

// __________________________________________
void Literal::addLanguageTag(std::string_view languageTag) {
  AD_CORRECTNESS_CHECK(!hasDatatype() && !hasLanguageTag());
  if (languageTag.starts_with('@')) {
    absl::StrAppend(&content_, languageTag);
  } else {
    absl::StrAppend(&content_, "@"sv, languageTag);
  }
}

// __________________________________________
void Literal::addDatatype(const Iri& datatype) {
  AD_CORRECTNESS_CHECK(!hasDatatype() && !hasLanguageTag());
  absl::StrAppend(&content_, "^^"sv, datatype.toStringRepresentation());
}

// __________________________________________
const std::string& Literal::toStringRepresentation() const { return content_; }

// __________________________________________
std::string& Literal::toStringRepresentation() { return content_; }

// __________________________________________
Literal Literal::fromStringRepresentation(std::string internal) {
  // TODO<joka921> This is a little dangerous as there might be quotes in the
  // IRI which might lead to unexpected results here.
  AD_CORRECTNESS_CHECK(internal.starts_with('"'));
  auto endIdx = internal.rfind('"');
  AD_CORRECTNESS_CHECK(endIdx > 0);
  return Literal{std::move(internal), endIdx + 1};
}

}  // namespace ad_utility::triple_component
