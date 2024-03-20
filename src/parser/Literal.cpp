// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#include "parser/Literal.h"

#include <utility>
#include <variant>

#include "parser/LiteralOrIri.h"

namespace ad_utility::triple_component {
// __________________________________________
Literal::Literal(NormalizedString content, size_t beginOfSuffix)
    : content_{std::move(content)}, beginOfSuffix_{beginOfSuffix} {
  NormalizedChar quote{'"'};
  NormalizedChar at{'@'};
  NormalizedChar hat{'^'};
  AD_CORRECTNESS_CHECK(content_.starts_with(quote));
  AD_CORRECTNESS_CHECK(beginOfSuffix_ >= 2);
  AD_CORRECTNESS_CHECK(content_[beginOfSuffix_ - 1] == quote);
  AD_CORRECTNESS_CHECK(beginOfSuffix_ == content_.size() ||
                       content_[beginOfSuffix] == at ||
                       content_[beginOfSuffix] == hat);
}

// __________________________________________
bool Literal::hasLanguageTag() const {
  return getSuffix().starts_with(NormalizedChar{'@'});
}

// __________________________________________
bool Literal::hasDatatype() const {
  return getSuffix().starts_with(NormalizedChar{'^'});
}

// __________________________________________
NormalizedStringView Literal::getContent() const {
  return NormalizedStringView{content_}.substr(1, beginOfSuffix_ - 2);
}

// __________________________________________
NormalizedStringView Literal::getDatatype() const {
  if (!hasDatatype()) {
    AD_THROW("The literal does not have an explicit datatype.");
  }
  // We don't return the enclosing <angle brackets>
  NormalizedStringView result = content_;
  result.remove_prefix(beginOfSuffix_ + 3);
  result.remove_suffix(1);
  return result;
}

// __________________________________________
NormalizedStringView Literal::getLanguageTag() const {
  if (!hasLanguageTag()) {
    AD_THROW("The literal does not have an explicit language tag.");
  }
  return NormalizedStringView{content_}.substr(beginOfSuffix_ + 1);
}

// __________________________________________
Literal Literal::literalWithQuotes(
    std::string_view rdfContentWithQuotes,
    std::optional<std::variant<Iri, string>> descriptor) {
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
    NormalizedString normalizedRdfContent,
    std::optional<std::variant<Iri, string>> descriptor) {
  auto quotes = asNormalizedStringViewUnsafe("\"");
  auto actualContent = quotes + std::move(normalizedRdfContent) + quotes;
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
  content_.push_back(NormalizedChar{'@'});
  content_.append(RdfEscaping::normalizeLanguageTag(languageTag));
}

// __________________________________________
void Literal::addDatatype(const Iri& datatype) {
  content_.append(asNormalizedStringViewUnsafe("^^"));
  content_.append(
      asNormalizedStringViewUnsafe(datatype.toInternalRepresentation()));
}

// __________________________________________
std::string_view Literal::toInternalRepresentation() const {
  return asStringViewUnsafe(content_);
}

// __________________________________________
Literal Literal::fromInternalRepresentation(std::string_view input) {
  // TODO<joka921> This is a little dangerous as there might be quotes in the
  // IRI which might lead to unexpected results here.
  AD_CORRECTNESS_CHECK(input.starts_with('"'));
  auto endIdx = input.rfind('"');
  AD_CORRECTNESS_CHECK(endIdx > 0);
  return Literal{NormalizedString{asNormalizedStringViewUnsafe(input)},
                 endIdx + 1};
}

}  // namespace ad_utility::triple_component
