// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#include "parser/Literal.h"

#include <utility>
#include <variant>

#include "parser/LiteralOrIri.h"

namespace ad_utility::triple_component {
// __________________________________________
Literal::Literal(NormalizedString content) : content_{std::move(content)} {}

// __________________________________________
Literal::Literal(NormalizedString content, Iri datatype)
    : content_{std::move(content)}, descriptor_{std::move(datatype)} {}

// __________________________________________
Literal::Literal(NormalizedString content, NormalizedString languageTag)
    : content_{std::move(content)}, descriptor_{std::move(languageTag)} {}

// __________________________________________
bool Literal::hasLanguageTag() const {
  return std::holds_alternative<NormalizedString>(descriptor_);
}

// __________________________________________
bool Literal::hasDatatype() const {
  return std::holds_alternative<Iri>(descriptor_);
}

// __________________________________________
NormalizedStringView Literal::getContent() const { return content_; }

// __________________________________________
Iri Literal::getDatatype() const {
  if (!hasDatatype()) {
    AD_THROW("The literal does not have an explicit datatype.");
  }
  return std::get<Iri>(descriptor_);
}

// __________________________________________
NormalizedStringView Literal::getLanguageTag() const {
  if (!hasLanguageTag()) {
    AD_THROW("The literal does not have an explicit language tag.");
  }
  return std::get<NormalizedString>(descriptor_);
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
  if (!descriptor.has_value()) {
    return Literal(std::move(normalizedRdfContent));
  }

  using namespace RdfEscaping;
  auto visitLanguageTag =
      [&normalizedRdfContent](std::string&& languageTag) -> Literal {
    return {std::move(normalizedRdfContent),
            normalizeLanguageTag(std::move(languageTag))};
  };

  auto visitDatatype = [&normalizedRdfContent](Iri&& datatype) -> Literal {
    return {std::move(normalizedRdfContent), std::move(datatype)};
  };

  return std::visit(
      ad_utility::OverloadCallOperator{visitDatatype, visitLanguageTag},
      std::move(descriptor.value()));
}

void Literal::addLanguageTag(std::string_view languageTag) {
  descriptor_ = RdfEscaping::normalizeLanguageTag(languageTag);
}

void Literal::addDatatype(Iri datatype) { descriptor_ = std::move(datatype); }

std::string Literal::toInternalRepresentation() const {
  auto first = literalPrefix;
  uint64_t sz = getContent().size();
  if (hasLanguageTag()) {
    sz |= (1ul << 63);
  }
  std::string_view metaData{reinterpret_cast<char*>(&sz), sizeof(sz)};
  if (hasLanguageTag()) {
    return absl::StrCat(first, asStringViewUnsafe(getContent()),
                        asStringViewUnsafe(getLanguageTag()), metaData);
  } else if (hasDatatype()) {
    return absl::StrCat(first, asStringViewUnsafe(getContent()),
                        asStringViewUnsafe(getDatatype().getContent()),
                        metaData);
  } else {
    return absl::StrCat(first, asStringViewUnsafe(getContent()), metaData);
  }
}

Literal Literal::fromInternalRepresentation(std::string_view internal) {
  // TODO<joka921> checkSizes.
  // TODO<joka921> Check that it is indeed a literal.
  internal.remove_prefix(1);
  uint64_t sz;
  std::memcpy(&sz, internal.data() + internal.size() - sizeof(sz), sizeof(sz));
  internal.remove_suffix(sizeof(sz));
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

}  // namespace ad_utility::triple_component
