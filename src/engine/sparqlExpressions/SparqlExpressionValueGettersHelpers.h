//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

// SparqlExpressionValueGettersHelpers.h exists to keep
// SparqlExpressionValueGetters.h a bit cleaned up.

#pragma once

using IRI = ad_utility::triple_component::Iri;

namespace sparqlExpression::detail {
namespace helpers {

//______________________________________________________________________________
inline auto iriFromDate = [](Id id) -> std::optional<IRI> {
  AD_CORRECTNESS_CHECK(id.getDatatype() == Datatype::Date);
  auto dateType = id.getDate().toStringAndType().second;
  if (dateType != nullptr) {
    return IRI::fromIri(dateType);
  }
  return std::nullopt;
};

//______________________________________________________________________________
inline auto iriFromLiteral = [](const std::string& str) -> std::optional<IRI> {
  // check that we return an Iri w.r.t. Literals
  if (!(str.starts_with("\"") || str.starts_with("'"))) {
    return std::nullopt;
  }
  auto posDt = str.rfind("^^");
  if (posDt != std::string::npos) {
    return Iri::fromStringRepresentation(str.substr(posDt + 2));
  } else if (str.rfind("@") != std::string::npos) {
    return Iri::fromIri(RDF_LANGTAG_STRING);
  } else {
    return Iri::fromIri(XSD_STRING);
  }
};

}  //  namespace helpers

//______________________________________________________________________________
template <auto FuncDate, auto FuncLiteral>
inline auto getIriFromId(const Index& index, Id id,
                         const LocalVocab& localVocab) -> std::optional<IRI> {
  using enum Datatype;
  auto datatype = id.getDatatype();
  std::optional<std::string> entity;
  switch (datatype) {
    case Bool:
      return IRI::fromIri(XSD_BOOLEAN_TYPE);
    case Double:
      return IRI::fromIri(XSD_DOUBLE_TYPE);
    case Int:
      return IRI::fromIri(XSD_INT_TYPE);
    case Date:
      return FuncDate(id);
    case LocalVocabIndex:
      return FuncLiteral(
          localVocab.getWord(id.getLocalVocabIndex()).toStringRepresentation());
    case TextRecordIndex:
      return FuncLiteral(index.getTextExcerpt(id.getTextRecordIndex()));
    case WordVocabIndex:
      entity = index.idToOptionalString(id.getWordVocabIndex());
      AD_CONTRACT_CHECK(entity.has_value());
      return FuncLiteral(entity.value());
    case VocabIndex:
      entity = index.idToOptionalString(id.getVocabIndex());
      AD_CONTRACT_CHECK(entity.has_value());
      return FuncLiteral(entity.value());
    case Undefined:
    case BlankNodeIndex:
      return std::nullopt;
  }
  AD_FAIL();
};

// used for `makeDatatypeValueGetter`, returns object of type
// `std::optional<ad_utility::triple_component::Iri>`
constexpr auto idToIri =
    getIriFromId<helpers::iriFromDate, helpers::iriFromLiteral>;

}  // namespace sparqlExpression::detail
