//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

#include "engine/sparqlExpressions/NaryExpressionImpl.h"

using Lit = ad_utility::triple_component::Literal;
using IRI = ad_utility::triple_component::Iri;

namespace sparqlExpression {
namespace detail::rdfExpressions {

inline auto getIri = [](std::string_view str) {
  return IRI::fromStringRepresentation(absl::StrCat("<"sv, str, ">"sv));
};

template <auto FuncIri>
inline auto getDatatypeForLit = [](const Lit& literal) {
  if (literal.hasDatatype()) {
    return LiteralOrIri{FuncIri(asStringViewUnsafe(literal.getDatatype()))};
  } else if (literal.hasLanguageTag()) {
    return LiteralOrIri{FuncIri(RDF_LANGTAG_STRING)};
  } else {
    return LiteralOrIri{FuncIri(XSD_STRING)};
  }
};
constexpr auto getDatatypeIri = getDatatypeForLit<getIri>;

// ____________________________________________________________________________
inline auto getDatatype = [](LiteralOrString input) -> IdOrLiteralOrIri {
  if (std::holds_alternative<Lit>(input)) {
    return getDatatypeIri(std::get<Lit>(input));
  } else if (std::holds_alternative<std::string>(input)) {
    return getDatatypeIri(
        Lit::fromStringRepresentation(std::get<std::string>(input)));
  } else {
    AD_CORRECTNESS_CHECK(std::holds_alternative<std::monostate>(input));
    return Id::makeUndefined();
  }
};

using GetDatatype = NARY<1, FV<decltype(getDatatype), makeDatatypeValueGetter>>;

}  //  namespace detail::rdfExpressions

using namespace detail::rdfExpressions;
using Expr = SparqlExpression::Ptr;

Expr makeDatatypeExpression(Expr child) {
  return std::make_unique<GetDatatype>(std::move(child));
}

}  // namespace sparqlExpression
