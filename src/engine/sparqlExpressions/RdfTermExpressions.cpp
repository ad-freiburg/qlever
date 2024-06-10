//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

#include "engine/sparqlExpressions/NaryExpressionImpl.h"

using Lit = ad_utility::triple_component::Literal;
using IRI = ad_utility::triple_component::Iri;

namespace sparqlExpression {
namespace detail {

inline auto getIri = [](std::string_view str) {
  return IRI::fromStringRepresentation(absl::StrCat("<"sv, str, ">"sv));
};

inline auto getDatatype = [](OptLiteral input) -> IdOrLiteralOrIri {
  if (!input.has_value()) {
    return Id::makeUndefined();
  } else {
    Lit literal = input.value();
    if (literal.hasDatatype()) {
      return LiteralOrIri{getIri(asStringViewUnsafe(literal.getDatatype()))};
    } else if (literal.hasLanguageTag()) {
      return LiteralOrIri{getIri(RDF_LANGTAG_STRING)};
    } else {
      return LiteralOrIri{getIri(XSD_STRING)};
    }
  }
};

  using GetDatatype = NARY<1, FV<decltype(getDatatype), LiteralValueGetter>>;
}  //  namespace detail

using namespace detail;
using Expr = SparqlExpression::Ptr;

Expr makeDatatypeExpression(Expr child) {
  return std::make_unique<GetDatatype>(std::move(child));
}

}  // namespace sparqlExpression
