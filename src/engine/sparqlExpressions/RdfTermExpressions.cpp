//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/RelationalExpressionHelpers.h"

namespace sparqlExpression {
namespace detail::rdfExpressions {

struct GetDatatypeImpl {
  IdOrLiteralOrIri operator()(OptIri input) const {
    if (!input.has_value()) {
      return Id::makeUndefined();
    } else {
      return LiteralOrIri{std::move(input.value())};
    }
  }
};

using GetDatatype = NARY<1, FV<GetDatatypeImpl, DatatypeValueGetter>>;

// Implementation of `sameTerm(term1, term2)`, see
// https://www.w3.org/TR/sparql11-query/#func-sameTerm. Return true iff `term1`
// and `term2` are the same RDF term, false otherwise, and an error (UNDEF) if
// one of them is unbound (or the result of an error). In contrast to `=`, this
// never compares the values of literals, so for example `sameTerm(1, 1.0)` is
// false and `sameTerm("NaN"^^xsd:double, "NaN"^^xsd:double)` is true, whereas
// `1 = 1.0` is true and `"NaN"^^xsd:double = "NaN"^^xsd:double` is false.
//
// NOTE: Two `Id`s are compared via `Id::operator==`, which is exactly the
// identity of the RDF terms that QLever stores:
// 1. Two words from the vocabularies (of the index, the local vocab, or an
//    encoded IRI) are equal iff their strings are byte-wise equal. In
//    particular, the comparison does not use the (looser) collation that `=`
//    uses, so `sameTerm("alpha", "Alpha")` is false.
// 2. An `Id` that directly encodes a value (`Int`, `Double`, `Bool`, `Date`,
//    `GeoPoint`) is equal to another such `Id` iff the value and the datatype
//    of the `Id`s are equal. As the lexical form and the exact datatype of such
//    a literal are not stored, literals that are folded into the same `Id`
//    are considered to be the same term, although strictly speaking they are
//    not: For example, `"1"^^xsd:integer`, `"01"^^xsd:integer`, and
//    `"1"^^xsd:int` are all stored as the `Int` 1, and `"1.0"^^xsd:decimal` and
//    `"1.0E0"^^xsd:double` are both stored as the `Double` 1.0. On the other
//    hand, `"1"^^xsd:boolean` and `"true"^^xsd:boolean` are stored differently,
//    and hence are different terms.
// 3. All `Double` NaNs are the same term (independent of the sign and the
//    payload bits, which are not part of the RDF term `"NaN"^^xsd:double`).
struct SameTermImpl {
  Id operator()(const IdOrLocalVocabEntry& term1,
                const IdOrLocalVocabEntry& term2) const {
    // NOTE: The `Id` of a `LocalVocabEntry` points to that entry, which lives
    // as long as the arguments of this function, so the `Id`s can only be used
    // within this function.
    Id id1 = makeValueId(term1, nullptr);
    Id id2 = makeValueId(term2, nullptr);
    if (id1.isUndefined() || id2.isUndefined()) {
      return Id::makeUndefined();
    }
    auto isNaN = [](Id id) {
      return id.getDatatype() == Datatype::Double && std::isnan(id.getDouble());
    };
    if (isNaN(id1) || isNaN(id2)) {
      return Id::makeFromBool(isNaN(id1) && isNaN(id2));
    }
    return Id::makeFromBool(id1 == id2);
  }
};

using SameTerm = NARY<2, FV<SameTermImpl, RdfTermValueGetter>>;

}  //  namespace detail::rdfExpressions

using namespace detail::rdfExpressions;
using Expr = SparqlExpression::Ptr;

Expr makeDatatypeExpression(Expr child) {
  return std::make_unique<GetDatatype>(std::move(child));
}

Expr makeSameTermExpression(Expr child1, Expr child2) {
  return std::make_unique<SameTerm>(std::move(child1), std::move(child2));
}

}  // namespace sparqlExpression
