// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Hannah Bast <bast@cs.uni-freiburg.de>

#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"

namespace sparqlExpression {
namespace detail {

// Quick recap of how defining n-ary functions works in QLever.
//
// 1. Define a subclass of `SparqlExpression` (see `SparqlExpression.h`),
//    called `...Expression`. For n-ary functions, make your life easier by
//    using the generic `NARY` (defined in `NaryExpressionImpl.h`).
//
// 2. NARY takes two arguments: the number of arguments (n) and a template
//    with the value getters for the arguments (one if all arguments are of
//    the same type, otherwise exactly one for each argument) and a function
//    to be applied to the results of the value getters.
//
// 3. Implement a function `make...Expression` that takes n arguments of type
//    `SparqlExpression::Ptr` and returns a `unique_ptr` to a new instance of
//    `...Expression` (`std::move` the arguments into the constructor). The
//    function should be declared in `NaryExpression.h`.
template <typename NaryOperation, prefilterExpressions::IsDatatype Datatype>
requires(isOperation<NaryOperation>)
class IsDatatypeExpressionImpl : public NaryExpression<NaryOperation> {
 public:
  using NaryExpression<NaryOperation>::NaryExpression;
  std::vector<PrefilterExprVariablePair> getPrefilterExpressionForMetadata(
      [[maybe_unused]] bool isNegated) const override {
    using namespace prefilterExpressions;
    std::vector<PrefilterExprVariablePair> prefilterVec;
    const auto& children = this->children();
    AD_CORRECTNESS_CHECK(children.size() == 1);
    const SparqlExpression* childExpr = children[0].get();
    // `VariableExpression` (e.g. `isLiteral(?x)`).
    std::optional<Variable> optVar = childExpr->getVariableOrNullopt();
    if (optVar.has_value()) {
      prefilterVec.emplace_back(
          std::make_unique<IsDatatypeExpression<Datatype>>(), optVar.value());
    }
    return prefilterVec;
  }
};

//______________________________________________________________________________
// Expressions for the builtin functions `isIRI`, `isBlank`, `isLiteral`,
// `isNumeric`, and the custom function `isWktPoint`. Note that the value
// getters already return the correct `Id`, hence `std::identity`.
template <typename Getter, prefilterExpressions::IsDatatype Datatype>
using IsDtypeExpression =
    IsDatatypeExpressionImpl<Operation<1, FV<std::identity, Getter>>, Datatype>;

using isLiteralExpression =
    IsDtypeExpression<IsLiteralValueGetter,
                      prefilterExpressions::IsDatatype::LITERAL>;
using isNumericExpression =
    IsDtypeExpression<IsNumericValueGetter,
                      prefilterExpressions::IsDatatype::NUMERIC>;
using isBlankExpression =
    IsDtypeExpression<IsValueIdValueGetter<Datatype::BlankNodeIndex>,
                      prefilterExpressions::IsDatatype::BLANK>;
using isIriExpression =
    IsDtypeExpression<IsIriValueGetter, prefilterExpressions::IsDatatype::IRI>;

// We currently don't support pre-filtering for `isGeoPointExpression`.
using isGeoPointExpression =
    NARY<1, FV<std::identity, IsValueIdValueGetter<Datatype::GeoPoint>>>;

//______________________________________________________________________________
// The expression for `bound` is slightly different as `IsValidValueGetter`
// returns a `bool` and not an `Id`.
inline auto boolToId = [](bool b) { return Id::makeFromBool(b); };
using boundExpression = NARY<1, FV<decltype(boolToId), IsValidValueGetter>>;

}  // namespace detail

SparqlExpression::Ptr makeIsIriExpression(SparqlExpression::Ptr arg) {
  return std::make_unique<detail::isIriExpression>(std::move(arg));
}
SparqlExpression::Ptr makeIsBlankExpression(SparqlExpression::Ptr arg) {
  return std::make_unique<detail::isBlankExpression>(std::move(arg));
}
SparqlExpression::Ptr makeIsLiteralExpression(SparqlExpression::Ptr arg) {
  return std::make_unique<detail::isLiteralExpression>(std::move(arg));
}
SparqlExpression::Ptr makeIsNumericExpression(SparqlExpression::Ptr arg) {
  return std::make_unique<detail::isNumericExpression>(std::move(arg));
}
SparqlExpression::Ptr makeIsGeoPointExpression(SparqlExpression::Ptr arg) {
  return std::make_unique<detail::isGeoPointExpression>(std::move(arg));
}
SparqlExpression::Ptr makeBoundExpression(SparqlExpression::Ptr arg) {
  return std::make_unique<detail::boundExpression>(std::move(arg));
}

}  // namespace sparqlExpression
