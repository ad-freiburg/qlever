// Copyright 2023 - 2026 The QLever Authors, in particular:
//
// 2023 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "global/ValueIdComparators.h"
#include "util/Algorithm.h"

namespace sparqlExpression {
namespace detail {

// If `child` is a variable by which the input of the `context` is sorted,
// return the set of rows where it is bound to an `Id` of one of the
// `datatypes` (or of none of them if `applyComplement` is set), otherwise
// return `std::nullopt`.
//
// NOTE 1: The `Id`s of each datatype are adjacent in the sorted input, so this
// only needs one binary search per datatype.
//
// NOTE 2: The result is also `std::nullopt` if the input contains UNDEF but
// `datatypes` doesn't, because then the result for UNDEF is UNDEF, which a
// `SetOfIntervals` can't represent.
static std::optional<ad_utility::SetOfIntervals>
evaluateDatatypesWithBinarySearch(const SparqlExpression& child,
                                  EvaluationContext* context,
                                  ql::span<const Datatype> datatypes,
                                  bool applyComplement) {
  auto variable = child.getVariableOrNullopt();
  if (!variable.has_value() ||
      context->_groupedVariables.contains(variable.value())) {
    return std::nullopt;
  }
  auto optColumn =
      ad_utility::contains(datatypes, Datatype::Undefined)
          ? context->getSortedColumnOrNullopt(variable.value())
          : context->getSortedAlwaysDefinedColumnOrNullopt(variable.value());
  if (!optColumn.has_value()) {
    return std::nullopt;
  }
  auto column = optColumn.value();
  ad_utility::SetOfIntervals::Vec intervals;
  for (Datatype datatype : datatypes) {
    auto [begin, end] = valueIdComparators::getRangeForDatatype(
        column.begin(), column.end(), datatype);
    if (begin != end) {
      intervals.emplace_back(begin - column.begin(), end - column.begin());
    }
  }
  auto result = ad_utility::SetOfIntervals::CheckSortedAndDisjointAndSimplify(
      {std::move(intervals), context->size()});
  if (applyComplement) {
    return ad_utility::SetOfIntervals::Complement{}(std::move(result));
  }
  return result;
}

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
CPP_class_template(typename NaryOperation,
                   prefilterExpressions::IsDatatype Datatype)(
    requires(isOperation<NaryOperation>)) class IsDatatypeExpressionImpl
    : public NaryExpression<NaryOperation> {
 public:
  using NaryExpression<NaryOperation>::NaryExpression;

  // Use the binary search if `Datatype` holds for exactly the `Id`s of its
  // datatypes, see `prefilterExpressions::isDatatypeComputableViaBinarySearch`.
  // The value getters must be consistent with this.
  ExpressionResult evaluate(EvaluationContext* context) const override {
    using namespace prefilterExpressions;
    if constexpr (isDatatypeComputableViaBinarySearch<Datatype>) {
      AD_CORRECTNESS_CHECK(this->children().size() == 1);
      auto result = evaluateDatatypesWithBinarySearch(
          *this->children()[0], context, getDatatypes<Datatype>(), false);
      if (result.has_value()) {
        return std::move(result.value());
      }
    }
    return NaryExpression<NaryOperation>::evaluate(context);
  }

  std::vector<PrefilterExprVariablePair> getPrefilterExpressionForMetadata(
      [[maybe_unused]] const LocalVocabContext& context,
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
// `isNumeric`, and the custom functions `ql:isEncodedIri` and `ql:isGeoPoint`.
// Note that the value getters already return the correct `Id`, hence
// `ql::identity`.
template <typename Getter, prefilterExpressions::IsDatatype Datatype>
using IsDtypeExpression =
    IsDatatypeExpressionImpl<Operation<1, FV<ql::identity, Getter>>, Datatype>;

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
using isEncodedIriExpression =
    IsDtypeExpression<IsValueIdValueGetter<Datatype::EncodedVal>,
                      prefilterExpressions::IsDatatype::ENCODED_IRI>;

using isGeoPointExpression =
    IsDtypeExpression<IsValueIdValueGetter<Datatype::GeoPoint>,
                      prefilterExpressions::IsDatatype::GEO_POINT>;

//______________________________________________________________________________
// The expression for `bound` is slightly different as `IsValidValueGetter`
// returns a `bool` and not an `Id`.
struct BoolToId {
  Id operator()(bool b) const { return Id::makeFromBool(b); }
};

using BoundExpressionBase = NARY<1, FV<BoolToId, IsValidValueGetter>>;

class BoundExpression : public BoundExpressionBase {
 public:
  using BoundExpressionBase::BoundExpressionBase;

  // On sorted inputs, `BOUND` is the complement of the (leading) UNDEF rows.
  ExpressionResult evaluate(EvaluationContext* context) const override {
    static constexpr std::array undefined{Datatype::Undefined};
    auto result = evaluateDatatypesWithBinarySearch(*this->children()[0],
                                                    context, undefined, true);
    if (result.has_value()) {
      return std::move(result.value());
    }
    return BoundExpressionBase::evaluate(context);
  }

  // The result of `BOUND` is always a valid bool.
  bool isResultAlwaysDefined(const VariableToColumnMap&) const override {
    return true;
  }
};

}  // namespace detail

SparqlExpression::Ptr makeIsIriExpression(SparqlExpression::Ptr arg) {
  return std::make_unique<detail::isIriExpression>(std::move(arg));
}
SparqlExpression::Ptr makeIsEncodedIriExpression(SparqlExpression::Ptr arg) {
  return std::make_unique<detail::isEncodedIriExpression>(std::move(arg));
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
  return std::make_unique<detail::BoundExpression>(std::move(arg));
}

}  // namespace sparqlExpression
