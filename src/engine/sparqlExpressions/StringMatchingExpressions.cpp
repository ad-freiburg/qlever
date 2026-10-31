// Copyright 2026 The QLever Authors, in particular:
//
// 2023 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

// The `STRSTARTS`, `STRENDS`, and `CONTAINS` expressions, which check whether a
// string contains another string at a certain position. They are split from
// `StringExpressions.cpp` to reduce the memory needed for compiling and
// statically analyzing a single translation unit.

#include "backports/StartsWithAndEndsWith.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/StringExpressionsHelper.h"

namespace sparqlExpression {
namespace detail::string_expressions {

// STRSTARTS
struct StrStartsImpl {
  Id operator()(std::string_view text, std::string_view pattern) const {
    return Id::makeFromBool(ql::starts_with(text, pattern));
  }
};

namespace {

CPP_template(typename NaryOperation)(requires isOperation<NaryOperation>)
class StrStartsExpressionImpl : public NaryExpression<NaryOperation> {
 public:
  using NaryExpression<NaryOperation>::NaryExpression;
  std::vector<PrefilterExprVariablePair> getPrefilterExpressionForMetadata(
      [[maybe_unused]] const LocalVocabContext& context,
      [[maybe_unused]] bool isNegated) const override {
    std::vector<PrefilterExprVariablePair> prefilterVec;
    const auto& children = this->children();
    AD_CORRECTNESS_CHECK(children.size() == 2);

    auto var = children[0].get()->getVariableOrNullopt();
    if (!var.has_value()) {
      return prefilterVec;
    }
    auto prefixStr = getLiteralFromLiteralExpression(children[1].get());
    if (!prefixStr.has_value()) {
      return prefilterVec;
    }

    prefilterVec.emplace_back(
        std::make_unique<prefilterExpressions::PrefixRegexExpression>(
            prefixStr.value()),
        var.value());
    return prefilterVec;
  }
};

}  // namespace

using StrStartsExpression = StrStartsExpressionImpl<
    Operation<2, FV<LiftStringFunction<StrStartsImpl>, StringValueGetter>>>;

// STRENDS
struct StrEndsImpl {
  Id operator()(std::string_view text, std::string_view pattern) const {
    return Id::makeFromBool(ql::ends_with(text, pattern));
  }
};

using StrEndsExpression =
    StringExpressionImpl<2, LiftStringFunction<StrEndsImpl>, StringValueGetter>;

// STRCONTAINS
struct ContainsImpl {
  Id operator()(std::string_view text, std::string_view pattern) const {
    return Id::makeFromBool(text.find(pattern) != std::string::npos);
  }
};

using ContainsExpression =
    StringExpressionImpl<2, LiftStringFunction<ContainsImpl>,
                         StringValueGetter>;

}  // namespace detail::string_expressions
using namespace detail::string_expressions;
using Expr = SparqlExpression::Ptr;

Expr makeStrStartsExpression(Expr child1, Expr child2) {
  return make<StrStartsExpression>(child1, child2);
}

Expr makeStrEndsExpression(Expr child1, Expr child2) {
  return make<StrEndsExpression>(child1, child2);
}

Expr makeContainsExpression(Expr child1, Expr child2) {
  return make<ContainsExpression>(child1, child2);
}

}  // namespace sparqlExpression
