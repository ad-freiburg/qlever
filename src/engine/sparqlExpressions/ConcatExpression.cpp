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

// The `CONCAT` expression. It is split from `StringExpressions.cpp` to reduce
// the memory needed for compiling and statically analyzing a single
// translation unit.

#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/StringExpressionsHelper.h"
#include "engine/sparqlExpressions/VariadicExpression.h"

namespace sparqlExpression {
namespace detail::string_expressions {

// Initialize or append a Literal. If both literals are valid and initialized,
// concatenate nextLiteral into literalSoFar. If not initialized yet, set
// literalSoFar to nextLiteral. If either is UNDEF, set literalSoFar to nullopt
// to indicate an undefined result.
void concatOrSetLiteral(
    std::optional<ad_utility::triple_component::Literal>& literalSoFarOpt,
    const std::optional<ad_utility::triple_component::Literal>& nextLiteral,
    const bool isFirstLiteral) {
  if (!nextLiteral.has_value() || !literalSoFarOpt.has_value()) {
    literalSoFarOpt = std::nullopt;  // UNDEF
  } else if (isFirstLiteral) {
    literalSoFarOpt = nextLiteral.value();
  } else {
    literalSoFarOpt.value().concat(nextLiteral.value());
  }
}

// CONCAT
class ConcatExpression : public detail::VariadicExpression {
 public:
  using VariadicExpression::VariadicExpression;

  // _________________________________________________________________
  ExpressionResult evaluate(EvaluationContext* ctx) const override {
    using Literal = ad_utility::triple_component::Literal;
    using LiteralVec = VectorWithMemoryLimit<std::optional<Literal>>;
    // We evaluate one child after the other and append the strings from child i
    // to the strings already constructed for children 0, …, i - 1. The
    // seemingly more natural row-by-row approach has two problems. First, the
    // distinction between children with constant results and vector results
    // would not be cache-efficient. Second, when the evaluation is part of
    // GROUP BY, we don't have the information in advance whether all children
    // have constant results (in which case, we need to evaluate the whole
    // expression only once).

    // We store the (intermediate) result either as single literal or a vector.
    // If the result is a Literal, then all the previously evaluated children
    // were constants (see above).

    // `LiteralVec` stores literals whose string contents are the result of
    // concatenation. Each literal also carries a suffix (language tag or
    // datatype) that is determined by the previously processed children. For
    // each row, the suffix reflects either the current matching suffix or a
    // mismatch (in which case the final suffix will be empty).

    auto valueGetter =
        sparqlExpression::detail::LiteralValueGetterWithoutStrFunction{};
    std::variant<std::optional<Literal>, LiteralVec> result =
        Literal::literalWithNormalizedContent("");
    bool isFirstLiteral = true;

    auto moveLiteralToResult =
        [ctx](std::optional<Literal>& literal) -> IdOrLocalVocabEntry {
      if (!literal.has_value()) {
        return Id::makeUndefined();
      }
      return LocalVocabEntry{std::move(literal.value()),
                             ctx->getLocalVocabContext()};
    };

    auto visitSingleExpressionResult = CPP_template_lambda(
        &ctx, &result, &isFirstLiteral, &valueGetter)(typename T)(T && s)(
        requires SingleExpressionResult<T> && std::is_rvalue_reference_v<T&&>) {
      if constexpr (isConstantResult<T>) {
        auto literalFromConstant = valueGetter(std::forward<T>(s), ctx);

        auto concatOrSetLitFromConst =
            [&](std::optional<Literal>& literalSoFar) {
              concatOrSetLiteral(literalSoFar, literalFromConstant,
                                 isFirstLiteral);
            };

        // All previous children were constants, and the current child also is
        // a constant.
        auto visitLiteralConcat =
            [&concatOrSetLitFromConst](std::optional<Literal>&& literalSoFar) {
              concatOrSetLitFromConst(literalSoFar);
            };

        // One of the previous children was not a constant, so we already
        // store a vector.
        auto visitLiteralVecConcat = [&concatOrSetLitFromConst](
                                         LiteralVec&& literalVec) {
          ql::ranges::for_each(std::move(literalVec), [&](auto& literalSoFar) {
            concatOrSetLitFromConst(literalSoFar);
          });
        };

        std::visit(ad_utility::OverloadCallOperator{visitLiteralConcat,
                                                    visitLiteralVecConcat},
                   std::move(result));
      } else {
        auto gen = sparqlExpression::detail::makeGenerator(AD_FWD(s),
                                                           ctx->size(), ctx);

        if (std::holds_alternative<std::optional<Literal>>(result)) {
          // All previous children were constants, but now we have a
          // non-constant child, so we have to expand the `result` from a single
          // string to a vector.
          std::optional<Literal> constantResultSoFar =
              std::move(std::get<std::optional<Literal>>(result));
          result.emplace<LiteralVec>(ctx->_allocator);
          auto& resultAsVec = std::get<LiteralVec>(result);
          resultAsVec.reserve(ctx->size());
          std::fill_n(std::back_inserter(resultAsVec), ctx->size(),
                      constantResultSoFar);
        }

        // The `result` already is a vector, and the current child also returns
        // multiple results, so we do the `natural` way.
        auto& resultAsVec = std::get<LiteralVec>(result);
        // TODO<C++23> Use `ql::views::zip` or `enumerate`.
        size_t i = 0;
        for (auto& el : gen) {
          auto literal = valueGetter(std::move(el), ctx);
          concatOrSetLiteral(resultAsVec[i], literal, isFirstLiteral);
          ctx->cancellationHandle_->throwIfCancelled();
          ++i;
        }
      }
      ctx->cancellationHandle_->throwIfCancelled();
    };
    ql::ranges::for_each(childrenVec(), [&ctx, &visitSingleExpressionResult,
                                         &isFirstLiteral](const auto& child) {
      std::visit(visitSingleExpressionResult, child->evaluate(ctx));
      isFirstLiteral = false;
    });

    // Lift the result from `string` to `IdOrLocalVocabEntry` which is needed
    // for the expression module.

    auto visitLiteralResult =
        [&moveLiteralToResult](
            std::optional<Literal>& literalSoFar) -> ExpressionResult {
      return moveLiteralToResult(literalSoFar);
    };

    auto visitLiteralVecResult =
        [&ctx,
         &moveLiteralToResult](LiteralVec& literalVec) -> ExpressionResult {
      VectorWithMemoryLimit<IdOrLocalVocabEntry> resultAsVec(ctx->_allocator);
      resultAsVec.reserve(literalVec.size());
      ql::ranges::copy(literalVec | ql::views::transform(moveLiteralToResult),
                       std::back_inserter(resultAsVec));
      return resultAsVec;
    };
    return std::visit(ad_utility::OverloadCallOperator{visitLiteralResult,
                                                       visitLiteralVecResult},
                      result);
  }
};

}  // namespace detail::string_expressions
using namespace detail::string_expressions;
using Expr = SparqlExpression::Ptr;

Expr makeConcatExpression(std::vector<Expr> children) {
  return std::make_unique<ConcatExpression>(std::move(children));
}

}  // namespace sparqlExpression
