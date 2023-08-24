// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "NaryExpression.h"
#include "NaryExpressionImpl.h"

namespace sparqlExpression {
namespace detail::conditional_expressions {
using namespace sparqlExpression::detail;
[[maybe_unused]] auto ifImpl = [](EffectiveBooleanValueGetter::Result condition,
                                  auto&& i, auto&& e) -> IdOrString {
  if (condition == EffectiveBooleanValueGetter::Result::True) {
    return AD_FWD(i);
  } else {
    return AD_FWD(e);
  }
};
NARY_EXPRESSION(IfExpression, 3,
                FV<decltype(ifImpl), EffectiveBooleanValueGetter,
                   ActualValueGetter, ActualValueGetter>);
}  // namespace detail::conditional_expressions

// ____________________________________________________
class CoalesceExpression : public SparqlExpression {
  std::vector<SparqlExpression::Ptr> children_;

 public:
  CoalesceExpression(std::vector<SparqlExpression::Ptr> children)
      : children_{std::move(children)} {};
  ExpressionResult evaluate(EvaluationContext* context) const override {
    std::vector<ExpressionResult> childResults;
    std::ranges::for_each(children_, [&](const auto& child) {
      childResults.push_back(child->evaluate(context));
    });
    std::vector<uint64_t> unboundResults;
    std::vector<uint64_t> nextUnboundResults;
    unboundResults.reserve(context->size());
    nextUnboundResults.reserve(context->size());
    for (size_t i = 0; i < unboundResults.size(); ++i) {
      unboundResults.push_back(i);
    }
    VectorWithMemoryLimit<IdOrString> results;
    std::fill_n(std::back_inserter(results), context->size(),
                IdOrString{Id::makeUndefined()});
    // TODO<joka921> implement this

    auto visitExpressionResult = [&](SingleExpressionResult auto&& res) {
      auto gen =
          detail::makeGenerator(std::move(res), context->size(), context);
      size_t i = 0;
      auto nextUnbound = unboundResults.begin();
      if (nextUnbound == unboundResults.end()) {
        return;
      }

      for (auto& el : gen) {
        if (i == *nextUnbound) {
          IdOrString val{std::move(el)};
          if (std::holds_alternative<Id>(val) &&
              std::get<Id>(val) == Id::makeUndefined()) {
            nextUnboundResults.push_back(i);
          } else {
            results.at(*nextUnbound) = std::move(val);
          }
          ++nextUnbound;
          if (nextUnbound == unboundResults.end()) {
            return;
          }
        }
        ++i;
      }
    };

    std::ranges::for_each(std::move(childResults),
                          [&](ExpressionResult& child) {
                            std::visit(visitExpressionResult, std::move(child));
                            unboundResults = std::move(nextUnboundResults);
                          });
    return results;
  }

  std::string getCacheKey(const VariableToColumnMap& varColMap) const override {
    auto childKeys = ad_utility::lazyStrJoin(
        children_ | std::views::transform([&varColMap](const auto& childPtr) {
          return childPtr->getCacheKey(varColMap);
        }));
    return absl::StrCat("COALESCE(", childKeys, ")");
  }

 private:
  std::span<SparqlExpression::Ptr> childrenImpl() override {
    return {children_.data(), children.size()};
  }
};

using namespace detail::conditional_expressions;
SparqlExpression::Ptr makeIfExpression(SparqlExpression::Ptr child1,
                                       SparqlExpression::Ptr child2,
                                       SparqlExpression::Ptr child3) {
  return std::make_unique<IfExpression>(std::move(child1), std::move(child2),
                                        std::move(child3));
}

SparqlExpression::Ptr makeCoalesceExpression(
    std::vector<SparqlExpression::Ptr> children) {
  return std::make_unique<CoalesceExpression>(std::move(children));
}

}  // namespace sparqlExpression
