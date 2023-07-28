//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
namespace sparqlExpression {
namespace detail {
// String functions.
[[maybe_unused]] auto strImpl = [](std::optional<std::string> s) {
  return IdOrString{std::move(s)};
};
NARY_EXPRESSION(StrExpressionImpl, 1, FV<decltype(strImpl), StringValueGetter>);

class StrExpression : public StrExpressionImpl {
  using StrExpressionImpl::StrExpressionImpl;
  bool isStrExpression() const override { return true; }
};

template <size_t N, typename Function>
class StringExpressionImpl : public SparqlExpression {
 private:
  using WithStrImpl = NARY<N, FV<Function, StringValueGetter>>;
  using WithoutStrImpl = NARY<N, FV<Function, LiteralFromIdGetter>>;

  SparqlExpression::Ptr impl_;

 public:
  StringExpressionImpl(SparqlExpression::Ptr child) {
    AD_CORRECTNESS_CHECK(child != nullptr);
    if (child->isStrExpression()) {
      impl_ = std::make_unique<WithStrImpl>(
          std::move(std::move(*child).moveChildrenOut().at(0)));
    } else {
      impl_ = std::make_unique<WithoutStrImpl>(std::move(child));
    }
  }

  ExpressionResult evaluate(EvaluationContext* context) const override {
    return impl_->evaluate(context);
  }
  std::string getCacheKey(const VariableToColumnMap& varColMap) const override {
    return impl_->getCacheKey(varColMap);
  }

 private:
  std::span<SparqlExpression::Ptr> children() override {
    return impl_->children();
  }
};

// Compute string length.
inline auto strlen = [](std::optional<std::string> s) {
  if (!s.has_value()) {
    return Id::makeUndefined();
  }
  return Id::makeFromInt(s.value().size());
};
using StrlenExpression = StringExpressionImpl<1, decltype(strlen)>;

}  // namespace detail
using namespace detail;
SparqlExpression::Ptr makeStrExpression(SparqlExpression::Ptr child) {
  return std::make_unique<StrExpression>(std::move(child));
}
SparqlExpression::Ptr makeStrlenExpression(SparqlExpression::Ptr child) {
  return std::make_unique<StrlenExpression>(std::move(child));
}
}  // namespace sparqlExpression
