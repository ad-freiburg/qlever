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

// Template for an expression that works on string literals. The arguments are
// the same as those to `NaryExpression` with the difference that the value
// getter is deduced automatically. If the child of the expression is the
// `STR()` expression, then the `StringValueGetter` will be used (which also
// returns string values for IRIs, numeric literals, etc.), otherwise the
// `LiteralFromIdGetter` is used (which returns `std::nullopt` for these cases).
template <size_t N, typename Function>
class StringExpressionImpl : public SparqlExpression {
 private:
  using ExpressionWithStr = NARY<N, FV<Function, StringValueGetter>>;
  using ExpressionWithoutStr = NARY<N, FV<Function, LiteralFromIdGetter>>;

  SparqlExpression::Ptr impl_;

 public:
  explicit StringExpressionImpl(SparqlExpression::Ptr child) {
    AD_CORRECTNESS_CHECK(child != nullptr);
    if (child->isStrExpression()) {
      auto childrenOfStr = std::move(*child).moveChildrenOut();
      AD_CORRECTNESS_CHECK(childrenOfStr.size() == 1);
      impl_ =
          std::make_unique<ExpressionWithStr>(std::move(childrenOfStr.at(0)));
    } else {
      impl_ = std::make_unique<ExpressionWithoutStr>(std::move(child));
    }
  }

  ExpressionResult evaluate(EvaluationContext* context) const override {
    return impl_->evaluate(context);
  }
  std::string getCacheKey(const VariableToColumnMap& varColMap) const override {
    return impl_->getCacheKey(varColMap);
  }

 private:
  std::span<SparqlExpression::Ptr> childrenImpl() override {
    return impl_->children();
  }
};

// Compute string length.
inline auto strlen = [](std::optional<std::string> s) {
  if (!s.has_value()) {
    return Id::makeUndefined();
  }
  return Id::makeFromInt(static_cast<int64_t>(s.value().size()));
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
