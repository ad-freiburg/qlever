//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

// Test for UuidExpressionImpl can be found in RandomExpressionTest.cpp

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_UUIDEXPRESSIONS_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_UUIDEXPRESSIONS_H

#include "engine/sparqlExpressions/SparqlExpression.h"
#include "util/ChunkedForLoop.h"
#include "util/Random.h"

namespace sparqlExpression {
namespace detail::uuidExpression {

using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;

inline constexpr auto fromLiteral = [](std::string_view str) {
  return LiteralOrIri{
      ad_utility::triple_component::Literal::literalWithNormalizedContent(
          asNormalizedStringViewUnsafe(str))};
};

inline constexpr auto fromIri = [](std::string_view str) {
  return LiteralOrIri{
      ad_utility::triple_component::Iri::fromStringRepresentation(
          absl::StrCat("<urn:uuid:"sv, str, ">"sv))};
};

inline constexpr auto litUuidKey = [](int64_t randId) {
  return absl::StrCat("STRUUID "sv, randId);
};

inline constexpr auto iriUuidKey = [](int64_t randId) {
  return absl::StrCat("UUID "sv, randId);
};

// With UuidExpressionImpl<fromIri, iriUuidKey>, the UUIDs are returned as an
// Iri object: <urn:uuid:b9302fb5-642e-4d3b-af19-29a8f6d894c9> (example). With
// UuidExpressionImpl<fromLiteral,, litUuidKey>, the UUIDs are returned as an
// Literal object: "73cd4307-8a99-4691-a608-b5bda64fb6c1" (example).
template <const auto& FuncConv, const auto& FuncKey>
class UuidExpressionImpl : public SparqlExpression {
 private:
  int64_t randId_ = ad_utility::FastRandomIntGenerator<int64_t>{}();

 public:
  ExpressionResult evaluate(EvaluationContext* context) const override {
    VectorWithMemoryLimit<IdOrLiteralOrIri> result{context->_allocator};
    const size_t numElements = context->_endIndex - context->_beginIndex;
    result.reserve(numElements);
    ad_utility::UuidGenerator uuidGen;

    if (context->_isPartOfGroupBy) {
      return FuncConv(uuidGen());
    }

    ad_utility::chunkedForLoop<1000>(
        0, numElements,
        [&result, &uuidGen](size_t) { result.push_back(FuncConv(uuidGen())); },
        [context]() { context->cancellationHandle_->throwIfCancelled(); });
    return result;
  }

  std::string getCacheKey(
      [[maybe_unused]] const VariableToColumnMap& varColMap) const override {
    return FuncKey(randId_);
  }

 private:
  ql::span<SparqlExpression::Ptr> childrenImpl() override { return {}; }
};

using UuidExpression = UuidExpressionImpl<fromIri, iriUuidKey>;
using StrUuidExpression = UuidExpressionImpl<fromLiteral, litUuidKey>;
}  //  namespace detail::uuidExpression

using UuidExpression = detail::uuidExpression::UuidExpression;
using StrUuidExpression = detail::uuidExpression::StrUuidExpression;

}  // namespace sparqlExpression

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_UUIDEXPRESSIONS_H
