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

// These are plain functions (rather than `inline constexpr` lambdas) because
// they are used below as reference non-type template arguments
// (`UuidExpressionImpl`'s `FuncConv`/`FuncKey`). Whether an `inline
// constexpr` variable of (unique, per-translation-unit) closure type is
// correctly merged into a single address across translation units is subject
// to compiler bugs -- in particular, GCC 8 does not always merge such
// variables correctly, which causes two translation units that both
// instantiate `UuidExpressionImpl<fromLiteral, litUuidKey>` (say) to end up
// with two distinct, `dynamic_cast`-incompatible types. Plain functions do
// not have this problem, since a function's identity/address has always been
// well-defined and consistently merged across translation units.
inline LiteralOrIri fromLiteral(std::string_view str) {
  return LiteralOrIri{
      ad_utility::triple_component::Literal::literalWithNormalizedContent(
          asNormalizedStringViewUnsafe(str))};
}

inline LiteralOrIri fromIri(std::string_view str) {
  return LiteralOrIri{
      ad_utility::triple_component::Iri::fromStringRepresentation(
          absl::StrCat("<urn:uuid:"sv, str, ">"sv))};
}

inline std::string litUuidKey(int64_t randId) {
  return absl::StrCat("STRUUID "sv, randId);
}

inline std::string iriUuidKey(int64_t randId) {
  return absl::StrCat("UUID "sv, randId);
}

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
    VectorWithMemoryLimit<IdOrLocalVocabEntry> result{context->_allocator};
    const size_t numElements = context->_endIndex - context->_beginIndex;
    result.reserve(numElements);
    ad_utility::UuidGenerator uuidGen;

    // As part of a GROUP BY (and outside of an aggregate) we only return one
    // value per group. Inside an aggregate we must still produce one value per
    // row, so that e.g. `COUNT(STRUUID())` counts the rows in the group.
    if (worksOnAggregatedData(context)) {
      return LocalVocabEntry{FuncConv(uuidGen()),
                             context->getLocalVocabContext()};
    }

    ad_utility::chunkedForLoop<1000>(
        0, numElements,
        [&result, &uuidGen, context](size_t) {
          result.push_back(LocalVocabEntry{FuncConv(uuidGen()),
                                           context->getLocalVocabContext()});
        },
        [context]() { context->cancellationHandle_->throwIfCancelled(); });
    return result;
  }

  std::string getCacheKey(
      [[maybe_unused]] const VariableToColumnMap& varColMap) const override {
    return FuncKey(randId_);
  }

  // UUID() / STRUUID() generate a fresh UUID on every invocation.
  bool isDeterministic() const override { return false; }

  // The result of `UUID`/`STRUUID` is always a defined value.
  bool isResultAlwaysDefined(const VariableToColumnMap&) const override {
    return true;
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
