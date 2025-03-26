//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "engine/sparqlExpressions/BlankNodeExpression.h"

#include <absl/strings/str_cat.h>

#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "util/ChunkedForLoop.h"

namespace sparqlExpression {

namespace detail {

ad_utility::triple_component::Iri createBlankNodeForLabel(
    std::string_view label) {
  std::string iriString =
      absl::StrCat(QLEVER_INTERNAL_BLANK_NODE_IRI_PREFIX, label, ">");
  return ad_utility::triple_component::Iri::fromStringRepresentation(
      std::move(iriString));
}
[[maybe_unused]] auto stringToBlankNodeImpl =
    [](std::optional<std::string> s) -> IdOrLiteralOrIri {
  if (s.has_value()) {
    std::string label = BlankNode{false, std::move(s.value())}.toSparql();
    return LiteralOrIri{createBlankNodeForLabel(label)};
  } else {
    return Id::makeUndefined();
  }
};

using SpecificBlankNodeExpression =
    NARY<1, FV<decltype(stringToBlankNodeImpl), StringValueGetter>>;

class UniqueBlankNodeExpression : public SparqlExpression {
  size_t label_ = 0;
  mutable uint64_t cacheBreaker_ = 0;
  mutable uint64_t counter_ = 0;

 public:
  // _____________________________________________________________________________
  explicit UniqueBlankNodeExpression(size_t label) : label_{label} {}
  UniqueBlankNodeExpression(const UniqueBlankNodeExpression&) = delete;
  UniqueBlankNodeExpression& operator=(const UniqueBlankNodeExpression&) =
      delete;

  // _____________________________________________________________________________
  ExpressionResult evaluate(EvaluationContext* context) const override {
    std::string label{QLEVER_INTERNAL_BLANK_NODE_IRI_PREFIX};

    VectorWithMemoryLimit<IdOrLiteralOrIri> result{context->_allocator};
    const size_t numElements = context->_endIndex - context->_beginIndex;
    result.reserve(numElements);

    ad_utility::chunkedForLoop<1000>(
        0, numElements,
        [this, &result](size_t) {
          auto uniqueLabel = absl::StrCat("_:bn", label_, "_", counter_);
          counter_++;
          result.push_back(LiteralOrIri{createBlankNodeForLabel(uniqueLabel)});
        },
        [context]() { context->cancellationHandle_->throwIfCancelled(); });
    return result;
  }

  // _____________________________________________________________________________
  std::string getCacheKey(const VariableToColumnMap&) const override {
    return absl::StrCat("#UniqueBlankNode#", label_, "_", cacheBreaker_++);
  }

 private:
  std::span<Ptr> childrenImpl() override { return {}; }
};
}  // namespace detail

SparqlExpression::Ptr makeBlankNodeExpression(SparqlExpression::Ptr child) {
  return std::make_unique<detail::SpecificBlankNodeExpression>(
      std::move(child));
}
SparqlExpression::Ptr makeUniqueBlankNodeExpression(size_t label) {
  return std::make_unique<detail::UniqueBlankNodeExpression>(label);
}
}  // namespace sparqlExpression
