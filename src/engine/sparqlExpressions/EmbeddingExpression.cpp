// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Sebastian Walter <swalter@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/sparqlExpressions/EmbeddingExpression.h"

#include <absl/strings/str_cat.h>

#include <cmath>
#include <optional>
#include <stdexcept>
#include <vector>

#include "backports/algorithm.h"
#include "backports/span.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "global/Id.h"
#include "index/EmbeddingTypeRegistry.h"
#include "util/Exception.h"

namespace sparqlExpression {
namespace {
using namespace detail;

using OptVector = std::optional<std::vector<float>>;

// Raw distance kernels, free of the metric/sign conventions. `double`
// accumulation is used for numerical stability.
double dotProduct(ql::span<const float> a, ql::span<const float> b) {
  double sum = 0.0;
  for (size_t i = 0; i < a.size(); ++i) {
    sum += static_cast<double>(a[i]) * static_cast<double>(b[i]);
  }
  return sum;
}

double squaredL2(ql::span<const float> a, ql::span<const float> b) {
  double sum = 0.0;
  for (size_t i = 0; i < a.size(); ++i) {
    double diff = static_cast<double>(a[i]) - static_cast<double>(b[i]);
    sum += diff * diff;
  }
  return sum;
}

// Compute the distance between two operand vectors under the type's metric, or
// throw a hard query error if an operand is not an embedding vector or has the
// wrong dimension. This is the metric policy of the `embf:distance` expression;
// the negated-dot and `1 - cos` conventions keep `ORDER BY ASC` yielding the
// nearest neighbours.
Id computeDistance(const EmbeddingTypeConfig& config, const OptVector& optA,
                   const OptVector& optB) {
  if (!optA.has_value() || !optB.has_value()) {
    throw std::runtime_error{
        "embf:distance: an argument is not an embedding vector literal of the "
        "type's precision (emb:fp32Vector)"};
  }
  ql::span<const float> a{*optA};
  ql::span<const float> b{*optB};
  if (a.size() != config.dimension_ || b.size() != config.dimension_) {
    throw std::runtime_error{absl::StrCat(
        "embf:distance: a vector's dimension (", a.size(), " resp. ", b.size(),
        ") does not match the embedding type's emb:hasDimension (",
        config.dimension_, ")")};
  }
  double distance;
  switch (config.metric_) {
    case EmbeddingMetric::DotProduct:
      // Negated so that `ORDER BY ASC` still yields the nearest neighbours.
      distance = -dotProduct(a, b);
      break;
    case EmbeddingMetric::SquaredL2:
      distance = squaredL2(a, b);
      break;
    case EmbeddingMetric::L2:
      distance = std::sqrt(squaredL2(a, b));
      break;
    case EmbeddingMetric::Cosine: {
      double dot = dotProduct(a, b);
      double denom = std::sqrt(dotProduct(a, a)) * std::sqrt(dotProduct(b, b));
      if (denom == 0.0) {
        throw std::runtime_error{
            "embf:distance: cosine distance is undefined for a zero-norm "
            "vector"};
      }
      distance = 1.0 - dot / denom;
      break;
    }
  }
  return Id::makeFromDouble(distance);
}

// The `embf:distance` expression. Holds the two vector child expressions and
// the constant embedding-type IRI; the metric/dimension come from the type's
// config, resolved against the index's `EmbeddingTypeRegistry` at evaluation
// time.
class EmbeddingDistanceExpression : public SparqlExpression {
 private:
  std::array<Ptr, 2> children_;  // [0] = a, [1] = b
  std::string typeIri_;          // the embedding-type IRI (object of emb:type)

 public:
  EmbeddingDistanceExpression(Ptr a, Ptr b, std::string typeIri)
      : children_{std::move(a), std::move(b)}, typeIri_{std::move(typeIri)} {}

  // __________________________________________________________________________
  ExpressionResult evaluate(EvaluationContext* context) const override {
    // Resolve (and validate) the type once per evaluation; this surfaces the
    // unknown-type error at the start of evaluation, not per row.
    const EmbeddingTypeConfig& config = resolveType(context);

    ExpressionResult resultA = children_[0]->evaluate(context);
    ExpressionResult resultB = children_[1]->evaluate(context);

    auto computeOnOperands = [context, &config](
                                 auto&& opA, auto&& opB) -> ExpressionResult {
      using A = std::decay_t<decltype(opA)>;
      using B = std::decay_t<decltype(opB)>;
      size_t targetSize = getResultSize(*context, opA, opB);
      constexpr bool resultIsConstant =
          isConstantResult<A> && isConstantResult<B>;

      // kNN fast path: when both operands are `Variable` columns and one of
      // them is constant-valued at runtime (the dominant case: a single query
      // vector compared against many candidates), decode that shared vector
      // once instead of re-decoding it (a vocab lookup + parse) on every row.
      // The query side is invisible to the compile-time `resultIsConstant`
      // check above (it is a `Variable`, just single-valued at runtime). A
      // literal / constant-folded operand is not a `Variable` and never reaches
      // here; it is already decoded once by the generic path below.
      if constexpr (ad_utility::isSimilar<::Variable, A> &&
                    ad_utility::isSimilar<::Variable, B>) {
        ql::span<const ValueId> idsA = getIdsFromVariable(opA, context);
        ql::span<const ValueId> idsB = getIdsFromVariable(opB, context);
        AD_CORRECTNESS_CHECK(idsA.size() == targetSize &&
                             idsB.size() == targetSize);

        // The single shared `ValueId` of a column, or `nullopt` if it varies.
        auto constantId =
            [](ql::span<const ValueId> ids) -> std::optional<ValueId> {
          if (ids.empty()) {
            return std::nullopt;
          }
          ValueId front = ids.front();
          return ql::ranges::all_of(ids,
                                    [front](ValueId id) { return id == front; })
                     ? std::optional<ValueId>{front}
                     : std::nullopt;
        };
        std::optional<ValueId> constA = constantId(idsA);
        std::optional<ValueId> constB = constantId(idsB);

        if (constA || constB) {
          EmbeddingValueGetter getter{};
          OptVector decodedA = constA ? getter(*constA, context) : OptVector{};
          OptVector decodedB = constB ? getter(*constB, context) : OptVector{};

          // Both columns constant: one distance, replicated for every row.
          if (constA && constB) {
            context->cancellationHandle_->throwIfCancelled();
            Id distance = computeDistance(config, decodedA, decodedB);
            VectorWithMemoryLimit<Id> result{context->_allocator};
            result.insert(result.end(), targetSize, distance);
            return result;
          }

          // Exactly one side is constant (decoded once above); the other is
          // decoded per row. The textual operand order is preserved so the
          // dimension-mismatch message matches the argument order.
          VectorWithMemoryLimit<Id> out{context->_allocator};
          out.reserve(targetSize);
          for (size_t i = 0; i < targetSize; ++i) {
            context->cancellationHandle_->throwIfCancelled();
            out.push_back(constA ? computeDistance(config, decodedA,
                                                   getter(idsB[i], context))
                                 : computeDistance(config,
                                                   getter(idsA[i], context),
                                                   decodedB));
          }
          return out;
        }
      }

      auto genA = valueGetterGenerator(targetSize, context, AD_FWD(opA),
                                       EmbeddingValueGetter{});
      auto genB = valueGetterGenerator(targetSize, context, AD_FWD(opB),
                                       EmbeddingValueGetter{});
      auto distanceFn = [&config](OptVector a, OptVector b) -> Id {
        return computeDistance(config, a, b);
      };
      auto resultGenerator = applyFunction(distanceFn, targetSize,
                                           std::move(genA), std::move(genB));

      VectorWithMemoryLimit<Id> result{context->_allocator};
      result.reserve(targetSize);
      for (auto&& element : resultGenerator) {
        result.push_back(element);
      }
      if constexpr (resultIsConstant) {
        AD_CORRECTNESS_CHECK(result.size() == 1);
        return result[0];
      } else {
        return result;
      }
    };
    return ad_utility::visitWithVariantsAndParameters(
        computeOnOperands, std::move(resultA), std::move(resultB));
  }

  // __________________________________________________________________________
  std::string getCacheKey(const VariableToColumnMap& varColMap) const override {
    return absl::StrCat("EMBEDDING_DISTANCE type=", typeIri_,
                        " a=", children_[0]->getCacheKey(varColMap),
                        " b=", children_[1]->getCacheKey(varColMap));
  }

 private:
  // __________________________________________________________________________
  ql::span<Ptr> childrenImpl() override {
    return {children_.data(), children_.size()};
  }

  // Resolve the embedding type against the index's registry, throwing a hard
  // query error if it is not a declared embedding type.
  const EmbeddingTypeConfig& resolveType(EvaluationContext* context) const {
    const auto& index = context->_qec.getIndex();
    const auto& registry = index.getEmbeddingTypeRegistry();
    const EmbeddingTypeConfig* config = nullptr;
    VocabIndex idx;
    if (index.getVocab().getId(typeIri_, &idx)) {
      config = registry.getConfig(Id::makeFromVocabIndex(idx));
    }
    if (config == nullptr) {
      throw std::runtime_error{absl::StrCat(
          "embf:distance: ", typeIri_,
          " is not a declared embedding type (the object of an emb:type "
          "triple)")};
    }
    return *config;
  }
};
}  // namespace

// ____________________________________________________________________________
SparqlExpression::Ptr makeEmbeddingDistanceExpression(
    SparqlExpression::Ptr child1, SparqlExpression::Ptr child2,
    SparqlExpression::Ptr child3) {
  // child1/child2 = a/b, child3 = the embedding-type IRI (must be a constant
  // IRI).
  const auto* iriExpression = dynamic_cast<const IriExpression*>(child3.get());
  if (iriExpression == nullptr) {
    throw std::runtime_error{
        "embf:distance: the third argument (the embedding type) must be a "
        "constant IRI"};
  }
  std::string typeIri = iriExpression->value().toStringRepresentation();
  return std::make_unique<EmbeddingDistanceExpression>(
      std::move(child1), std::move(child2), std::move(typeIri));
}

}  // namespace sparqlExpression
