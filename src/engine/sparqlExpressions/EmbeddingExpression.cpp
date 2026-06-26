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

#include <vector>

#include "backports/algorithm.h"
#include "engine/sparqlExpressions/EmbeddingDistance.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionGenerators.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "index/EmbeddingTypeRegistry.h"
#include "util/Exception.h"
#include "util/HashMap.h"

namespace sparqlExpression {
namespace {
using namespace detail;

using OptVector = std::optional<std::vector<float>>;

// `computeDistance` (the metric policy) lives in `EmbeddingDistance.h`, and the
// raw `dotProduct`/`squaredL2` kernels in `EmbeddingDistanceKernels.h`.

// The maximum number of distinct query vectors the many-vs-(few) fast path will
// cache. A `Variable` column with at most this many distinct Ids is treated as
// the "query" side (its decodes are hoisted); more than this and it is the
// high-cardinality "candidate" side. `n = 1` is the dominant kNN case.
constexpr size_t kMaxDistinctQueries = 32;

// The distinct `ValueId`s of `ids`, or `nullopt` if there are more than `cap`
// of them. The high-cardinality candidate side therefore bails after ~`cap`
// rows; the linear membership test stays cheap because `distinct` never exceeds
// `cap`.
std::optional<std::vector<ValueId>> distinctCapped(ql::span<const ValueId> ids,
                                                   size_t cap) {
  std::vector<ValueId> distinct;
  for (ValueId id : ids) {
    if (ql::ranges::find(distinct, id) == distinct.end()) {
      distinct.push_back(id);
      if (distinct.size() > cap) {
        return std::nullopt;
      }
    }
  }
  return distinct;
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

      // Many-vs-(few) fast path. When both operands are `Variable` columns and
      // one of them has only a few distinct values, that side is the "query"
      // set and the other is the "candidate" set we stream. This covers the
      // dominant kNN shapes:
      //   * n = 1: a query bound by a single-subject triple
      //     (`<X> emb:hasEmbedding/emb:asFp32Vector ?q . … distance(?q, ?ev)`),
      //     whose `?q` column is constant-valued at runtime — invisible to the
      //     compile-time `resultIsConstant` check above.
      //   * 2 <= n <= kMaxDistinctQueries: a handful of queries crossed with
      //   the
      //     candidates (e.g. `VALUES ?q { … } . ?e emb:hasEmbedding/… ?ev`).
      // The generic per-row path would re-decode the query operand on every row
      // (a vocab lookup + parse). Instead we decode the few distinct query
      // vectors *once* and reuse them.
      //
      // A literal / constant-folded query operand is NOT a `Variable`, so it
      // does not reach here; it is already decoded once by the constant
      // `resultGenerator` in the generic path below.
      if constexpr (ad_utility::isSimilar<::Variable, A> &&
                    ad_utility::isSimilar<::Variable, B>) {
        ql::span<const ValueId> idsA = getIdsFromVariable(opA, context);
        ql::span<const ValueId> idsB = getIdsFromVariable(opB, context);
        AD_CORRECTNESS_CHECK(idsA.size() == targetSize &&
                             idsB.size() == targetSize);

        // Distinct Ids of each column, capped: `nullopt` == "more than
        // kMaxDistinctQueries distinct" (the high-cardinality candidate side,
        // detected after only ~kMaxDistinctQueries rows).
        auto distinctA = distinctCapped(idsA, kMaxDistinctQueries);
        auto distinctB = distinctCapped(idsB, kMaxDistinctQueries);

        EmbeddingValueGetter getter{};

        // Both columns single-valued: one distance, replicated for every row.
        if (distinctA && distinctA->size() == 1 && distinctB &&
            distinctB->size() == 1) {
          context->cancellationHandle_->throwIfCancelled();
          Id distance = computeDistance(config, getter(idsA.front(), context),
                                        getter(idsB.front(), context));
          VectorWithMemoryLimit<Id> result{context->_allocator};
          result.insert(result.end(), targetSize, distance);
          return result;
        }

        // Compute distances for the candidate column. `getQuery(row)` yields
        // the already-decoded query operand for that row; `queryIsFirst` keeps
        // the operand order of `computeDistance` (and the dimension-mismatch
        // message) identical to the textual argument order.
        auto computeColumn =
            [&config, context, &getter](
                ql::span<const ValueId> candidates, auto&& getQuery,
                bool queryIsFirst) -> VectorWithMemoryLimit<Id> {
          auto one = [&](size_t row) {
            const OptVector& query = getQuery(row);
            OptVector candidate = getter(candidates[row], context);
            return queryIsFirst ? computeDistance(config, query, candidate)
                                : computeDistance(config, candidate, query);
          };
          VectorWithMemoryLimit<Id> out{context->_allocator};
          size_t k = candidates.size();
          out.reserve(k);
          for (size_t i = 0; i < k; ++i) {
            context->cancellationHandle_->throwIfCancelled();
            out.push_back(one(i));
          }
          return out;
        };

        // Pick the low-cardinality "query" side (fewer distinct values; prefer
        // A on a tie). If neither side is low-cardinality, fall through to the
        // generic both-vary path.
        const bool aIsQuery =
            distinctA && (!distinctB || distinctA->size() <= distinctB->size());
        const bool bIsQuery = !aIsQuery && distinctB.has_value();
        if (aIsQuery || bIsQuery) {
          ql::span<const ValueId> queryIds = aIsQuery ? idsA : idsB;
          ql::span<const ValueId> candidateIds = aIsQuery ? idsB : idsA;
          const std::vector<ValueId>& distinct =
              aIsQuery ? *distinctA : *distinctB;
          if (distinct.size() == 1) {
            // n == 1: decode the single shared query once, no per-row lookup.
            OptVector query = getter(queryIds.front(), context);
            return computeColumn(
                candidateIds,
                [&query](size_t) -> const OptVector& { return query; },
                aIsQuery);
          }
          // 2 <= n <= kMaxDistinctQueries: decode the distinct query vectors
          // once into a small cache, then look each row's query up by its Id.
          ad_utility::HashMap<ValueId, OptVector> queryCache;
          queryCache.reserve(distinct.size());
          for (ValueId id : distinct) {
            queryCache.emplace(id, getter(id, context));
          }
          return computeColumn(
              candidateIds,
              [&queryCache, queryIds](size_t row) -> const OptVector& {
                return queryCache.at(queryIds[row]);
              },
              aIsQuery);
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
