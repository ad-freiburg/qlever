// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Prashanth Premakumar <prashanthp0703@gmail.com>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_NUMERICOPERANDCLASSIFICATION_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_NUMERICOPERANDCLASSIFICATION_H

#include <algorithm>
#include <array>
#include <cstddef>
#include <optional>
#include <tuple>
#include <type_traits>

#include "backports/concepts.h"
#include "engine/sparqlExpressions/SparqlExpressionTypes.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "util/ChunkedForLoop.h"

namespace sparqlExpression::detail::homogeneousNumeric {

// Helpers for classifying numeric and optionally date operands by their
// homogeneous and majority datatypes in a single pass over each operand.

// `Int`, `Double`, and `Date` denote the corresponding `Datatype`. `Other`
// denotes any other datatype, which currently has no typed fast path.
enum class NumericType { Int, Double, Date, Other };

// Classification of a numeric or date operand. `homogeneousType_` is `Int`,
// `Double`, or `Date` only if every value has that datatype. Otherwise it is
// `Other`. `majorityType_` is `Int`, `Double`, or `Date` if that datatype is
// the unique most common datatype in the operand. Otherwise it is `Other`. It
// is used for speculative evaluation and need not occur in more than half the
// rows. `Date` is considered only when date classification is enabled.
struct NumericOperandClassification {
  NumericType homogeneousType_ = NumericType::Other;
  NumericType majorityType_ = NumericType::Other;
  // Whether any value has a datatype rejected by NumericValueGetter (anything
  // except Int, Double, Bool), or NumericOrDateValueGetter when date
  // classification is enabled (anything except Int, Double, Bool, Date).
  // Booleans require generic conversion but are accepted by both getters.
  // Date payloads, including invalid dates, remain subject to typed arithmetic.
  // For an unclassified or empty operand, true conservatively retains the
  // per-row checks that can return UNDEF without invoking the getters.
  bool containsValueRejectedByGetter_ = true;
};

// Check whether an expression supports date arguments. This is currently true
// if all ValueGetters, specified as a std::tuple, are NumericOrDateValueGetter.
template <typename ValueGetters>
inline constexpr bool supportsDateOperands = false;

template <typename... ValueGetters>
inline constexpr bool supportsDateOperands<std::tuple<ValueGetters...>> =
    (... && std::is_same_v<ValueGetters, NumericOrDateValueGetter>);

// Whether a value getter can participate in the homogeneous numeric fast path.
template <typename ValueGetter>
inline constexpr bool supportsNumericFastPath =
    ad_utility::SameAsAny<ValueGetter, NumericValueGetter,
                          NumericOrDateValueGetter>;

// Return whether `Operand` has a representation that can be inspected directly
// by the homogeneous numeric classifier. Currently this is restricted to
// `ValueId` constants and vectors whose elements are `ValueId`s.
template <typename Operand>
constexpr bool supportsNumericFastPathOperand() {
  using OperandType = std::decay_t<Operand>;

  if constexpr (ad_utility::isSimilar<OperandType, ValueId>) {
    return true;
  } else if constexpr (isVectorResult<OperandType>) {
    using ElementType = ql::ranges::range_value_t<OperandType>;
    return ad_utility::isSimilar<ElementType, ValueId>;
  } else {
    return false;
  }
}

// Map Datatype to NumericType. Int and Double are always recognized; Date is
// recognized only if IncludeDates is true. Return Other for all remaining
// datatypes, including Bool, which requires conversion by the generic getter.
template <bool IncludeDates = false>
constexpr NumericType numericTypeForDatatype(Datatype type) {
  switch (type) {
    case Datatype::Int:
      return NumericType::Int;
    case Datatype::Double:
      return NumericType::Double;
    case Datatype::Date:
      if constexpr (IncludeDates) {
        return NumericType::Date;
      }
      return NumericType::Other;
    default:
      return NumericType::Other;
  }
}

// Classify a column by determining whether all values have the same numeric
// or date datatype, and whether Int, Double, or Date is the unique most common
// datatype. Date is considered only if IncludeDates is true. Also record
// whether any value has a datatype rejected by the selected numeric getter
// (see containsValueRejectedByGetter_). Empty columns return the default
// classification, with neither a homogeneous nor a majority type.
//
// Count all datatypes in one pass over the column. Blocks aggregate repeated
// histogram updates; the individual steps and their motivation are below.
template <bool IncludeDates = false>
inline NumericOperandClassification classifyNumericOperand(
    ConstIdColumnRef values, const EvaluationContext* context) {
  if (values.empty()) {
    return {};
  }
  context->cancellationHandle_->throwIfCancelled();
  constexpr size_t numDatatypes = static_cast<size_t>(Datatype::MaxValue) + 1;
  std::array<size_t, numDatatypes> counts{};
  constexpr size_t blockSize = 32;
  ad_utility::chunkedForLoop<100'000 / blockSize>(
      0, values.size() / blockSize,
      [&](size_t block) {
        // Cache one block so later comparisons never reread the source IDs.
        std::array<Datatype, blockSize> types;
        for (size_t i = 0; i < blockSize; ++i) {
          types[i] = values[block * blockSize + i].getDatatype();
        }
        // The OR of all XORs with the first tag is zero exactly when all tags
        // are equal. This reduction lets the compiler vectorize the comparison;
        // std::all_of was about twice as slow on homogeneous columns in a
        // release classifier microbenchmark (100k IDs, five samples).
        unsigned differences = 0;
        for (auto type : types) {
          differences |=
              static_cast<unsigned>(type) ^ static_cast<unsigned>(types[0]);
        }
        const auto first = types.front();
        if (differences == 0) {
          // One histogram update replaces 32 dependent increments of the same
          // counter. A plain per-ID histogram was about seven times slower on
          // homogeneous columns in the same classifier-only benchmark.
          counts[static_cast<size_t>(first)] += blockSize;
        } else {
          // Try aggregating a block with exactly two datatypes. For such a
          // block, differences is first XOR second, so XOR with first recovers
          // second. With three or more types it is only a candidate and might
          // not even be a valid Datatype; do not index counts with it yet.
          // Compared with find_if plus separate std::count calls, this avoids
          // an additional search and counts both types in one reduction loop.
          const auto firstTag = static_cast<unsigned>(first);
          const auto secondCandidate = firstTag ^ differences;
          size_t firstCount = 0;
          size_t secondCount = 0;
          for (auto type : types) {
            firstCount += static_cast<unsigned>(type) == firstTag;
            secondCount += static_cast<unsigned>(type) == secondCandidate;
          }
          if (firstCount + secondCount == blockSize) {
            // Both types account for the entire mixed block. The candidate
            // therefore occurs in types and is a valid histogram index.
            // Two updates replace repeated writes to the same counters. In
            // classifier-only tests, this was about 2x faster than incrementing
            // the histogram per tag for shuffled double/UNDEF mixtures.
            counts[firstTag] += firstCount;
            counts[secondCandidate] += secondCount;
          } else {
            // Three or more datatypes: count each saved tag individually.
            for (auto type : types) {
              ++counts[static_cast<size_t>(type)];
            }
          }
        }
      },
      [context]() { context->cancellationHandle_->throwIfCancelled(); });
  // Count the final incomplete block without reading beyond the column.
  for (size_t i = values.size() / blockSize * blockSize; i < values.size();
       ++i) {
    ++counts[static_cast<size_t>(values[i].getDatatype())];
  }

  // Booleans have no typed fast path, but both numeric getters accept them.
  size_t numValuesAcceptedByGetter =
      counts[static_cast<size_t>(Datatype::Int)] +
      counts[static_cast<size_t>(Datatype::Double)] +
      counts[static_cast<size_t>(Datatype::Bool)];
  if constexpr (IncludeDates) {
    numValuesAcceptedByGetter += counts[static_cast<size_t>(Datatype::Date)];
  }
  const bool containsValueRejectedByGetter =
      numValuesAcceptedByGetter != values.size();
  // A typed majority must beat every datatype, including rejected ones.
  const auto largest = std::max_element(counts.begin(), counts.end());
  const auto candidate = numericTypeForDatatype<IncludeDates>(
      static_cast<Datatype>(largest - counts.begin()));
  NumericOperandClassification result{NumericType::Other, NumericType::Other,
                                      containsValueRejectedByGetter};
  if (candidate == NumericType::Other) {
    return result;
  }
  if (*largest == values.size()) {
    return {candidate, candidate, containsValueRejectedByGetter};
  }
  // Ties have no majority; a unique plurality below 50% is sufficient.
  if (std::count(counts.begin(), counts.end(), *largest) == 1) {
    result.majorityType_ = candidate;
  }
  return result;
}

// Classify a single ValueId and use its supported datatype as both the
// homogeneous and majority type. Date is supported only with IncludeDates.
template <bool IncludeDates = false>
inline NumericOperandClassification classifyNumericOperand(ValueId value) {
  const auto type = numericTypeForDatatype<IncludeDates>(value.getDatatype());
  const bool containsValueRejectedByGetter =
      type == NumericType::Other && value.getDatatype() != Datatype::Bool;
  return {type, type, containsValueRejectedByGetter};
}

// Classify a supported operand representation. Constants are classified
// directly, while vector-like operands are viewed as spans of `ValueId`.
template <bool IncludeDates = false, typename Operand>
inline NumericOperandClassification classifyNumericOperand(
    const Operand& operand, const EvaluationContext* context) {
  using OperandType = std::decay_t<Operand>;

  static_assert(
      supportsNumericFastPathOperand<Operand>(),
      "Unsupported operand representation for numeric classification");

  if constexpr (ad_utility::isSimilar<OperandType, ValueId>) {
    return classifyNumericOperand<IncludeDates>(operand);
  } else {
    return classifyNumericOperand<IncludeDates>(
        ConstIdColumnRef{operand.data(), operand.size()}, context);
  }
}

// Classify all operands and determine both their homogeneous and majority
// supported datatypes, including Date when IncludeDates is true.
template <bool IncludeDates = false, typename... Operands>
inline auto classifyNumericOperands(const EvaluationContext* context,
                                    const Operands&... operands) {
  return std::array<NumericOperandClassification, sizeof...(Operands)>{
      classifyNumericOperand<IncludeDates>(operands, context)...};
}

// Extract the supported type selected by projection from every operand.
// Return `std::nullopt` if any operand has type `Other`.
template <size_t N, typename Projection>
std::optional<std::array<NumericType, N>> getNumericTypes(
    const std::array<NumericOperandClassification, N>& classifications,
    Projection projection) {
  std::array<NumericType, N> types;

  for (size_t i = 0; i < N; ++i) {
    const auto type = projection(classifications[i]);
    if (type == NumericType::Other) {
      return std::nullopt;
    }
    types[i] = type;
  }

  return types;
}

// Return the homogeneous supported type of every operand if all operands are
// homogeneous. Otherwise return `std::nullopt`.
template <size_t N>
std::optional<std::array<NumericType, N>> getHomogeneousNumericTypes(
    const std::array<NumericOperandClassification, N>& classifications) {
  return getNumericTypes(classifications, [](const auto& classification) {
    return classification.homogeneousType_;
  });
}

// Return the majority supported type of every operand if all have one.
// Otherwise return `std::nullopt`.
template <size_t N>
std::optional<std::array<NumericType, N>> getMajorityNumericTypes(
    const std::array<NumericOperandClassification, N>& classifications) {
  return getNumericTypes(classifications, [](const auto& classification) {
    return classification.majorityType_;
  });
}

}  // namespace sparqlExpression::detail::homogeneousNumeric

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_NUMERICOPERANDCLASSIFICATION_H
