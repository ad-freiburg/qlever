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

// Datatypes supported by typed expression evaluation. Date denotes the full
// DateYearOrDuration representation, including dateTimes, durations and years.
// Other denotes a datatype without a typed fast path for the selected getters.
enum class NumericType { Int, Double, Date, Other };

// The homogeneous type is supported only if every value has that datatype.
// The majority type is supported only if it is the unique most common datatype
// in the operand, even when it occurs in fewer than half of the rows.
// The same pass also records whether generic fallback needs unsupported-ID
// checks.
struct NumericOperandClassification {
  NumericType homogeneousType_ = NumericType::Other;
  NumericType majorityType_ = NumericType::Other;
  // Whether fallback must check for getter-unsupported IDs. Booleans are
  // getter-supported; unknown/empty classifications retain the checks.
  bool hasUnsupported_ = true;
};

// Enable date classification/dispatch only when every getter accepts dates.
// Other getter combinations retain numeric dispatch and their generic fallback.
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

// Map an ID datatype to a supported typed value. Date is enabled only for
// date-aware getters; booleans retain the generic getter's numeric conversion.
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

// Classify a column in one pass, counting supported and competing datatypes
// together. Each small block is read once into datatype tags. Homogeneous
// blocks need one histogram update; two-type blocks aggregate both counts.
// This preserves efficient classification of numeric/date columns and common
// numeric/UNDEF mixtures. Other mixed blocks reuse the tags to count each type.
// IncludeDates enables Date alongside Int and Double. Empty input is untyped.
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
        std::array<Datatype, blockSize> types;
        for (size_t i = 0; i < blockSize; ++i) {
          types[i] = values[block * blockSize + i].getDatatype();
        }
        unsigned differences = 0;
        for (auto type : types) {
          differences |=
              static_cast<unsigned>(type) ^ static_cast<unsigned>(types[0]);
        }
        if (differences == 0) {
          counts[static_cast<size_t>(types[0])] += blockSize;
        } else {
          const auto first = static_cast<unsigned>(types[0]);
          // For exactly two types, the XOR differences recover the second.
          // It is only a candidate until both counts account for the block.
          const auto second = first ^ differences;
          size_t firstCount = 0;
          size_t secondCount = 0;
          for (auto type : types) {
            firstCount += static_cast<unsigned>(type) == first;
            secondCount += static_cast<unsigned>(type) == second;
          }
          if (firstCount + secondCount == blockSize) {
            counts[first] += firstCount;
            counts[second] += secondCount;
          } else {
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

  size_t supported = counts[static_cast<size_t>(Datatype::Int)] +
                     counts[static_cast<size_t>(Datatype::Double)] +
                     counts[static_cast<size_t>(Datatype::Bool)];
  if constexpr (IncludeDates) {
    supported += counts[static_cast<size_t>(Datatype::Date)];
  }
  const bool hasUnsupported = supported != values.size();
  const auto largest = std::max_element(counts.begin(), counts.end());
  const auto candidate = numericTypeForDatatype<IncludeDates>(
      static_cast<Datatype>(largest - counts.begin()));
  NumericOperandClassification result{NumericType::Other, NumericType::Other,
                                      hasUnsupported};
  if (candidate == NumericType::Other) {
    return result;
  }
  if (*largest == values.size()) {
    return {candidate, candidate, hasUnsupported};
  }
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
  const bool hasUnsupported =
      type == NumericType::Other && value.getDatatype() != Datatype::Bool;
  return {type, type, hasUnsupported};
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
