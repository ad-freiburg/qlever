#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_BINARYEXPRESSION_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_BINARYEXPRESSION_H

#include <array>

#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "util/ChunkedForLoop.h"

namespace sparqlExpression::detail {

// Generic infrastructure for binary expressions. The function and value
// getter(s) are specified using `FunctionAndValueGetters`. A single value
// getter can be used for both operands, or separate getters can be specified.
//
// This is more efficient than the generic `NaryExpression` infrastructure for
// binary expressions because the operation is applied in direct loops over
// vector or constant operands, avoiding the generator-based per-element
// abstraction.

template <typename Function, typename LeftValueGetter,
          typename RightValueGetter, typename Left, typename Right>
ExpressionResult evaluateBinaryOperation(Left&& left, Right&& right,
                                         EvaluationContext* context);

enum class HomogeneousNumericType {
  Int,
  Double,
  Other,
};

struct HomogeneousNumericTypes {
  HomogeneousNumericType left;
  HomogeneousNumericType right;
};

template <typename Function, typename LeftValueGetter,
          typename RightValueGetter, typename Left, typename Right>
ExpressionResult evaluateBinaryOperationOnVectorOrConstant(
    Left&& left, Right&& right, EvaluationContext* context);

template <typename FunctionAndValueGettersT>
class BinaryExpression;

// Binary expression whose operation and value getters are known at compile
// time.
template <typename Function, typename... ValueGetters>
class BinaryExpression<FunctionAndValueGetters<Function, ValueGetters...>>
    : public NaryExpressionBase<2> {
 public:
  using Base = NaryExpressionBase<2>;
  using Children = typename Base::Children;
  using Getters = ValueGetterPack<2, std::tuple<ValueGetters...>>;
  using LeftValueGetter = std::tuple_element_t<0, Getters>;
  using RightValueGetter = std::tuple_element_t<1, Getters>;
  BinaryExpression(SparqlExpression::Ptr lhs, SparqlExpression::Ptr rhs)
      : Base{Children{std::move(lhs), std::move(rhs)}} {}

  ExpressionResult evaluate(EvaluationContext* context) const override {
    auto leftResult = this->children_[0]->evaluate(context);
    auto rightResult = this->children_[1]->evaluate(context);

    auto visitor = [context](auto&& left, auto&& right) -> ExpressionResult {
      return evaluateBinaryOperation<Function, LeftValueGetter,
                                     RightValueGetter>(AD_FWD(left),
                                                       AD_FWD(right), context);
    };

    return std::visit(visitor, std::move(leftResult), std::move(rightResult));
  }
};

// Convert an expression result into either a vector-like or constant
// representation that can be handled directly by the binary evaluation loop.
template <typename T>
decltype(auto) convertToVectorOrConstant(T&& value,
                                         EvaluationContext* context) {
  using Type = std::decay_t<T>;

  if constexpr (ad_utility::isSimilar<Type, ad_utility::SetOfIntervals>) {
    return ad_utility::SetOfIntervals::toIdVector(value, context->size(),
                                                  context->_allocator);
  } else if constexpr (ad_utility::isSimilar<Type, ::Variable>) {
    return getIdsFromVariable(value, context);
  } else {
    static_assert(isVectorResult<Type> || isConstantResult<Type>,
                  "BinaryExpression only supports vectors and constants after "
                  "conversion");
    return AD_FWD(value);
  }
}

// Convert both operands to the supported representations and evaluate the
// binary operation.
template <typename Function, typename LeftValueGetter,
          typename RightValueGetter, typename Left, typename Right>
ExpressionResult evaluateBinaryOperation(Left&& left, Right&& right,
                                         EvaluationContext* context) {
  decltype(auto) leftConverted =
      convertToVectorOrConstant(AD_FWD(left), context);
  decltype(auto) rightConverted =
      convertToVectorOrConstant(AD_FWD(right), context);

  return evaluateBinaryOperationOnVectorOrConstant<Function, LeftValueGetter,
                                                   RightValueGetter>(
      AD_FWD(leftConverted), AD_FWD(rightConverted), context);
}

// Return a callable that provides the converted value at index `i`. For
// constant operands, the converted value is computed only once.
template <typename ValueGetter, typename Operand>
auto makeIndexedValueGetter(Operand&& operand, EvaluationContext* context) {
  using OperandType = std::decay_t<Operand>;

  if constexpr (isVectorResult<OperandType>) {
    AD_CORRECTNESS_CHECK(operand.size() == context->size());

    // TODO: The generator-based `NaryExpression` infrastructure forwards/moves
    // individual values into the value getter. Here, indexed vector elements
    // are passed as lvalues. This is irrelevant for the currently used value
    // getters, but should be revisited for move-sensitive value types.
    return [&operand, context](size_t i) {
      return ValueGetter{}(operand[i], context);
    };
  } else {
    return [value = ValueGetter{}(AD_FWD(operand), context)](
               size_t) -> decltype(auto) { return (value); };
  }
}

// Specialized path for homogeneous numeric operands.
//
// Numeric value getters normally produce variants whose alternatives are
// dispatched for every row. For operands that consist entirely of `int64_t`
// or `double` values, classify the operand once and dispatch to a loop over the
// corresponding primitive types. Non-homogeneous operands use the generic
// value-getter-based implementation below.

template <typename ValueGetter>
inline constexpr bool supportsHomogeneousNumericFastPath = false;

template <>
inline constexpr bool supportsHomogeneousNumericFastPath<NumericValueGetter> =
    true;

template <>
inline constexpr bool
    supportsHomogeneousNumericFastPath<NumericOrDateValueGetter> = true;

template <typename Operand>
constexpr bool supportsHomogeneousNumericOperand() {
  using OperandType = std::decay_t<Operand>;

  if constexpr (ad_utility::isSimilar<OperandType, ValueId>) {
    return true;
  } else if constexpr (isVectorResult<OperandType>) {
    using ElementType =
        std::decay_t<decltype(std::declval<const OperandType&>()[size_t{}])>;

    return ad_utility::isSimilar<ElementType, ValueId>;
  } else {
    return false;
  }
}

inline HomogeneousNumericType classifyNumericOperand(
    ql::span<const ValueId> values, EvaluationContext* context) {
  // An empty vector has no meaningful homogeneous numeric type.
  if (values.empty()) {
    return HomogeneousNumericType::Other;
  }

  bool allInt = true;
  bool allDouble = true;

  ad_utility::chunkedForLoop<1000>(
      0, values.size(),
      [&](size_t i) {
        const auto type = values[i].getDatatype();

        allInt &= type == Datatype::Int;
        allDouble &= type == Datatype::Double;
      },
      [context]() { context->cancellationHandle_->throwIfCancelled(); });

  if (allInt) {
    return HomogeneousNumericType::Int;
  }

  if (allDouble) {
    return HomogeneousNumericType::Double;
  }

  return HomogeneousNumericType::Other;
}

inline HomogeneousNumericType classifyNumericOperand(ValueId value) {
  switch (value.getDatatype()) {
    case Datatype::Int:
      return HomogeneousNumericType::Int;
    case Datatype::Double:
      return HomogeneousNumericType::Double;
    default:
      return HomogeneousNumericType::Other;
  }
}

template <typename Operand>
inline HomogeneousNumericType classifyNumericOperand(
    const Operand& operand, EvaluationContext* context) {
  using OperandType = std::decay_t<Operand>;

  static_assert(supportsHomogeneousNumericOperand<Operand>(),
                "Unsupported operand representation for homogeneous numeric "
                "classification");

  if constexpr (ad_utility::isSimilar<OperandType, ValueId>) {
    return classifyNumericOperand(operand);
  } else {
    return classifyNumericOperand(
        ql::span<const ValueId>{operand.data(), operand.size()}, context);
  }
}

template <typename Left, typename Right>
inline HomogeneousNumericTypes classifyNumericOperands(
    const Left& left, const Right& right, EvaluationContext* context) {
  return {
      classifyNumericOperand(left, context),
      classifyNumericOperand(right, context),
  };
}

template <typename NumericType>
NumericType getHomogeneousNumericValue(ValueId value) {
  if constexpr (std::same_as<NumericType, int64_t>) {
    return value.getInt();
  } else if constexpr (std::same_as<NumericType, double>) {
    return value.getDouble();
  } else {
    static_assert(ad_utility::alwaysFalse<NumericType>,
                  "Unsupported homogeneous numeric type");
  }
}

template <typename NumericType, typename Operand>
auto makeHomogeneousNumericGetter(const Operand& operand) {
  using OperandType = std::decay_t<Operand>;

  if constexpr (isVectorResult<OperandType>) {
    return [&operand](size_t i) {
      return getHomogeneousNumericValue<NumericType>(operand[i]);
    };
  } else {
    static_assert(ad_utility::isSimilar<OperandType, ValueId>,
                  "Homogeneous numeric fast path currently supports "
                  "ValueId constants");

    const auto value = getHomogeneousNumericValue<NumericType>(operand);

    return [value](size_t) { return value; };
  }
}

template <typename Function>
struct RawNumericFunction {
  using type = Function;
};

template <typename Function, bool NanOrInfToUndef>
struct RawNumericFunction<MakeNumericExpression<Function, NanOrInfToUndef>> {
  using type = NumericIdWrapper<Function, NanOrInfToUndef>;
};

template <typename Function>
using RawNumericFunctionT = typename RawNumericFunction<Function>::type;

template <typename Function, typename LeftNumericType,
          typename RightNumericType, typename Left, typename Right>
ExpressionResult evaluateHomogeneousNumericOperation(
    const Left& left, const Right& right, EvaluationContext* context) {
  using LeftType = std::decay_t<Left>;
  using RightType = std::decay_t<Right>;

  if constexpr (isVectorResult<LeftType>) {
    AD_CORRECTNESS_CHECK(left.size() == context->size());
  }

  if constexpr (isVectorResult<RightType>) {
    AD_CORRECTNESS_CHECK(right.size() == context->size());
  }

  using FastFunction = RawNumericFunctionT<Function>;
  FastFunction function;

  auto getLeft = makeHomogeneousNumericGetter<LeftNumericType>(left);
  auto getRight = makeHomogeneousNumericGetter<RightNumericType>(right);

  VectorWithMemoryLimit<Id> result{context->_allocator};
  result.reserve(context->size());

  ad_utility::chunkedForLoop<1000>(
      0, context->size(),
      [&](size_t i) { result.push_back(function(getLeft(i), getRight(i))); },
      [context]() { context->cancellationHandle_->throwIfCancelled(); });

  return result;
}

// Evaluate a binary operation whose operands are already vectors or constants.
template <typename Function, typename LeftValueGetter,
          typename RightValueGetter, typename Left, typename Right>
ExpressionResult evaluateBinaryOperationOnVectorOrConstant(
    Left&& left, Right&& right, EvaluationContext* context) {
  using LeftType = std::decay_t<Left>;
  using RightType = std::decay_t<Right>;

  Function function;

  // Constant–constant operands don't benefit from upfront classification.
  if constexpr (isConstantResult<LeftType> && isConstantResult<RightType>) {
    context->cancellationHandle_->throwIfCancelled();
    return function(LeftValueGetter{}(AD_FWD(left), context),
                    RightValueGetter{}(AD_FWD(right), context));

  } else if constexpr ((isVectorResult<LeftType> ||
                        isConstantResult<LeftType>) &&
                       (isVectorResult<RightType> ||
                        isConstantResult<RightType>)) {
    if constexpr (supportsHomogeneousNumericFastPath<LeftValueGetter> &&
                  supportsHomogeneousNumericFastPath<RightValueGetter> &&
                  supportsHomogeneousNumericOperand<Left>() &&
                  supportsHomogeneousNumericOperand<Right>()) {
      const auto types = classifyNumericOperands(left, right, context);

      if (types.left == HomogeneousNumericType::Int &&
          types.right == HomogeneousNumericType::Int) {
        return evaluateHomogeneousNumericOperation<Function, int64_t, int64_t>(
            left, right, context);
      }

      if (types.left == HomogeneousNumericType::Int &&
          types.right == HomogeneousNumericType::Double) {
        return evaluateHomogeneousNumericOperation<Function, int64_t, double>(
            left, right, context);
      }

      if (types.left == HomogeneousNumericType::Double &&
          types.right == HomogeneousNumericType::Int) {
        return evaluateHomogeneousNumericOperation<Function, double, int64_t>(
            left, right, context);
      }

      if (types.left == HomogeneousNumericType::Double &&
          types.right == HomogeneousNumericType::Double) {
        return evaluateHomogeneousNumericOperation<Function, double, double>(
            left, right, context);
      }
    }
    auto getLeft =
        makeIndexedValueGetter<LeftValueGetter>(AD_FWD(left), context);
    auto getRight =
        makeIndexedValueGetter<RightValueGetter>(AD_FWD(right), context);

    VectorWithMemoryLimit<Id> result{context->_allocator};
    result.reserve(context->size());

    ad_utility::chunkedForLoop<1000>(
        0, context->size(),
        [&](size_t i) { result.push_back(function(getLeft(i), getRight(i))); },
        [context]() { context->cancellationHandle_->throwIfCancelled(); });

    return result;

  } else {
    static_assert(ad_utility::alwaysFalse<std::tuple<LeftType, RightType>>,
                  "Unhandled binary expression operand types");
  }
}

#ifdef _QLEVER_TYPE_ERASED_EXPRESSIONS

#define BINARY_EXPRESSION(Name, ...) NARY_EXPRESSION(Name, 2, __VA_ARGS__)

#else

#define BINARY_EXPRESSION(Name, ...)                  \
  class Name : public BinaryExpression<__VA_ARGS__> { \
    using Base = BinaryExpression<__VA_ARGS__>;       \
    using Base::Base;                                 \
  }

#endif

}  // namespace sparqlExpression::detail

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_BINARYEXPRESSION_H
