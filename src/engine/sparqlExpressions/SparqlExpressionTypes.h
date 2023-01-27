//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

// Several helper types needed for the SparqlExpression module

#ifndef QLEVER_SPARQLEXPRESSIONTYPES_H
#define QLEVER_SPARQLEXPRESSIONTYPES_H

#include "engine/QueryExecutionContext.h"
#include "engine/ResultTable.h"
#include "engine/sparqlExpressions/SetOfIntervals.h"
#include "global/Id.h"
#include "parser/data/Variable.h"
#include "util/AllocatorWithLimit.h"
#include "util/ConstexprSmallString.h"
#include "util/Generator.h"
#include "util/HashMap.h"
#include "util/TypeTraits.h"

namespace sparqlExpression {

/// A std::vector<T, AllocatorWithLimit> with deleted copy constructor
/// and copy assignment. Used in the SparqlExpression module, where we want
/// no accidental copies of large intermediate results.
template <typename T>
class VectorWithMemoryLimit
    : public std::vector<T, ad_utility::AllocatorWithLimit<T>> {
 public:
  using Base = std::vector<T, ad_utility::AllocatorWithLimit<T>>;
  using Base::Base;
  // Disable copy constructor and copy assignment operator (copying is too
  // expensive in the setting where we want to use this class and not
  // necessary).
  // The copy constructor is not deleted, but private, because it is used
  // for the explicit clone() function.
 private:
  VectorWithMemoryLimit(const VectorWithMemoryLimit&) = default;

 public:
  VectorWithMemoryLimit& operator=(const VectorWithMemoryLimit&) = delete;

  // Moving is fine.
  VectorWithMemoryLimit(VectorWithMemoryLimit&&) noexcept = default;
  VectorWithMemoryLimit& operator=(VectorWithMemoryLimit&&) noexcept = default;

  // Allow copying via an explicit clone() function.
  [[nodiscard]] VectorWithMemoryLimit clone() const {
    // Call the private copy constructor.
    return VectorWithMemoryLimit(*this);
  }
};

/// A simple wrapper around a bool that prevents the strangely-behaving
/// std::vector<bool> optimization.
struct Bool {
  bool _value;
  // Implicit conversion from and to bool.
  Bool(bool value) : _value{value} {}

  operator bool() const { return _value; }

  // Default construction yields undefined value.
  Bool() = default;

  bool operator==(const Bool& b) const = default;

  // Make the type hashable for absl, see
  // https://abseil.io/docs/cpp/guides/hash.
  template <typename H>
  friend H AbslHashValue(H h, const Bool& b) {
    return H::combine(std::move(h), b._value);
  }
};

}  // namespace sparqlExpression

// Specializations of std::common_type for the Bool type.
namespace std {
template <typename T>
struct common_type<T, sparqlExpression::Bool> {
  using type = std::common_type_t<T, bool>;
};

template <typename T>
struct common_type<sparqlExpression::Bool, T> {
  using type = std::common_type_t<T, bool>;
};
}  // namespace std

namespace sparqlExpression {

/// A list of StrongIds that all have the same datatype.
using StrongIdsWithResultType = VectorWithMemoryLimit<ValueId>;

/// Typedef for a map from variables names to (input column, type of input
/// column.
using VariableToColumnAndResultTypeMap =
    ad_utility::HashMap<Variable, std::pair<size_t, ResultTable::ResultType>>;

/// All the additional information which is needed to evaluate a Sparql
/// expression.
struct EvaluationContext {
  const QueryExecutionContext& _qec;
  // The VariableToColumnMap of the input
  const VariableToColumnAndResultTypeMap& _variableToColumnAndResultTypeMap;

  /// The input of the expression.
  const IdTable& _inputTable;

  /// The indices of the actual range of rows in the _inputTable on which the
  /// expression is evaluated. For BIND expressions this is always [0,
  /// _inputTable.size()) but for GROUP BY evaluation we also need only parts
  /// of the input.
  size_t _beginIndex = 0;
  size_t _endIndex = _inputTable.size();

  /// The input is sorted on these columns. This information can be used to
  /// perform efficient relational operations like `equal` or `less than`
  std::vector<size_t> _columnsByWhichResultIsSorted;

  /// Let the expression evaluation also respect the memory limit.
  ad_utility::AllocatorWithLimit<Id> _allocator;

  /// The local vocabulary of the input.
  const LocalVocab& _localVocab;

  /// Constructor for evaluating an expression on the complete input.
  EvaluationContext(
      const QueryExecutionContext& qec,
      const VariableToColumnAndResultTypeMap& variableToColumnAndResultTypeMap,
      const IdTable& inputTable,
      const ad_utility::AllocatorWithLimit<Id>& allocator,
      const LocalVocab& localVocab)
      : _qec{qec},
        _variableToColumnAndResultTypeMap{variableToColumnAndResultTypeMap},
        _inputTable{inputTable},
        _allocator{allocator},
        _localVocab{localVocab} {}

  /// Constructor for evaluating an expression on a part of the input
  /// (only considers the rows [beginIndex, endIndex) from the input.
  EvaluationContext(const QueryExecutionContext& qec,
                    const VariableToColumnAndResultTypeMap& map,
                    const IdTable& inputTable, size_t beginIndex,
                    size_t endIndex,
                    const ad_utility::AllocatorWithLimit<Id>& allocator,
                    const LocalVocab& localVocab)
      : _qec{qec},
        _variableToColumnAndResultTypeMap{map},
        _inputTable{inputTable},
        _beginIndex{beginIndex},
        _endIndex{endIndex},
        _allocator{allocator},
        _localVocab{localVocab} {}

  bool isResultSortedBy(const Variable& variable) {
    if (_columnsByWhichResultIsSorted.empty()) {
      return false;
    }
    if (!_variableToColumnAndResultTypeMap.contains(variable)) {
      return false;
    }

    return getColumnIndexForVariable(variable) ==
           _columnsByWhichResultIsSorted[0];
  }
  // The size (in number of elements) that this evaluation context refers to.
  [[nodiscard]] size_t size() const { return _endIndex - _beginIndex; }

  // ____________________________________________________________________________
  [[nodiscard]] size_t getColumnIndexForVariable(const Variable& var) const {
    const auto& map = _variableToColumnAndResultTypeMap;
    if (!map.contains(var)) {
      throw std::runtime_error(absl::StrCat(
          "Variable ", var.name(), " was not found in input to expression."));
    }
    return map.at(var).first;
  }
};

/// The result of an expression can either be a vector of bool/double/int/string
/// a variable (e.g. in BIND (?x as ?y)) or a "Set" of indices, which identifies
/// the row indices in which a boolean expression evaluates to "true". Constant
/// results are represented by a vector with only one element.
namespace detail {
// For each type T in this tuple, T as well as VectorWithMemoryLimit<T> are
// possible expression result types.
using ConstantTypes = std::tuple<double, int64_t, Bool, string, ValueId>;
using ConstantTypesAsVector =
    ad_utility::LiftedTuple<ConstantTypes, VectorWithMemoryLimit>;

// Each type in this tuple also is a possible expression result type.
using OtherTypes = std::tuple<ad_utility::SetOfIntervals, ::Variable>;

using AllTypesAsTuple =
    ad_utility::TupleCat<ConstantTypes, ConstantTypesAsVector, OtherTypes>;
}  // namespace detail

/// An Expression result is a std::variant of all the different types from
/// the expressionResultDetail namespace (see above).
using ExpressionResult = ad_utility::TupleToVariant<detail::AllTypesAsTuple>;

/// Only the different types contained in the variant `ExpressionResult` (above)
/// match this concept.
template <typename T>
concept SingleExpressionResult =
    ad_utility::isTypeContainedIn<T, ExpressionResult>;

/// True iff T represents a constant.
template <typename T>
constexpr static bool isConstantResult =
    ad_utility::isTypeContainedIn<T, detail::ConstantTypes>;

/// True iff T is one of the ConstantTypesAsVector
template <typename T>
constexpr static bool isVectorResult =
    ad_utility::isTypeContainedIn<T, detail::ConstantTypesAsVector>;

namespace detail {

/// Convert expression result type T to corresponding qlever ResultType.
/// TODO<joka921>: currently all constants are floats.
template <SingleExpressionResult T>
constexpr static qlever::ResultType expressionResultTypeToQleverResultType() {
  if constexpr (ad_utility::isSimilar<T, string> ||
                ad_utility::isSimilar<T, VectorWithMemoryLimit<string>>) {
    return qlever::ResultType::LOCAL_VOCAB;
  } else if constexpr (isConstantResult<T> || isVectorResult<T>) {
    return qlever::ResultType::FLOAT;
  } else if constexpr (std::is_same_v<T, ad_utility::SetOfIntervals>) {
    return qlever::ResultType::VERBATIM;
  } else {
    // for Variables and StrongIdWithDatatypes we cannot get the result type at
    // compile time.
    static_assert(ad_utility::alwaysFalse<T>);
  }
}

/// Get Id of constant result of type T.
template <SingleExpressionResult T, typename LocalVocabT>
Id constantExpressionResultToId(T&& result, LocalVocabT& localVocab) {
  static_assert(isConstantResult<T>);
  if constexpr (ad_utility::isSimilar<T, string>) {
    return Id::makeFromLocalVocabIndex(
        localVocab.getIndexAndAddIfNotContained(std::forward<T>(result)));
  } else if constexpr (ad_utility::isSimilar<double, T>) {
    return Id::makeFromDouble(result);
  } else if constexpr (ad_utility::isSimilar<T, Id>) {
    return result;

  } else {
    static_assert(ad_utility::isSimilar<int64_t, T> ||
                  ad_utility::isSimilar<Bool, T>);
    // This currently covers int and bool.
    // TODO<joka921> represent bool in the `ValueId` class and adapt this.
    return Id::makeFromInt(result);
  }
}

/// A Tag type that has to be used as the `CalculationWithSetOfIntervals`
/// template parameter in `NaryExpression.h` and `AggregateExpression.h` when
/// no such calculation is possible
struct NoCalculationWithSetOfIntervals {};

/// A `Function` and one or more `ValueGetters`, that are applied to the
/// operands of the function before passing them. The number of `ValueGetters`
/// must either be 1 (the same `ValueGetter` is used for all the operands to the
/// `Function`, or it must be equal to the number of operands to the `Function`.
/// This invariant is checked in the `Operation` class template below,
/// which uses this helper struct.
template <typename FunctionType, typename... ValueGettersTypes>
struct FunctionAndValueGetters {
  using Function = FunctionType;
  using ValueGetters = std::tuple<ValueGettersTypes...>;
};

/// A `Function` that only works on certain input types together with a compile
/// time check, whether a certain set of inputs fulfills these requirements.
template <typename FunctionT, typename CheckT>
struct SpecializedFunction {
  using Function = FunctionT;

  // Check if the function can be applied to arguments of type(s) `Operands`
  template <typename... Operands>
  static bool areAllOperandsValid(const Operands&... operands) {
    return CheckT{}.template operator()<Operands...>(operands...);
  }

  // Evaluate the function on the `operands`. Return std::nullopt if the
  // function cannot be evaluated on the `operands`
  template <typename... Operands>
  std::optional<ExpressionResult> evaluateIfOperandsAreValid(
      Operands&&... operands) {
    if (!areAllOperandsValid<Operands...>(operands...)) {
      return std::nullopt;
    } else {
      if constexpr (requires {
                      Function{}(std::forward<Operands>(operands)...);
                    }) {
        return Function{}(std::forward<Operands>(operands)...);
      } else {
        AD_FAIL();
      }
    }
  }
};

/// Return true iff there exists a `SpecializedFunction` in the
/// `SpecializedFunctionsTuple` that can be evaluated on all the `Operands`
template <typename SpecializedFunctionsTuple, typename... Operands>
constexpr bool isAnySpecializedFunctionPossible(SpecializedFunctionsTuple&& tup,
                                                const Operands&... operands) {
  auto onPack = [&](auto&&... fs) constexpr {
    return (... || fs.template areAllOperandsValid(operands...));
  };

  return std::apply(onPack, tup);
}

/// Evaluate the SpecializedFunction, that matches the input. If no such
/// function exists, return `std::nullopt`.
template <typename SpecializedFunctionsTuple, typename... Operands>
std::optional<ExpressionResult> evaluateOnSpecializedFunctionsIfPossible(
    SpecializedFunctionsTuple&& tup, Operands&&... operands) {
  std::optional<ExpressionResult> result = std::nullopt;

  auto writeToResult = [&](auto f) {
    if (!result) {
      result =
          f.evaluateIfOperandsAreValid(std::forward<Operands>(operands)...);
    }
  };

  auto onSingle = [&](auto&&... els) { (..., writeToResult(els)); };
  std::apply(onSingle, tup);

  return result;
}

/// An Operation that consists of a `FunctionAndValueGetters` that takes
/// `NumOperands` parameters. The `FunctionForSetOfIntervalsType` is a function,
/// that can efficiently perform the operation when all the operands are
/// `SetOfInterval`s.
/// It is necessary to use the `FunctionAndValueGetters` struct to allow for
/// multiple `ValueGetters` (a parameter pack, that has to appear at the end of
/// the template declaration) and the default parameter for the
/// `FunctionForSetOfIntervals` (which also has to appear at the end).
template <
    size_t NumOperands,
    ad_utility::isInstantiation<FunctionAndValueGetters>
        FunctionAndValueGettersT,
    ad_utility::isInstantiation<SpecializedFunction>... SpecializedFunctions>

struct Operation {
 private:
  using OriginalValueGetters = typename FunctionAndValueGettersT::ValueGetters;
  static constexpr size_t NV = std::tuple_size_v<OriginalValueGetters>;

 public:
  constexpr static size_t N = NumOperands;
  using Function = typename FunctionAndValueGettersT::Function;
  using ValueGetters = std::conditional_t<
      NV == 1, std::array<std::tuple_element_t<0, OriginalValueGetters>, N>,
      OriginalValueGetters>;
  Function _function;
  ValueGetters _valueGetters{};
  std::tuple<SpecializedFunctions...> _specializedFunctions{};
};

/// Helper variable to decide at compile time, if a type is an
/// Operation.
template <typename T>
constexpr bool isOperation = false;

template <size_t NumOperations, typename... Ts>
constexpr bool isOperation<Operation<NumOperations, Ts...>> = true;

// Return the common logical size of the `SingleExpressionResults`.
// This is either 1 (in case all the `inputs` are constants) or the
// size of the `context`.
template <SingleExpressionResult... Inputs>
size_t getResultSize(const EvaluationContext& context, const Inputs&...) {
  return (... && isConstantResult<Inputs>) ? 1ul : context.size();
}

}  // namespace detail
}  // namespace sparqlExpression

#endif  // QLEVER_SPARQLEXPRESSIONTYPES_H
