//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

// Several helper types needed for the SparqlExpression module

#ifndef QLEVER_SPARQLEXPRESSIONTYPES_H
#define QLEVER_SPARQLEXPRESSIONTYPES_H

#include "../../global/Id.h"
#include "../../util/AllocatorWithLimit.h"
#include "../../util/ConstexprSmallString.h"
#include "../../util/Generator.h"
#include "../../util/HashMap.h"
#include "../../util/TypeTraits.h"
#include "../QueryExecutionContext.h"
#include "../ResultTable.h"
#include "SetOfIntervals.h"

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

/// A strong type for Ids from the knowledge base to distinguish them from plain
/// integers.
struct StrongId {
  Id _value;
  friend auto operator<=>(const StrongId&, const StrongId&) = default;

  // Make the type hashable for absl, see https://abseil.io/docs/cpp/guides/hash
  template <typename H>
  friend H AbslHashValue(H h, const StrongId& id) {
    return H::combine(std::move(h), id._value);
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

/// A StrongId and its type. The type is needed to get the actual value from the
/// knowledge base.
struct StrongIdWithResultType {
  StrongId _id;
  ResultTable::ResultType _type;
  bool operator==(const StrongIdWithResultType&) const = default;
  size_t size() const { return 1; }

  // Make the type hashable for absl, see https://abseil.io/docs/cpp/guides/hash
  template <typename H>
  friend H AbslHashValue(H h, const StrongIdWithResultType& id) {
    return H::combine(std::move(h), id._id, id._type);
  }
};

/// A list of StrongIds that all have the same datatype.
struct StrongIdsWithResultType {
  VectorWithMemoryLimit<StrongId> _ids;
  ResultTable::ResultType _type;
  size_t size() const { return _ids.size(); }
};

/// Typedef for a map from variable names to the corresponding column in the
/// input of a SparqlExpression.
using VariableToColumnMap = ad_utility::HashMap<std::string, size_t>;

/// Typedef for a map from variables names to (input column, type of input
/// column.
using VariableToColumnAndResultTypeMap =
    ad_utility::HashMap<std::string,
                        std::pair<size_t, ResultTable::ResultType>>;

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
  const ResultTable::LocalVocab& _localVocab;

  /// Constructor for evaluating an expression on the complete input.
  EvaluationContext(
      const QueryExecutionContext& qec,
      const VariableToColumnAndResultTypeMap& variableToColumnAndResultTypeMap,
      const IdTable& inputTable,
      const ad_utility::AllocatorWithLimit<Id>& allocator,
      const ResultTable::LocalVocab& localVocab)
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
                    const ResultTable::LocalVocab& localVocab)
      : _qec{qec},
        _variableToColumnAndResultTypeMap{map},
        _inputTable{inputTable},
        _beginIndex{beginIndex},
        _endIndex{endIndex},
        _allocator{allocator},
        _localVocab{localVocab} {}
};

/// Strong type for a Sparql Variable, e.g. "?x"
struct Variable {
  std::string _variable;
  bool operator==(const Variable&) const = default;
};

/// The result of an expression can either be a vector of bool/double/int/string
/// a variable (e.g. in BIND (?x as ?y)) or a "Set" of indices, which identifies
/// the row indices in which a boolean expression evaluates to "true". Constant
/// results are represented by a vector with only one element.
namespace detail {
// For each type T in this tuple, T as well as VectorWithMemoryLimit<T> are
// possible expression result types.
using ConstantTypes = std::tuple<double, int64_t, Bool, string>;
using ConstantTypesAsVector =
    ad_utility::LiftedTuple<ConstantTypes, VectorWithMemoryLimit>;

// Each type in this tuple also is a possible expression result type.
using OtherTypes =
    std::tuple<ad_utility::SetOfIntervals, StrongIdWithResultType, Variable>;

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
    ad_utility::isTypeContainedIn<T, detail::ConstantTypes> ||
    std::is_same_v<T, StrongIdWithResultType>;

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
template <SingleExpressionResult T, typename LocalVocab>
Id constantExpressionResultToId(T&& result, LocalVocab& localVocab,
                                bool isRepetitionOfConstant) {
  static_assert(isConstantResult<T>);
  constexpr static auto type = expressionResultTypeToQleverResultType<T>();
  if constexpr (type == qlever::ResultType::VERBATIM) {
    return Id{result};
  } else if constexpr (type == qlever::ResultType::FLOAT) {
    auto tmpF = static_cast<float>(result);
    Id id = Id::make(0);
    std::memcpy(&id.get(), &tmpF, sizeof(float));
    return id;
  } else if constexpr (type == qlever::ResultType::LOCAL_VOCAB) {
    // Return the index in the local vocabulary.
    if (!isRepetitionOfConstant) {
      localVocab.push_back(std::forward<T>(result));
    }
    return Id::make(localVocab.size() - 1);
  } else {
    static_assert(ad_utility::alwaysFalse<T>,
                  "The result types other than VERBATIM, FLOAT and LOCAL_VOCAB "
                  "have to be handled differently");
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
  static constexpr bool checkIfOperandsAreValid() {
    return CheckT{}.template operator()<Operands...>();
  }

  // Evaluate the function on the `operands`. Return std::nullopt if the
  // function cannot be evaluated on the `operands`
  template <typename... Operands>
  std::optional<ExpressionResult> evaluateIfOperandsAreValid(
      Operands&&... operands) {
    if constexpr (!checkIfOperandsAreValid<Operands...>()) {
      return std::nullopt;
    } else {
      return Function{}(std::forward<Operands>(operands)...);
    }
  }
};

/// Return true iff there exists a `SpecializedFunction` in the
/// `SpecializedFunctionsTuple` that can be evaluated on all the `Operands`
template <typename SpecializedFunctionsTuple, typename... Operands>
constexpr bool isAnySpecializedFunctionPossible(SpecializedFunctionsTuple&& tup,
                                                Operands&&...) {
  auto onPack = [](auto&&... fs) constexpr {
    return (... || fs.template checkIfOperandsAreValid<Operands...>());
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
template <size_t NumOperands, typename FunctionAndValueGettersT,
          typename... SpecializedFunctions>
    requires ad_utility::isInstantiation<FunctionAndValueGetters,
                                         FunctionAndValueGettersT> &&
    (... && ad_utility::isInstantiation<SpecializedFunction,
                                        SpecializedFunctions>)struct Operation {
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
  ValueGetters _valueGetters;
  std::tuple<SpecializedFunctions...> _specializedFunctions;
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
  return (... && isConstantResult<Inputs>)
             ? 1ul
             : context._endIndex - context._beginIndex;
}

}  // namespace detail
}  // namespace sparqlExpression

#endif  // QLEVER_SPARQLEXPRESSIONTYPES_H
