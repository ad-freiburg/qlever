//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

// Several helper types needed for the SparqlExpression module

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_SPARQLEXPRESSIONTYPES_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_SPARQLEXPRESSIONTYPES_H

#include <vector>

#include "engine/QueryExecutionContext.h"
#include "engine/sparqlExpressions/SetOfIntervals.h"
#include "global/Id.h"
#include "parser/data/Variable.h"
#include "util/AllocatorWithLimit.h"
#include "util/HashSet.h"
#include "util/TypeTraits.h"
#include "util/VisitMixin.h"

namespace sparqlExpression {

// A std::vector<T, AllocatorWithLimit> with deleted copy constructor
// and copy assignment. Used in the SparqlExpression module, where we want
// no accidental copies of large intermediate results.
template <typename T>
class VectorWithMemoryLimit
    : public std::vector<T, ad_utility::AllocatorWithLimit<T>> {
 public:
  using Allocator = ad_utility::AllocatorWithLimit<T>;
  using Base = std::vector<T, ad_utility::AllocatorWithLimit<T>>;

  // The `AllocatorWithMemoryLimit` is not default-constructible (on purpose).
  // Unfortunately, the support for such allocators is not really great in the
  // standard library. In particular, the type trait
  // `std::default_initializable<std::vector<T, Alloc>>` will be true, even if
  // the `Alloc` is not default-initializable, which leads to hard compile
  // errors with the ranges library. For this reason we cannot simply inherit
  // all the constructors from `Base`, but explicitly have to forward all but
  // the default constructor. In particular, we only forward constructors that
  // have
  // * at least one argument
  // * the first argument must not be similar to `std::vector` or
  // `VectorWithMemoryLimit` to not hide copy or move constructors
  // * the last argument must be `AllocatorWithMemoryLimit` (all constructors to
  // `vector` take the allocator as a last parameter)
  // * there must be a constructor of `Base` for the given arguments.
  CPP_template(typename... Args)(
      requires(sizeof...(Args) > 0) CPP_and CPP_NOT(
          concepts::derived_from<
              std::remove_cvref_t<ad_utility::First<Args...>>, Base>)
          CPP_and concepts::convertible_to<ad_utility::Last<Args...>, Allocator>
              CPP_and concepts::constructible_from<
                  Base, Args&&...>) explicit(sizeof...(Args) == 1)
      VectorWithMemoryLimit(Args&&... args)
      : Base{AD_FWD(args)...} {}

  // We have to explicitly forward the `initializer_list` constructor because it
  // for some reason is not covered by the above generic mechanism.
  VectorWithMemoryLimit(std::initializer_list<T> init, const Allocator& alloc)
      : Base(init, alloc) {}

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
static_assert(!std::default_initializable<VectorWithMemoryLimit<int>>);

// A class to store the results of expressions that can yield strings or IDs as
// their result (for example IF and COALESCE). It is also used for expressions
// that can only yield strings.
using IdOrLiteralOrIri = std::variant<ValueId, LocalVocabEntry>;
// Printing for GTest.
void PrintTo(const IdOrLiteralOrIri& var, std::ostream* os);

/// The result of an expression can either be a vector of bool/double/int/string
/// a variable (e.g. in BIND (?x as ?y)) or a "Set" of indices, which identifies
/// the row indices in which a boolean expression evaluates to "true". Constant
/// results are represented by a vector with only one element.
namespace detail {
// For each type T in this tuple, T as well as VectorWithMemoryLimit<T> are
// possible expression result types.
using ConstantTypes = std::tuple<IdOrLiteralOrIri, ValueId>;
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
CPP_concept SingleExpressionResult =
    ad_utility::SimilarToAnyTypeIn<T, ExpressionResult>;

// Copy an expression result.
CPP_template(typename ResultT)(
    requires ad_utility::SimilarTo<ResultT,
                                   ExpressionResult>) inline ExpressionResult
    copyExpressionResult(ResultT&& result) {
  auto copyIfCopyable =
      []<typename R>(const R& x) -> CPP_ret(ExpressionResult)(
                                     requires SingleExpressionResult<R>) {
    if constexpr (std::is_constructible_v<R, decltype(AD_FWD(x))>) {
      return AD_FWD(x);
    } else {
      return x.clone();
    }
  };
  return std::visit(copyIfCopyable, AD_FWD(result));
}

/// True iff T represents a constant.
template <typename T>
constexpr static bool isConstantResult =
    ad_utility::SimilarToAnyTypeIn<T, detail::ConstantTypes>;

/// True iff T is one of the ConstantTypesAsVector
template <typename T>
constexpr static bool isVectorResult =
    ad_utility::SimilarToAnyTypeIn<T, detail::ConstantTypesAsVector> ||
    ad_utility::isSimilar<T, std::span<const ValueId>>;

/// All the additional information which is needed to evaluate a SPARQL
/// expression.
struct EvaluationContext {
  const QueryExecutionContext& _qec;
  // The VariableToColumnMap of the input
  const VariableToColumnMap& _variableToColumnMap;

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
  std::vector<ColumnIndex> _columnsByWhichResultIsSorted;

  /// Let the expression evaluation also respect the memory limit.
  ad_utility::AllocatorWithLimit<Id> _allocator;

  /// The local vocabulary of the input.
  LocalVocab& _localVocab;

  // If the expression is part of a GROUP BY then this member has to be set to
  // the variables by which the input is grouped. These variables will then be
  // treated as constants.
  ad_utility::HashSet<Variable> _groupedVariables;

  // Only needed during GROUP BY evaluation.
  // Stores information about the results from previous expressions of the same
  // SELECT clause line that might be accessed in the same SELECT clause.

  // This map maps variables that are bound in the select clause to indices.
  VariableToColumnMap _variableToColumnMapPreviousResults;
  // This vector contains the last result of the expressions in the SELECT
  // clause. The correct index for a given variable is obtained from the
  // `_variableToColumnMapPreviousResults`.
  std::vector<ExpressionResult> _previousResultsFromSameGroup;

  // Used to modify the behavior of the RAND() expression when it is evaluated
  // as part of a GROUP BY clause.
  bool _isPartOfGroupBy = false;

  ad_utility::SharedCancellationHandle cancellationHandle_;

  // A point in time at which the evaluation of the expression is to be
  // cancelled because of a timeout. This is used by mechanisms that can't use
  // the `cancellationHandle_` directly, like the sorting in a `COUNT DISTINCT
  // *` expression.
  using TimePoint = std::chrono::steady_clock::time_point;
  TimePoint deadline_;

  /// Constructor for evaluating an expression on the complete input.
  EvaluationContext(const QueryExecutionContext& qec,
                    const VariableToColumnMap& variableToColumnMap,
                    const IdTable& inputTable,
                    const ad_utility::AllocatorWithLimit<Id>& allocator,
                    LocalVocab& localVocab,
                    ad_utility::SharedCancellationHandle cancellationHandle,
                    TimePoint deadline);

  bool isResultSortedBy(const Variable& variable);
  // The size (in number of elements) that this evaluation context refers to.
  [[nodiscard]] size_t size() const;

  // ____________________________________________________________________________
  [[nodiscard]] std::optional<ColumnIndex> getColumnIndexForVariable(
      const Variable& var) const;

  // _____________________________________________________________________________
  std::optional<ExpressionResult> getResultFromPreviousAggregate(
      const Variable& var) const;
};

namespace detail {
/// Get Id of constant result of type T.
CPP_template(typename T, typename LocalVocabT)(
    requires SingleExpressionResult<T> CPP_and isConstantResult<T> CPP_and
        std::is_rvalue_reference_v<T&&>) Id
    constantExpressionResultToId(T&& result, LocalVocabT& localVocab) {
  if constexpr (ad_utility::isSimilar<T, Id>) {
    return result;
  } else if constexpr (ad_utility::isSimilar<T, IdOrLiteralOrIri>) {
    return std::visit(
        [&localVocab]<typename R>(R&& el) mutable {
          if constexpr (ad_utility::isSimilar<R, LocalVocabEntry>) {
            return Id::makeFromLocalVocabIndex(
                localVocab.getIndexAndAddIfNotContained(AD_FWD(el)));
          } else {
            static_assert(ad_utility::isSimilar<R, Id>);
            return el;
          }
        },
        AD_FWD(result));
  } else {
    static_assert(ad_utility::alwaysFalse<T>);
  }
}

/// A `Function` and one or more `ValueGetters`, that are applied to the
/// operands of the function before passing them. The number of `ValueGetters`
/// must either be 1 (the same `ValueGetter` is used for all the operands to
/// the `Function`), or it must be equal to the number of operands to the
/// `Function`. This invariant is checked in the `Operation` class template
/// below, which uses this helper struct.
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
      if constexpr (ranges::invocable<Function, Operands&&...>) {
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

// Class for an operation used in a `SparqlExpression`, consisting of the
// function for computing the operation and the value getters for the operands.
// The number of operands is fixed.
//
// NOTE: This class is defined in the namespace `sparqlExpression` and is
// different from the class with the same name defined in `Operation.h`
//
// An Operation that consists of a `FunctionAndValueGetters` that takes
// `NumOperands` parameters. The `SpecializedFunction`s can be used to choose a
// more efficient implementation given the types of the operands. For example,
// expressions like `logical-or` or `logical-and` can be implemented more
// efficiently if all the inputs are `SetOfInterval`s`.
// Note: It is necessary to use the `FunctionAndValueGetters` struct to allow
// for multiple `ValueGetters` because there can be multiple `ValueGetters` as
// well as zero or more `SpezializedFunctions`, but there can only be a single
// parameter pack in C++.
template <size_t NumOperands,
          QL_CONCEPT_OR_TYPENAME(
              ad_utility::isInstantiation<FunctionAndValueGetters>)
              FunctionAndValueGettersT,
          QL_CONCEPT_OR_TYPENAME(ad_utility::isInstantiation<
                                 SpecializedFunction>)... SpecializedFunctions>
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
CPP_template(typename... Inputs)(requires(SingleExpressionResult<Inputs>&&...))
    size_t getResultSize(const EvaluationContext& context, const Inputs&...) {
  return (... && isConstantResult<Inputs>) ? 1ul : context.size();
}

}  // namespace detail
}  // namespace sparqlExpression

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_SPARQLEXPRESSIONTYPES_H
