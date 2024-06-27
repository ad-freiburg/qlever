//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

// Several helper types needed for the SparqlExpression module

#ifndef QLEVER_SPARQLEXPRESSIONTYPES_H
#define QLEVER_SPARQLEXPRESSIONTYPES_H

#include <vector>

#include "engine/QueryExecutionContext.h"
#include "engine/sparqlExpressions/SetOfIntervals.h"
#include "global/Id.h"
#include "parser/LiteralOrIri.h"
#include "parser/TripleComponent.h"
#include "parser/data/Variable.h"
#include "util/AllocatorWithLimit.h"
#include "util/HashSet.h"
#include "util/TypeTraits.h"
#include "util/VisitMixin.h"

namespace sparqlExpression {

/// A std::vector<T, AllocatorWithLimit> with deleted copy constructor
/// and copy assignment. Used in the SparqlExpression module, where we want
/// no accidental copies of large intermediate results.
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
  template <typename... Args>
  requires(sizeof...(Args) > 0 &&
           !std::derived_from<std::remove_cvref_t<ad_utility::First<Args...>>,
                              Base> &&
           std::convertible_to<ad_utility::Last<Args...>, Allocator> &&
           std::constructible_from<Base, Args && ...>)
  explicit(sizeof...(Args) == 1) VectorWithMemoryLimit(Args&&... args)
      : Base{AD_FWD(args)...} {}

  // We have to explicitly forward the `initializer_list` constructor because it
  // for some reason is not covered by the above generic mechanism.
  VectorWithMemoryLimit(std::initializer_list<T> init, const Allocator& alloc)
      : Base(init, alloc){};

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
using IdOrLiteralOrIri =
    std::variant<ValueId, ad_utility::triple_component::LiteralOrIri>;
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
concept SingleExpressionResult =
    ad_utility::SimilarToAnyTypeIn<T, ExpressionResult>;

// Copy an expression result.
inline ExpressionResult copyExpressionResult(
    ad_utility::SimilarTo<ExpressionResult> auto&& result) {
  auto copyIfCopyable =
      []<SingleExpressionResult R>(const R& x) -> ExpressionResult {
    if constexpr (requires { R{AD_FWD(x)}; }) {
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
  const LocalVocab& _localVocab;

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

  /// Constructor for evaluating an expression on the complete input.
  EvaluationContext(const QueryExecutionContext& qec,
                    const VariableToColumnMap& variableToColumnMap,
                    const IdTable& inputTable,
                    const ad_utility::AllocatorWithLimit<Id>& allocator,
                    const LocalVocab& localVocab,
                    ad_utility::SharedCancellationHandle cancellationHandle);

  bool isResultSortedBy(const Variable& variable);
  // The size (in number of elements) that this evaluation context refers to.
  [[nodiscard]] size_t size() const;

  // ____________________________________________________________________________
  [[nodiscard]] ColumnIndex getColumnIndexForVariable(
      const Variable& var) const;

  // _____________________________________________________________________________
  std::optional<ExpressionResult> getResultFromPreviousAggregate(
      const Variable& var) const;
};

namespace detail {
/// Get Id of constant result of type T.
template <SingleExpressionResult T, typename LocalVocabT>
requires isConstantResult<T> && std::is_rvalue_reference_v<T&&>
Id constantExpressionResultToId(T&& result, LocalVocabT& localVocab) {
  if constexpr (ad_utility::isSimilar<T, Id>) {
    return result;
  } else if constexpr (ad_utility::isSimilar<T, IdOrLiteralOrIri>) {
    return std::visit(
        [&localVocab]<typename R>(R&& el) mutable {
          if constexpr (ad_utility::isSimilar<
                            R, ad_utility::triple_component::LiteralOrIri>) {
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
