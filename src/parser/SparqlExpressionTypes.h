//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

// Several helper types needed for the SparqlExpression module

#ifndef QLEVER_SPARQLEXPRESSIONTYPES_H
#define QLEVER_SPARQLEXPRESSIONTYPES_H

#include "../engine/QueryExecutionContext.h"
#include "../engine/ResultTable.h"
#include "../global/Id.h"
#include "../util/AllocatorWithLimit.h"
#include "../util/ConstexprSmallString.h"
#include "../util/HashMap.h"
#include "../util/TypeTraits.h"
#include "./SetOfIntervals.h"

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
  VectorWithMemoryLimit(VectorWithMemoryLimit&&) = default;
  VectorWithMemoryLimit& operator=(VectorWithMemoryLimit&&) = default;

  // Allow copying via an explicit clone() function.
  VectorWithMemoryLimit clone() const {
    // Call the private copy constructor.
    return VectorWithMemoryLimit(*this);
  }
};

/// A strong type for Ids from the knowledge base to distinguish them from plain
/// integers.
struct StrongId {
  Id _value;
  bool operator==(const StrongId&) const = default;

  // Make the type hashable for absl, see https://abseil.io/docs/cpp/guides/hash
  template <typename H>
  friend H AbslHashValue(H h, const StrongId& id) {
    return H::combine(std::move(h), id._value);
  }
};

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
namespace expressionResultDetail {
// For each type T in this tuple, T as well as VectorWithMemoryLimit<T> are
// possible expression result types.
using ConstantTypes = std::tuple<double, int64_t, bool, string>;
using ConstantTypesAsVector =
    ad_utility::LiftedTuple<ConstantTypes, VectorWithMemoryLimit>;

// Each type in this tuple also is a possible expression result type.
using OtherTypes =
    std::tuple<ad_utility::SetOfIntervals, StrongIdWithResultType, Variable>;

using AllTypesAsTuple =
    ad_utility::TupleCat<ConstantTypes, ConstantTypesAsVector, OtherTypes>;
}  // namespace expressionResultDetail

/// An Expression result is a std::variant of all the different types from
/// the expressionResultDetail namespace (see above).
using ExpressionResult =
    ad_utility::TupleToVariant<expressionResultDetail::AllTypesAsTuple>;

/// Only the different types contained in the variant `ExpressionResult` (above)
/// match this concept.
template <typename T>
concept SingleExpressionResult =
    ad_utility::isTypeContainedIn<T, ExpressionResult>;

/// True iff T represents a constant.
template <SingleExpressionResult T>
constexpr static bool isConstantResult =
    ad_utility::isTypeContainedIn<T, expressionResultDetail::ConstantTypes> ||
    std::is_same_v<T, StrongIdWithResultType>;

/// True iff T is one of the ConstantTypesAsVector
template <SingleExpressionResult T>
constexpr static bool isVectorResult = ad_utility::isTypeContainedIn<
    T, expressionResultDetail::ConstantTypesAsVector>;

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
    Id id = 0;
    ;
    std::memcpy(&id, &tmpF, sizeof(float));
    return id;
  } else if constexpr (type == qlever::ResultType::LOCAL_VOCAB) {
    // Return the index in the local vocabulary.
    if (!isRepetitionOfConstant) {
      localVocab.push_back(std::forward<T>(result));
    }
    return localVocab.size() - 1;
  } else {
    static_assert(ad_utility::alwaysFalse<T>,
                  "The result types other than VERBATIM, FLOAT and LOCAL_VOCAB "
                  "have to be handled differently");
  }
}

/// We will use the string representation of various functions (e.g. '+' '*')
/// directly as template parameters. Currently 15 characters are enough for
/// this, but if we need longer names in the future, we can still change this at
/// the cost of a recompilation.
using TagString = ad_utility::ConstexprSmallString<16>;

/// Annotate an arbitrary type (we will use this for callables) with a
/// TagString. The TagString is part of the type.
template <TagString Tag, typename Function>
struct TaggedFunction {
  static constexpr TagString tag = Tag;
  using functionType = Function;
};

/// Helper variables and concepts to decide at compile time, if a type is an
/// instantiation of TaggedFunction.
template <typename T>
constexpr bool IsTaggedFunction = false;

template <TagString Tag, typename Function>
constexpr bool IsTaggedFunction<TaggedFunction<Tag, Function>> = true;

template <typename T>
concept TaggedFunctionConcept = IsTaggedFunction<T>;
}  // namespace sparqlExpression

#endif  // QLEVER_SPARQLEXPRESSIONTYPES_H
