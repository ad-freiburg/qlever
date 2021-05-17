//
// Created by johannes on 09.05.21.
//

#ifndef QLEVER_SPARQLEXPRESSION_H
#define QLEVER_SPARQLEXPRESSION_H

#include <memory>
#include <ranges>
#include <variant>
#include <vector>

#include "../engine/QueryExecutionContext.h"
#include "../engine/ResultTable.h"
#include "../global/Id.h"
#include "../util/ConstexprSmallString.h"

namespace sparqlExpression {

template <conststr Tag, typename Function>
struct TaggedFunction {
  static constexpr conststr tag = Tag;
  using functionType = Function;
};


struct Variable {
  std::string _variable;
  size_t _columnIndex = size_t(-1);
  ResultTable::ResultType _type;
};

// Result type for certain boolean expression: all the values in the intervals are "true"
using Ranges = std::vector<std::pair<size_t, size_t>>;
template <typename T>
constexpr bool isVector = false;

template <typename T>
constexpr bool isVector<std::vector<T>> = true;

using VariableColumnMap = ad_utility::HashMap<std::string, std::pair<size_t, ResultTable::ResultType>>;

struct evaluationInput {
  const QueryExecutionContext* _qec;
  IdTable::const_iterator _begin;
  IdTable::const_iterator _end;
};



inline double getNumericValueFromVariable(const Variable& variable, size_t i, evaluationInput* input) {
  // TODO<joka921>: Optimization for the IdTableStatic.
  const auto id = (*(input->_begin + i))[variable._columnIndex];
  if (variable._type == ResultTable::ResultType::VERBATIM) {
    return id;
  } else if (variable._type == ResultTable::ResultType::FLOAT) {
    // used to store the id value of the entry interpreted as a float
    float tmpF;
    std::memcpy(&tmpF, &id, sizeof(float));
    return tmpF;
  } else if (variable._type == ResultTable::ResultType::TEXT ||
             variable._type == ResultTable::ResultType::LOCAL_VOCAB) {
    return std::numeric_limits<float>::quiet_NaN();
  } else {
    // load the string, parse it as an xsd::int or float
    std::string entity =
        input->_qec->getIndex().idToOptionalString(id).value_or("");
    if (!ad_utility::startsWith(entity, VALUE_FLOAT_PREFIX)) {
      return std::numeric_limits<float>::quiet_NaN();
    } else {
      return ad_utility::convertIndexWordToFloat(entity);
    }
  }
}


struct NumericValueGetter {
  double operator()(double v, evaluationInput*) const { return v; }
  int64_t operator()(int64_t v, evaluationInput*) const { return v; }
  bool operator()(bool v, evaluationInput*) const { return v; }
};

struct BooleanValueGetter {
  bool operator()(double v, evaluationInput*) const { return v; }
  bool operator()(int64_t v, evaluationInput*) const { return v; }
  bool operator()(bool v, evaluationInput*) const { return v; }
};

// Result type for certain boolean expression: all the values in the intervals are "true"
using Ranges = std::vector<std::pair<size_t, size_t>>;

struct RangeIntersector {
  // when we have two sets of ranges, get their "union"
  Ranges operator()(Ranges rangesA, Ranges rangesB) const {
    // First sort by the beginning of the ranges
    std::sort(rangesA.begin(), rangesA.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });
    std::sort(rangesB.begin(), rangesB.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });

    Ranges result;
    auto iteratorA = rangesA.begin();
    auto iteratorB = rangesB.begin();

    // All indices that are smaller than minIdxNotChecked are either already contained in the result ranges or will never become part of them
    size_t minIdxNotChecked = 0;

    // Basically: List merging with ranges
    while (iteratorA < rangesA.end() && iteratorB < rangesB.end()) {
      auto& itSmaller =
          iteratorA->first < iteratorB->first ? iteratorA : iteratorB;
      auto& itGreaterEq =
          iteratorA->first < iteratorB->first ? iteratorB : iteratorA;
      if (itSmaller->second < itGreaterEq->first) {
        // The ranges do not overlap, due to the sorting this means that the smaller (wrt starting index) range is not part of the intersection.
        minIdxNotChecked = itSmaller->second;
        ++itSmaller;
      } else {
        // The ranges overlap
        std::pair<size_t, size_t> nextOutput{
            std::max(minIdxNotChecked, itGreaterEq->first),
            std::min(itGreaterEq->second, itSmaller->second)};
        if (nextOutput.first < nextOutput.second) {
          minIdxNotChecked = nextOutput.second;
          result.push_back(std::move(nextOutput));
        }
        if (minIdxNotChecked >= itSmaller->second) {
          ++itSmaller;
        }
        if (minIdxNotChecked >= itGreaterEq->second) {
          ++itGreaterEq;
        }
      }
    }

    return result;
  }
};

struct RangeMerger {
  // when we have two sets of ranges, get their "union"
  Ranges operator()(Ranges rangesA, Ranges rangesB) const {
    // First sort by the beginning of the ranges
    std::sort(rangesA.begin(), rangesA.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });
    std::sort(rangesB.begin(), rangesB.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });

    Ranges result;
    auto iteratorA = rangesA.begin();
    auto iteratorB = rangesB.begin();

    // All indices that are smaller than minIdxNotChecked are either already contained in the result ranges or will never become part of them
    size_t minIdxNotChecked = 0;

    // Basically: List merging with ranges
    while (iteratorA < rangesA.end() && iteratorB < rangesB.end()) {
      auto& itSmaller =
          iteratorA->first < iteratorB->first ? iteratorA : iteratorB;
      auto& itGreaterEq =
          iteratorA->first < iteratorB->first ? iteratorB : iteratorA;
      if (itSmaller->second < itGreaterEq->first) {
        // The ranges do not overlap, simply append the smaller one, if it was not previously covered
        std::pair<size_t, size_t> nextOutput{
            std::max(minIdxNotChecked, itSmaller->first), itSmaller->second};
        // This check may fail, if this range is fully covered by a range which we have previously handled
        if (nextOutput.first < nextOutput.second) {
          minIdxNotChecked = nextOutput.second;
          result.push_back(std::move(nextOutput));
        }
        ++itSmaller;
      } else {
        // The ranges overlap
        std::pair<size_t, size_t> nextOutput{
            std::max(minIdxNotChecked, itSmaller->first),
            std::max(itGreaterEq->second, itSmaller->second)};
        if (nextOutput.first < nextOutput.second) {
          minIdxNotChecked = nextOutput.second;
          result.push_back(std::move(nextOutput));
        }
        ++itSmaller;
        ++itGreaterEq;
      }
    }

    // Attach the remaining ranges, which do not have any overlap.
    auto attachRemainder = [&minIdxNotChecked, &result](auto iterator,
                                                        const auto& end) {
      for (; iterator < end; ++iterator) {
        std::pair<size_t, size_t> nextOutput{
            std::max(minIdxNotChecked, iterator->first), iterator->second};
        if (nextOutput.first < nextOutput.second) {
          minIdxNotChecked = nextOutput.second;
          result.push_back(std::move(nextOutput));
        }
      }
    };

    attachRemainder(iteratorA, rangesA.end());
    attachRemainder(iteratorB, rangesB.end());

    return result;
  }
};

// Transform a range into a std::vector<bool>, we have to be given the maximal
//  size of an interval
inline std::vector<bool> expandRanges(const Ranges& a, size_t targetSize) {
  // Initialized to 0/false
  std::vector<bool> result(targetSize);
  for (const auto& [first, last] : a) {
    // TODO<joka921> Can we use a more efficient STL algorithm here?
    for (size_t i = first; i < last; ++i) {
      result[i] = true;
    }
  }
  return result;
}

template <typename T>
requires(!std::is_same_v<Ranges, std::decay_t<T>>) auto expandRanges(
    T&& a, [[maybe_unused]] size_t targetSize) {
  if constexpr (isVector<T>) {
    AD_CHECK(a.size() == targetSize);
  }
  return std::forward<T>(a);
}

class SparqlExpression {
 public:
  using EvaluateResult =
      std::variant<std::vector<double>,
                   std::vector<int64_t>, std::vector<bool>, Ranges,
                   double, int64_t, bool, Variable>;

  using Ptr = std::unique_ptr<SparqlExpression>;
  virtual EvaluateResult evaluate(evaluationInput*) const = 0;
  virtual void initializeVariables(
      const VariableColumnMap& variableColumnMap) = 0;

  virtual vector<std::string*> strings() = 0;
  virtual ~SparqlExpression() = default;
};


template <typename T>
class LiteralExpression : public SparqlExpression {
 public:
  LiteralExpression(T _value) : _value{std::move(_value)} {}
  EvaluateResult evaluate(evaluationInput*) const override {
    return EvaluateResult{_value};
  }
  void initializeVariables(
      [[maybe_unused]] const VariableColumnMap& variableColumnMap) override {
    if constexpr (std::is_same_v<T, Variable>) {
      if (!variableColumnMap.contains(_value._variable)) {
        throw std::runtime_error(
            "Variable " + _value._variable +
            "could not be mapped to a column of an expression input");
      }
      _value._columnIndex = variableColumnMap.at(_value._variable).first;
      _value._type = variableColumnMap.at(_value._variable).second;
    }
  }

  vector<std::string *> strings() override {
    if constexpr (std::is_same_v<T, Variable>) {
      return {&_value._variable};
    } else {
      return {};
    }
  }

 private:
  T _value;
};


template <bool RangeCalculationAllowed, typename RangeCalculation,
          typename ValueExtractor, typename BinaryOperation>
class BinaryExpression : public SparqlExpression {
 public:
  BinaryExpression(std::vector<Ptr>&& children)
      : _children{std::move(children)} {};
  EvaluateResult evaluate(evaluationInput* input) const override;;
  void initializeVariables(
      const VariableColumnMap& variableColumnMap) override {
    for (const auto& ptr : _children) {
      ptr->initializeVariables(variableColumnMap);
    }
  }

  // _____________________________________________________________________
  vector<std::string *> strings() override {
    std::vector<string*> result;
    for (const auto& ptr : _children) {
      auto childResult = ptr->strings();
      result.insert(result.end(), childResult.begin(), childResult.end());
    }
    return result;
  }

 private:
  std::vector<Ptr> _children;
};

template <bool RangeCalculationAllowed, typename RangeCalculation,
          typename ValueExtractor, typename UnaryOperation>
class UnaryExpression : public SparqlExpression {
 public:
  UnaryExpression(Ptr&& child) : _child{std::move(child)} {};
  EvaluateResult evaluate(evaluationInput* input) const override;
  void initializeVariables(
      const VariableColumnMap& variableColumnMap) override {
    _child->initializeVariables(variableColumnMap);
  }
  // _____________________________________________________________________
  vector<std::string *> strings() override {
    return _child->strings();
  }

 private:
  Ptr _child;
};

template <typename ValueExtractor, typename... TagAndFunctions>
class DispatchedBinaryExpression2 : public SparqlExpression {
 public:
  DispatchedBinaryExpression2(std::vector<Ptr>&& children,
                             std::vector<conststr>&& relations)
      : _children{std::move(children)}, _relations{std::move(relations)} {};
  EvaluateResult evaluate(evaluationInput* input) const override;
  void initializeVariables(
      const VariableColumnMap& variableColumnMap) override {
    for (const auto& ptr : _children) {
      ptr->initializeVariables(variableColumnMap);
    }
  }

  // _____________________________________________________________________
  vector<std::string *> strings() override {
    std::vector<string*> result;
    for (const auto& ptr : _children) {
      auto childResult = ptr->strings();
      result.insert(result.end(), childResult.begin(), childResult.end());
    }
    return result;
  }

 private:
  std::vector<Ptr> _children;
  std::vector<conststr> _relations;
};

/// The Actual aliases for the expressions

inline auto orLambda = [](bool a, bool b) { return a || b; };
using ConditionalOrExpression =
    BinaryExpression<true, RangeMerger, BooleanValueGetter,
                     decltype(orLambda)>;

inline auto andLambda = [](bool a, bool b) { return a && b; };
using ConditionalAndExpression =
    BinaryExpression<true, RangeIntersector, BooleanValueGetter,
                     decltype(andLambda)>;

struct EmptyType{};
inline auto unaryNegate = [](bool a) -> bool { return !a; };
using UnaryNegateExpression =
    UnaryExpression<false, EmptyType, BooleanValueGetter,
                    decltype(unaryNegate)>;
inline auto unaryMinus = [](auto a) -> double { return -a; };
using UnaryMinusExpression =
    UnaryExpression<false, EmptyType, NumericValueGetter,
                    decltype(unaryMinus)>;

inline auto multiply = [](const auto& a, const auto& b) -> double { return a * b; };
inline auto divide = [](const auto& a, const auto& b) -> double { return static_cast<double>(a) / b; };
using MultiplicativeExpression =
    DispatchedBinaryExpression2<NumericValueGetter, TaggedFunction<"*", decltype(multiply)>, TaggedFunction<"/", decltype(divide)>>;

inline auto add = [](const auto& a, const auto& b) -> double { return a + b; };
inline auto subtract = [](const auto& a, const auto& b) -> double { return a - b; };
using AdditiveExpression =
    DispatchedBinaryExpression2<NumericValueGetter, TaggedFunction<"+", decltype(add)>, TaggedFunction<"-", decltype(subtract)>>;

using BooleanLiteralExpression = LiteralExpression<bool>;
using IntLiteralExpression = LiteralExpression<int64_t>;
using DoubleLiteralExpression = LiteralExpression<double>;
using VariableExpression = LiteralExpression<Variable>;

}  // namespace sparqlExpression

#endif  // QLEVER_SPARQLEXPRESSION_H
