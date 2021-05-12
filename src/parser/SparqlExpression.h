//
// Created by johannes on 09.05.21.
//

#ifndef QLEVER_SPARQLEXPRESSION_H
#define QLEVER_SPARQLEXPRESSION_H

#include <vector>
#include <memory>
#include <variant>
#include <ranges>
#include "../engine/ResultTable.h"
#include "../global/Id.h"
#include "../engine/QueryExecutionContext.h"

template<typename T>
constexpr bool isVector = false;

template<typename T>
constexpr bool isVector<std::vector<T>> = true;

struct evaluationInput {
  size_t _inputSize;
  const QueryExecutionContext* _qec;
  ad_utility::HashMap<std::string, size_t> _variableColumnMap;
  const IdTable* _inputTable;
};

struct StrongId {
  Id _value;
};

struct Variable {
  std::string _variable;
  size_t _columnIndex = size_t(-1);
  ResultTable::ResultType _type;
  double operator()(size_t i, evaluationInput* input) {
    const auto& id = input->_inputTable->operator()(i, _columnIndex);
    if (_type == ResultTable::ResultType::VERBATIM) {
      return id;
    } else if (_type == ResultTable::ResultType::FLOAT) {
      // used to store the id value of the entry interpreted as a float
      float tmpF;
      std::memcpy(&tmpF, &id, sizeof(float));
      return tmpF;
    } else if (_type == ResultTable::ResultType::TEXT ||
    _type == ResultTable::ResultType::LOCAL_VOCAB) {
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
};

struct NumericValueGetter {
  double operator()(double v, evaluationInput*) const { return v; }
  int64_t operator()(int64_t v, evaluationInput*) const { return v; }
  bool operator()(bool v, evaluationInput*) const { return v; }

  double operator()(StrongId id, evaluationInput*) const;

};

struct BooleanValueGetter {
  bool operator()(double v, evaluationInput*) const { return v; }
  bool operator()(int64_t v, evaluationInput*) const { return v; }
   bool operator()(bool v, evaluationInput*) const { return v; }

   bool operator()(StrongId id, evaluationInput* input) const {
    // load the string, parse it as an xsd::int or float
    auto optional = input->_qec->getIndex().idToOptionalString(id._value);
    if (!optional) {
      return false;
    }
    const auto& entity = optional.value();
    if (!ad_utility::startsWith(entity, VALUE_FLOAT_PREFIX)) {
      // Strings that exist are considered as a boolean "True"
      // TODO: what do we do about "" and <>, are they false
      return true;
    } else {
      auto value = ad_utility::convertIndexWordToFloat(entity);
      return value != 0.0;
    }
  }
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
        std::pair<size_t, size_t> nextOutput{std::max(minIdxNotChecked, itGreaterEq->first), std::min(itGreaterEq->second, itSmaller->second)};
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
        std::pair<size_t, size_t> nextOutput{std::max(minIdxNotChecked, itSmaller->first), itSmaller->second};
        // This check may fail, if this range is fully covered by a range which we have previously handled
        if (nextOutput.first < nextOutput.second) {
          minIdxNotChecked = nextOutput.second;
          result.push_back(std::move(nextOutput));
        }
        ++itSmaller;
      } else {
        // The ranges overlap
        std::pair<size_t, size_t> nextOutput{std::max(minIdxNotChecked, itSmaller->first), std::max(itGreaterEq->second, itSmaller->second)};
        if (nextOutput.first < nextOutput.second) {
          minIdxNotChecked = nextOutput.second;
          result.push_back(std::move(nextOutput));
        }
        ++itSmaller;
        ++itGreaterEq;
      }
    }

    // Attach the remaining ranges, which do not have any overlap.
    auto attachRemainder = [&minIdxNotChecked, &result](auto iterator, const auto& end) {
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

//Transform a range into a std::vector<bool>, we have to be given the maximal
// size of an interval
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

template<typename T> requires (!std::is_same_v<Ranges, std::decay_t<T>>)
auto expandRanges(T&& a,[[maybe_unused]] size_t targetSize) {
  if constexpr (isVector<T>) {
    AD_CHECK(a.size() == targetSize);
  }
    return std::forward<T>(a);
}

// calculate the boolean negation of a range.
Ranges negateRanges(const Ranges& a, size_t targetSize);

// Class for a completely invalid result
class InvalidResult{};

class SparqlExpression {
 public:
  using EvaluateResult = std::variant<std::vector<StrongId>, std::vector<double>, std::vector<int64_t>, std::vector<bool>, Ranges, StrongId, double, int64_t, bool>;
  using Ptr = std::unique_ptr<SparqlExpression>;
  virtual EvaluateResult evaluate(evaluationInput*) const = 0;
  virtual ~SparqlExpression() = default;
};


template <bool RangeCalculationAllowed, typename RangeCalculation, typename ValueExtractor, typename BinaryOperation>
auto liftBinaryCalculationToEvaluateResults(RangeCalculation rangeCalculation, ValueExtractor valueExtractor, BinaryOperation binaryOperation, evaluationInput* input) {
  return [input, rangeCalculation, valueExtractor, binaryOperation](const auto& a, const auto& b) -> SparqlExpression::EvaluateResult {
    using A = std::decay_t<decltype(a)>;
    using B = std::decay_t<decltype(b)>;
    if constexpr (RangeCalculationAllowed && std::is_same_v<A, Ranges> && std::is_same_v<B, Ranges>) {
      return rangeCalculation(a, b);
    } else {
      auto aExpand = expandRanges(std::move(a), input->_inputSize);
      auto bExpand = expandRanges(std::move(b), input->_inputSize);
      //AD_CHECK(aExpand.size() == bExpand.size());


      auto extractValueA = [&aExpand, &valueExtractor, input](size_t index) {
        if constexpr (isVector<A>) {
          return valueExtractor(aExpand[index], input);
        } else if constexpr (std::is_same_v<Variable, A>) {
          return valueExtractor(StrongId{input->_inputTable->template operator()(index, aExpand._columnIndex)});
        } else {
          return valueExtractor(aExpand, input);
        }
      };

      auto extractValueB = [&bExpand, &valueExtractor, input](size_t index) {
        if constexpr (isVector<B>) {
          return valueExtractor(bExpand[index], input);
        } else if constexpr (std::is_same_v<Variable, B>) {
          return valueExtractor(StrongId{input->_inputTable->template operator()(index, bExpand._columnIndex)});
        } else {
          return valueExtractor(bExpand, input);
        }
      };

      using ResultType = std::decay_t<decltype(binaryOperation(extractValueA(0),
                                                               extractValueB(0)))>;
      std::vector<ResultType> result;
      size_t targetSize = 1;
      if constexpr (isVector<A>) {
        targetSize = aExpand.size();
      }
      if constexpr (isVector<B>) {
        targetSize = bExpand.size();
      }
      result.reserve(targetSize);
      for (size_t i = 0; i < targetSize; ++i) {
        result.push_back(binaryOperation(extractValueA(i), extractValueB(i)));
      }
      if (targetSize == 1) {
        return result[0];
      } else {
        return result;
      }
    }
  };

}

template <bool RangeCalculationAllowed, typename RangeCalculation, typename ValueExtractor, typename BinaryOperation>
class BinaryExpression : public SparqlExpression {
 public:
  BinaryExpression(std::vector<Ptr>&& children): _children{std::move(children)} {};
  EvaluateResult  evaluate(evaluationInput* input) const override {
    auto firstResult = _children[0]->evaluate(input);

    auto calculator = liftBinaryCalculationToEvaluateResults<RangeCalculationAllowed>(RangeCalculation{}, ValueExtractor{}, BinaryOperation{} , input);
    for (size_t i = 1; i < _children.size(); ++i) {
      firstResult = std::visit(calculator, firstResult, _children[i]->evaluate(input));
    }
    return firstResult;

  };
 private:
  std::vector<Ptr> _children;

};


template <size_t MaxValue>
struct LambdaVisitor {
  template<typename Tuple, typename Enum, typename ValueExtractor, typename... Args>
  decltype(auto) operator()(Tuple t, Enum r, Args&&... args) {
    static_assert(MaxValue < std::tuple_size_v<Tuple>);
    AD_CHECK(static_cast<size_t>(r) < std::tuple_size_v<Tuple>);
    if (static_cast<size_t>(r) == MaxValue) {
      return liftBinaryCalculationToEvaluateResults<false>(0, ValueExtractor{}, std::get<MaxValue>(t), std::forward<Args>(args)...);
    } else {
      return LambdaVisitor<MaxValue - 1>{}(t, r,
                                           std::forward<Args>(args)...);
    }
  }
};

template <>
struct LambdaVisitor<0> {
  template<typename Tuple, typename Enum, typename ValueExtractor, typename... Args>
  auto operator()(Tuple t, Enum r, Args&&... args) {
    if (static_cast<size_t>(r) == 0) {
      return liftBinaryCalculationToEvaluateResults<false>(0, ValueExtractor{}, std::get<0>(t), std::forward<Args>(args)...);
    } else {
      AD_CHECK(false);
    }
  }
};

template <typename ValueExtractor, typename BinaryOperationTuple, typename RelationDispatchEnum>
class DispatchedBinaryExpression : public SparqlExpression {
 public:
  DispatchedBinaryExpression(std::vector<Ptr>&& children, std::vector<RelationDispatchEnum>&& relations): _children{std::move(children)}, _relations{std::move(relations)} {};
  EvaluateResult  evaluate(evaluationInput* input) const override {
    auto firstResult = _children[0]->evaluate(input);

    for (size_t i = 1; i < _children.size(); ++i) {
      //auto calculator = liftBinaryCalculationToEvaluateResults<false>(0, ValueExtractor{}, BinaryOperation{} , input);
      auto calculator = LambdaVisitor<std::tuple_size_v<BinaryOperationTuple> - 1>{}(BinaryOperationTuple{}, _relations[i-1], ValueExtractor{}, input);
      firstResult = std::visit(calculator, firstResult, _children[i]->evaluate(input));
    }
    return firstResult;

  };
 private:
  std::vector<Ptr> _children;
  std::vector<RelationDispatchEnum> _relations;

};

using ConditionalOrExpression = BinaryExpression<true, RangeMerger, BooleanValueGetter, decltype([](bool a, bool b){return a || b;})>;
using ConditionalAndExpression = BinaryExpression<true, RangeIntersector, BooleanValueGetter, decltype([](bool a, bool b){return a && b;})>;


/*
class conditionalAndExpression;
class relationalExpression;
class multiplicativeExpression {
 public:
  enum class Type {MULTIPLY, DIVIDE};
  std::vector<unaryExpression> _children;
  auto evaluate() {
    if (_children.size() == 1) {
      return _chilren[0].evaluate();
    }
  }

};
class additiveExpression;

class unaryExpression
 */

auto equals = [](const auto& a, const auto& b) {return a == b;};
auto nequals = [](const auto& a, const auto& b) {return a != b;};

enum class RelationalExpressionEnum : char {
  EQUALS = 0, NEQUALS = 1, LESS, GREATER, LESS_EQUAL, GREATER_EQUAL
};
using RelationalTuple = std::tuple<decltype(equals), decltype(nequals), std::less<>, std::greater<>, std::less_equal<>, std::greater_equal<>>;

using RelationalExpression = DispatchedBinaryExpression<NumericValueGetter, RelationalTuple, RelationalExpressionEnum>;

enum class MultiplicativeExpressionEnum : char {
  MULTIPLY, DIVIDE
};
auto multiply = [] (const auto& a, const auto& b) {return a * b;};
auto divide = [] (const auto& a, const auto& b) {return a / b;};
using MultiplicationTuple = std::tuple<decltype(multiply), decltype(divide)>;
using MultiplicativeExpression = DispatchedBinaryExpression<NumericValueGetter, MultiplicationTuple , MultiplicativeExpressionEnum>;

auto add = [] (const auto& a, const auto& b) {return a + b;};
auto subtract = [] (const auto& a, const auto& b) {return a - b;};

enum class AdditiveEnum {ADD = 0, SUBTRACT};
using AdditiveTuple = std::tuple<decltype(add), decltype(subtract)>;
using AdditiveExpression = DispatchedBinaryExpression<NumericValueGetter, AdditiveTuple , AdditiveEnum>;


//auto x = LambdaVisitor<1>{}(LambdaTuple{}, RelationalExpressionEnum::EQUALS, 3ul, 4ul);


#endif  // QLEVER_SPARQLEXPRESSION_H
