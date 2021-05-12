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


struct evaluationInput {
  size_t _inputSize;
  QueryExecutionContext* _qec;
};

struct StrongId {
  Id _value;
};

inline double getNumericValue(double v, evaluationInput*) { return v;}
inline int64_t getNumericValue(int64_t v, evaluationInput*) { return v;}
inline bool getNumericValue(bool v, evaluationInput*) { return v;}

inline double getNumericValue(StrongId id, evaluationInput*); // Todo: implement

inline bool getBoolValue(double v, evaluationInput*) { return v;}
inline bool getBoolValue(int64_t v, evaluationInput*) { return v;}
inline bool getBoolValue(bool v, evaluationInput*) { return v;}

inline bool getBoolValue(StrongId id, evaluationInput* input) {
  // load the string, parse it as an xsd::int or float
  auto optional =
      input->_qec->getIndex().idToOptionalString(id._value);
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
}// Todo: implement



// Result type for certain boolean expression: all the values in the intervals are "true"
using Ranges = std::vector<std::pair<size_t, size_t>>;

// when we have two sets of ranges, get their union
Ranges mergeRanges(Ranges rangesA, Ranges rangesB) {
  std::sort(rangesA.begin(), rangesA.end(), [](const auto&a, const auto& b) {return a.first < b.first;});
  std::sort(rangesB.begin(), rangesB.end(), [](const auto&a, const auto& b) {return a.first < b.first;});

  Ranges result;
  auto iteratorA = rangesA.begin();
  auto iteratorB = rangesB.begin();

  while (iteratorA < rangesA.end() && iteratorB < rangesB.end()) {
    auto& itSmaller =
        iteratorA->first < iteratorB->first ? iteratorA : iteratorB;
    auto& itGreaterEq =
        iteratorA->first < iteratorB->first ? iteratorB : iteratorA;
    if (itSmaller->second < itGreaterEq->first) {
      // the ranges do not overlap, simply append the smaller one
      result.push_back(*itSmaller);
    } else {
      // the ranges overlap
      result.emplace_back(itSmaller->first, itGreaterEq->second);
    }
    ++itSmaller;
  }

  // Attach the remainder
  result.insert(result.end(), iteratorA, rangesA.end());
  result.insert(result.end(), iteratorB, rangesB.end());
  return result;
}

Ranges intersectRanges(const Ranges& a, const Ranges& b);

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
auto expandRanges(T&& a, size_t targetSize) {
    AD_CHECK(a.size() == targetSize);
    return std::forward<T>(a);
}

// calculate the boolean negation of a range.
Ranges negateRanges(const Ranges& a, size_t targetSize);

// Class for a completely invalid result
class InvalidResult{};

class SparqlExpression {
 public:
  using EvaluateResult = std::variant<std::vector<StrongId>, std::vector<double>, std::vector<int64_t>, std::vector<bool>, Ranges>;
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
      AD_CHECK(bExpand.size() == bExpand.size());
      std::vector<bool> result;
      result.reserve(aExpand.size());
      for (size_t i = 0; i < aExpand.size(); ++i) {
        result.push_back(binaryOperation(valueExtractor(aExpand[i], input),
                                         valueExtractor(bExpand[i], input)));
      }
      return result;
    }
  };

}

class ConditionalOrExpression : public SparqlExpression {
 public:
  ConditionalOrExpression(std::vector<Ptr> children): _children{std::move(children)} {};
  EvaluateResult  evaluate(evaluationInput* input) const override {
      auto firstResult = _children[0]->evaluate(input);
      auto calculator = [input](const auto& a, const auto& b) -> EvaluateResult {
        using A = std::decay_t<decltype(a)>;
        using B = std::decay_t<decltype(b)>;
        if constexpr (std::is_same_v<A, Ranges> && std::is_same_v<B, Ranges>) {
          return mergeRanges(a, b);
        }
        auto aExpand = expandRanges(std::move(a), input->_inputSize);
        auto bExpand = expandRanges(std::move(b), input->_inputSize);
        AD_CHECK(bExpand.size() == bExpand.size());
        std::vector<bool> result;
        result.reserve(aExpand.size());
        for (size_t i = 0; i < aExpand.size(); ++i) {
          result.push_back(getBoolValue(aExpand[i], input) || getBoolValue(bExpand[i], input));
        }
        return result;
      };
      for (size_t i = 1; i < _children.size(); ++i) {
        firstResult = std::visit(calculator, firstResult, _children[i]->evaluate(input));
      }
    return firstResult;

  };
 private:
  std::vector<Ptr> _children;
};

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



#endif  // QLEVER_SPARQLEXPRESSION_H
