//
// Created by johannes on 09.05.21.
//

#ifndef QLEVER_SPARQLEXPRESSION_H
#define QLEVER_SPARQLEXPRESSION_H

#include <vector>
#include <memory>
#include <variant>
#include "../engine/ResultTable.h"
#include "../global/Id.h"


struct evaluationInput {
  size_t _inputSize;
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

bool getBoolValue(StrongId id, evaluationInput*); // Todo: implement



// Result type for certain boolean expression: all the values in the intervals are "true"
using Ranges = std::vector<std::pair<size_t, size_t>>;

// when we have two sets of ranges, get their union
Ranges mergeRanges(const Ranges& a, const Ranges& b);
Ranges intersectRanges(const Ranges& a, const Ranges& b);

//Transform a range into a std::vector<bool>, we have to be given the maximal
// size of an interval
std::vector<bool> expandRanges(const Ranges& a, size_t targetSize);

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

class conditionalOrExpression : public SparqlExpression {
 public:
  conditionalOrExpression(std::vector<Ptr> children): _children{std::move(children)} {};
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
