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

namespace sparqlExpression {
struct conststr
{
  static constexpr size_t MaxSize = 8;
  char p[MaxSize] = {0};
  std::size_t sz = 0;
 public:
  template<std::size_t N>
  constexpr conststr(const char(&a)[N]) : sz(N - 1) {
    if (N > MaxSize) {
      throw std::runtime_error{"conststr can only be constructed from strings with a maximum size of " + std::to_string(MaxSize - 1)};
    }
    // TODO: enforce proper zero-termination
    for (size_t i = 0; i < N; ++i) {
      p[i] = a[i];
    }
  }

  conststr(std::string_view input) {
    if (input.size() >= MaxSize) {
      throw std::runtime_error{"conststr can only be constructed from strings with a maximum size of " + std::to_string(MaxSize - 1)};
    }
    for (size_t i = 0; i < input.size(); ++i) {
      p[i] = input[i];
    }
  }

  constexpr char operator[](std::size_t n) const
  {
    return n < sz ? p[n] : throw std::out_of_range("");
  }
  constexpr std::size_t size() const { return sz; }

  bool operator==(const conststr& rhs) const {
    return !std::strcmp(p, rhs.p);
  }
  
};

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

using VariableColumnMap = ad_utility::HashMap<std::string, size_t>;

struct evaluationInput {
  const QueryExecutionContext* _qec;
  IdTable::const_iterator _begin;
  IdTable::const_iterator _end;
};



inline double getNumericValueFromVariable(const Variable& variable, size_t i, evaluationInput* input) {
  // TODO<joka921>: Optimization for the IdTableStatic.
  const auto& id = (*(input->_begin + i))[variable._columnIndex];
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
      _value._columnIndex = variableColumnMap.at(_value._variable);
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
auto liftBinaryCalculationToEvaluateResults(RangeCalculation rangeCalculation,
                                            ValueExtractor valueExtractor,
                                            BinaryOperation binaryOperation,
                                            evaluationInput* input) {
  return [input, rangeCalculation, valueExtractor,
          binaryOperation](auto&&... args) -> SparqlExpression::EvaluateResult {
    if constexpr (RangeCalculationAllowed &&
                  (... &&
                   std::is_same_v<std::decay_t<decltype(args)>, Ranges>)) {
      return rangeCalculation(std::forward<decltype(args)>(args)...);
    } else {
      auto getSize = [&input](const auto& x) -> std::pair<size_t, bool> {
        using T = std::decay_t<decltype(x)>;
        if constexpr (isVector<T>) {
          return {x.size(), true};
        } else if constexpr (std::is_same_v<Ranges, T>) {
          return {input->_end - input->_begin, true};
        } else {
          return {1, false};
        }
      };

      auto targetSize = std::max({getSize(args).first...});

      // If any of the inputs is a list/vector/range of results, the result will be a std::vector
      bool resultIsAVector = (... || getSize(args).second);

      auto extractors =
          std::make_tuple([expanded = expandRanges(
                               std::forward<decltype(args)>(args), targetSize),
                           &valueExtractor, input](size_t index) {
            using A = std::decay_t<decltype(expanded)>;
            if constexpr (isVector<A>) {
              return valueExtractor(expanded[index], input);
            } else if constexpr (std::is_same_v<Variable, A>) {
              return valueExtractor(getNumericValueFromVariable(expanded,index, input), input);
            } else {
              return valueExtractor(expanded, input);
            }
          }...);

      auto create = [&](auto&&... extractors) {
        using ResultType =
            std::decay_t<decltype(binaryOperation(extractors(0)...))>;
        std::vector<ResultType> result;
        result.reserve(targetSize);
        for (size_t i = 0; i < targetSize; ++i) {
          result.push_back(binaryOperation(extractors(i)...));
        }
        return result;
      };

      auto result = std::apply(create, std::move(extractors));

      if (!resultIsAVector) {
        return SparqlExpression::EvaluateResult {std::move(result[0])};
      } else {
        return SparqlExpression::EvaluateResult(std::move(result));
      }
    }
  };
}

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


template <typename ValueExtractor, typename BinaryOperationTuple,
          typename RelationDispatchEnum>
class DispatchedBinaryExpression : public SparqlExpression {
 public:
  DispatchedBinaryExpression(std::vector<Ptr>&& children,
                             std::vector<RelationDispatchEnum>&& relations)
      : _children{std::move(children)}, _relations{std::move(relations)} {};
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
  std::vector<RelationDispatchEnum> _relations;
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

inline auto equals = [](const auto& a, const auto& b) -> bool { return a == b; };
inline auto nequals = [](const auto& a, const auto& b) ->bool { return a != b; };

enum class RelationalExpressionEnum : char {
  EQUALS = 0,
  NEQUALS = 1,
  LESS,
  GREATER,
  LESS_EQUAL,
  GREATER_EQUAL
};
using RelationalTuple =
    std::tuple<decltype(equals), decltype(nequals), std::less<>, std::greater<>,
               std::less_equal<>, std::greater_equal<>>;

using RelationalExpression =
    DispatchedBinaryExpression<NumericValueGetter, RelationalTuple,
                               RelationalExpressionEnum>;

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
