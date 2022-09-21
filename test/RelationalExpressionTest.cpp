//  Copyright 2021, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include <limits>
#include <string>

#include "./IndexTestHelpers.h"
#include "./SparqlExpressionTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"
#include "gtest/gtest.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/Index.h"

using namespace sparqlExpression;
using ad_utility::source_location;
using namespace std::literals;
using enum valueIdComparators::Comparison;

// First some internal helper functions and constants.
namespace {
// Make a `ValueId` from an int. Shorter name, as it will be used often.
ValueId IntId(int64_t i) { return ValueId::makeFromInt(i); }

// Make a `ValueId` from a double. Shorter name, as it will be used often.
ValueId DoubleId(double d) { return ValueId::makeFromDouble(d); }

// Create an `Index`, the vocabulary of which contains the literals `"alpha",
// "älpha", "A", "Beta"`. The subjects and predicates of the triples are not
// important for these unit tests.
Index makeTestIndex() {
  std::string filename = "relationalExpressionTestIndex.ttl";
  std::string dummyKb =
      "<x> <label> \"alpha\" . <x> <label> \"älpha\" . <x> <label> \"A\" . <x> "
      "<label> \"Beta\".";

  FILE_BUFFER_SIZE() = 1000;
  std::fstream f(filename, std::ios_base::out);
  f << dummyKb;
  f.close();
  std::string indexBasename = "_relationalExpressionTestIndex";
  {
    Index index = makeIndexWithTestSettings();
    index.setOnDiskBase(indexBasename);
    index.createFromFile<TurtleParserAuto>(filename);
  }
  Index index;
  index.createFromOnDiskIndex(indexBasename);
  return index;
}

// Return a static  `QueryExecutionContext` that refers to an index that was
// build using `makeTestIndex()` (see above). The index (most notably its
// vocabulary) is the only part of the `QueryExecutionContext` that is actually
// relevant for these tests, so the other members are defaultet.
static QueryExecutionContext* getQec() {
  static ad_utility::AllocatorWithLimit<Id> alloc{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(100'000)};
  static const Index index = makeTestIndex();
  static const Engine engine{};
  static QueryResultCache cache{};
  static QueryExecutionContext qec{
      index,
      engine,
      &cache,
      ad_utility::AllocatorWithLimit<Id>{
          ad_utility::makeAllocationMemoryLeftThreadsafeObject(100'000)},
      {}};
  return &qec;
}

// Struct that stores a `sparqlExpression::EvaluationContext` and all the data
// structures that this context refers to. Most importantly it uses the
// `QueryExecutionContext` from `getQec()` (see above), and  has an `inputTable`
// that refers to a previous partial query result with multiple columns of
// various types. For details see the constructor.
struct TestContext {
  QueryExecutionContext* qec = getQec();
  sparqlExpression::VariableToColumnAndResultTypeMap varToColMap;
  ResultTable::LocalVocab localVocab;
  IdTable table{qec->getAllocator()};
  sparqlExpression::EvaluationContext context{*getQec(), varToColMap, table,
                                              qec->getAllocator(), localVocab};
  TestContext() {
    // First get some IDs for strings from the vocabulary to later reuse them
    Id alpha;
    Id aelpha;
    Id A;
    Id Beta;

    bool b = qec->getIndex().getId("\"alpha\"", &alpha);
    AD_CHECK(b);
    b = qec->getIndex().getId("\"älpha\"", &aelpha);
    AD_CHECK(b);
    b = qec->getIndex().getId("\"A\"", &A);
    AD_CHECK(b);
    b = qec->getIndex().getId("\"Beta\"", &Beta);
    AD_CHECK(b);

    // Set up the `table` that represents the previous partial query results. It
    // has five columns/variables: ?ints (only integers), ?doubles (only
    // doubles), ?numeric (int and double), ?vocab (only entries from the
    // vocabulary), ?mixed (all of the previous). None of the columns is sorted.
    table.setCols(5);
    // Order of the columns:
    // ?ints ?doubles ?numeric ?vocab ?mixed
    table.push_back({IntId(1), DoubleId(0.1), IntId(1), Beta, IntId(1)});
    table.push_back(
        {IntId(0), DoubleId(-.1), DoubleId(-.1), alpha, DoubleId(-.1)});
    table.push_back({IntId(-1), DoubleId(2.8), DoubleId(3.4), aelpha, A});

    context._beginIndex = 0;
    context._endIndex = table.size();
    // Define the mapping from variable names to column indices.
    varToColMap["?ints"] = {0, qlever::ResultType::KB};
    varToColMap["?doubles"] = {1, qlever::ResultType::KB};
    varToColMap["?numeric"] = {2, qlever::ResultType::KB};
    varToColMap["?vocab"] = {3, qlever::ResultType::KB};
    varToColMap["?mixed"] = {4, qlever::ResultType::KB};
  }
};

// A global allocator that will be used to setup test inputs.
ad_utility::AllocatorWithLimit<Id> allocator{
    ad_utility::makeAllocationMemoryLeftThreadsafeObject(100'000)};

// Convenient access to constants for "infinity" and "not a number". The
// spelling `NaN` was chosen because `nan` conflicts with the standard library.
const auto inf = std::numeric_limits<double>::infinity();
const auto NaN = std::numeric_limits<double>::quiet_NaN();

// Convert a vector of doubles into a vector of `ValueId`s that stores the
// values of the original vector.
VectorWithMemoryLimit<ValueId> makeValueIdVector(
    const VectorWithMemoryLimit<double>& vec) {
  VectorWithMemoryLimit<ValueId> result{allocator};
  for (double d : vec) {
    result.push_back(DoubleId(d));
  }
  return result;
}

// Same as the above function, but for `int64_t` instead of `double`.
VectorWithMemoryLimit<ValueId> makeValueIdVector(
    const VectorWithMemoryLimit<int64_t>& vec) {
  VectorWithMemoryLimit<ValueId> result{allocator};
  for (int64_t i : vec) {
    result.push_back(IntId(i));
  }
  return result;
}

// Create and return a `RelationalExpression` with the given Comparison and the
// given operands `leftValue` and `rightValue`.
template <valueIdComparators::Comparison comp>
auto makeExpression(SingleExpressionResult auto leftValue,
                    SingleExpressionResult auto rightValue) {
  auto leftChild = std::make_unique<DummyExpression>(std::move(leftValue));
  auto rightChild = std::make_unique<DummyExpression>(std::move(rightValue));

  return relational::RelationalExpression<comp>(
      {std::move(leftChild), std::move(rightChild)});
}

// Evaluate the given `expression` on a `TestContext` (see above).
auto evaluateOnTestContext = [](const SparqlExpression& expression) {
  TestContext context{};
  return expression.evaluate(&context.context);
};

// If `input` has a `.clone` member function, return the result of that
// function. Otherwise, return a copy of `input` (via the copy constructor).
// Used to generically create copies for `VectorWithMemoryLimit` (clone()
// method, but no copy constructor) and the other `SingleExpressionResult`s
// (which have a copy constructor, but no `.clone()` method. The name `makeCopy`
// was chosen because `clone` conflicts with the standard library.
auto makeCopy = [](const auto& input) {
  if constexpr (requires { input.clone(); }) {
    return input.clone();
  } else {
    return input;
  }
};

// Assert that the given `expression`, when evaluated on the `TestContext` (see
// above), has a single boolean result that is true.
auto expectTrueBoolean = [](const SparqlExpression& expression,
                            source_location l = source_location::current()) {
  auto trace = generateLocationTrace(l, "expectTrueBoolean was called here");
  auto result = evaluateOnTestContext(expression);
  EXPECT_TRUE(std::get<Bool>(result));
};

// Similar to `expectTrueBoolean`, but assert that the boolean is `false`.
auto expectFalseBoolean = [](const SparqlExpression& expression,
                             source_location l = source_location::current()) {
  auto trace = generateLocationTrace(l, "expectFalseBoolean was called here");
  auto result = evaluateOnTestContext(expression);
  EXPECT_FALSE(std::get<Bool>(result));
};

// Run tests for all the different comparisons on constants of type `T` and `U`
// (e.g. numeric or string constants). In the first pair `lessThanPair` the
// first entry must be less than the second. In `greaterThanPair` the first
// entry has to be greater than the second. In `equalPair`, the two entries have
// to be equalPair.
template <typename T, typename U>
auto testLessThanGreaterThanEqual(
    std::pair<T, U> lessThanPair, std::pair<T, U> greaterThanPair,
    std::pair<T, U> equalPair, source_location l = source_location::current()) {
  auto trace =
      generateLocationTrace(l, "testLessThanGreaterThanEqual was called here");
  auto True = expectTrueBoolean;
  auto False = expectFalseBoolean;

  True(makeExpression<LT>(lessThanPair.first, lessThanPair.second));
  True(makeExpression<LE>(lessThanPair.first, lessThanPair.second));
  False(makeExpression<EQ>(lessThanPair.first, lessThanPair.second));
  True(makeExpression<NE>(lessThanPair.first, lessThanPair.second));
  False(makeExpression<GE>(lessThanPair.first, lessThanPair.second));
  False(makeExpression<GT>(lessThanPair.first, lessThanPair.second));

  False(makeExpression<LT>(greaterThanPair.first, greaterThanPair.second));
  False(makeExpression<LE>(greaterThanPair.first, greaterThanPair.second));
  False(makeExpression<EQ>(greaterThanPair.first, greaterThanPair.second));
  True(makeExpression<NE>(greaterThanPair.first, greaterThanPair.second));
  True(makeExpression<GE>(greaterThanPair.first, greaterThanPair.second));
  True(makeExpression<GT>(greaterThanPair.first, greaterThanPair.second));

  False(makeExpression<LT>(equalPair.first, equalPair.second));
  True(makeExpression<LE>(equalPair.first, equalPair.second));
  True(makeExpression<EQ>(equalPair.first, equalPair.second));
  False(makeExpression<NE>(equalPair.first, equalPair.second));
  True(makeExpression<GE>(equalPair.first, equalPair.second));
  False(makeExpression<GT>(equalPair.first, equalPair.second));
}

// Test that all comparisons between `inputA` and `inputB` result in a single
// boolean that is false. The only exception is the `not equal` comparison, for
// which true is expected.
auto testNotEqual = [](auto inputA, auto inputB,
                       source_location l = source_location::current()) {
  auto trace = generateLocationTrace(l, "testNotEqual was called here");
  auto True = expectTrueBoolean;
  auto False = expectFalseBoolean;

  False(makeExpression<LT>(makeCopy(inputA), makeCopy(inputB)));
  False(makeExpression<LE>(makeCopy(inputA), makeCopy(inputB)));
  False(makeExpression<EQ>(makeCopy(inputA), makeCopy(inputB)));
  True(makeExpression<NE>(makeCopy(inputA), makeCopy(inputB)));
  False(makeExpression<GT>(makeCopy(inputA), makeCopy(inputB)));
  False(makeExpression<GE>(makeCopy(inputA), makeCopy(inputB)));

  False(makeExpression<LT>(makeCopy(inputB), makeCopy(inputA)));
  False(makeExpression<LE>(makeCopy(inputB), makeCopy(inputA)));
  False(makeExpression<EQ>(makeCopy(inputB), makeCopy(inputA)));
  True(makeExpression<NE>(makeCopy(inputB), makeCopy(inputA)));
  False(makeExpression<GT>(makeCopy(inputB), makeCopy(inputA)));
  False(makeExpression<GE>(makeCopy(inputB), makeCopy(inputA)));
};
}  // namespace

TEST(RelationalExpression, IntAndDouble) {
  testLessThanGreaterThanEqual<int64_t, double>({3, 3.3}, {4, -inf},
                                                {-12, -12.0});
  // Similar tests, but on the level of `ValueIds`.
  testLessThanGreaterThanEqual<ValueId, double>(
      {IntId(3), inf}, {IntId(4), -3.1}, {IntId(-12), -12.0});
  testLessThanGreaterThanEqual<int64_t, ValueId>(
      {3, DoubleId(inf)}, {4, DoubleId(-3.1)}, {-12, DoubleId(-12.0)});
  testLessThanGreaterThanEqual<ValueId, ValueId>({IntId(3), DoubleId(inf)},
                                                 {IntId(4), DoubleId(-3.1)},
                                                 {IntId(-12), DoubleId(-12.0)});

  testNotEqual(int64_t{3}, NaN);
  testNotEqual(int64_t{3}, DoubleId(NaN));
}

TEST(RelationalExpression, DoubleAndInt) {
  testLessThanGreaterThanEqual<double, int64_t>({3.1, 4}, {4.2, -3},
                                                {-12.0, -12});
  // Similar tests, but on the level of `ValueIds`.
  testLessThanGreaterThanEqual<double, ValueId>(
      {3.1, IntId(4)}, {4.2, IntId(-3)}, {-12.0, IntId(-12)});
  testLessThanGreaterThanEqual<ValueId, int64_t>(
      {DoubleId(3.1), 4}, {DoubleId(4.2), -3}, {DoubleId(-12.0), -12});
  testLessThanGreaterThanEqual<ValueId, ValueId>({DoubleId(3.1), IntId(4)},
                                                 {DoubleId(4.2), IntId(-3)},
                                                 {DoubleId(-12.0), IntId(-12)});

  testNotEqual(NaN, int64_t{42});
  testNotEqual(DoubleId(NaN), int64_t{42});
}

TEST(RelationalExpression, IntAndInt) {
  testLessThanGreaterThanEqual<int64_t, int64_t>({-3, 3}, {4, 3}, {-12, -12});
  // Similar tests, but on the level of `ValueIds`.
  testLessThanGreaterThanEqual<int64_t, ValueId>({-3, IntId(3)}, {4, IntId(3)},
                                                 {-12, IntId(-12)});
  testLessThanGreaterThanEqual<ValueId, int64_t>({IntId(-3), 3}, {IntId(4), 3},
                                                 {IntId(-12), -12});
  testLessThanGreaterThanEqual<ValueId, ValueId>(
      {IntId(-3), IntId(3)}, {IntId(4), IntId(3)}, {IntId(-12), IntId(-12)});
}

TEST(RelationalExpression, DoubleAndDouble) {
  testLessThanGreaterThanEqual<double, double>({-3.1, -3.0}, {4.2, 4.1},
                                               {-12.83, -12.83});
  // Similar tests, but on the level of `ValueIds`.
  testLessThanGreaterThanEqual<ValueId, double>(
      {DoubleId(-3.1), -3.0}, {DoubleId(4.2), 4.1}, {DoubleId(-12.83), -12.83});
  testLessThanGreaterThanEqual<double, ValueId>(
      {-3.1, DoubleId(-3.0)}, {4.2, DoubleId(4.1)}, {-12.83, DoubleId(-12.83)});
  testLessThanGreaterThanEqual<ValueId, ValueId>(
      {DoubleId(-3.1), DoubleId(-3.0)}, {DoubleId(4.2), DoubleId(4.1)},
      {DoubleId(-12.83), DoubleId(-12.83)});

  testNotEqual(NaN, NaN);
  testNotEqual(12.0, NaN);
  testNotEqual(NaN, -1.23e12);
  testNotEqual(DoubleId(NaN), -1.23e12);
  testNotEqual(NaN, DoubleId(-1.23e12));
}

TEST(RelationalExpression, StringAndString) {
  testLessThanGreaterThanEqual<std::string, std::string>(
      {"alpha", "beta"}, {"sigma", "delta"}, {"epsilon", "epsilon"});
  // TODO<joka921> These tests only work, when we actually use unicode
  // comparisons for the string based expressions.
  // TODO<joka921> Add an example for strings that are bytewise different but
  // equal on the unicode level (e.g.`ä` vs `a + dots`.
  // testLessThanGreaterThanEqual<std::string, std::string>({"Alpha", "beta"},
  // {"beta", "äpfel"}, {"xxx", "xxx"});
}

TEST(RelationalExpression, NumericAndStringAreNeverEqual) {
  auto stringVec = VectorWithMemoryLimit<std::string>({"hallo",
                                                       "by"
                                                       ""},
                                                      allocator);
  auto intVec = VectorWithMemoryLimit<int64_t>({-12365, 0, 12}, allocator);
  auto doubleVec =
      VectorWithMemoryLimit<double>({-12.365, 0, 12.1e5}, allocator);
  testNotEqual(int64_t{3}, "hallo"s);
  testNotEqual(int64_t{3}, "3"s);
  testNotEqual(-12.0, "hallo"s);
  testNotEqual(-12.0, "-12.0"s);
  testNotEqual(intVec.clone(), "someString"s);
  testNotEqual(doubleVec.clone(), "someString"s);
  testNotEqual(int64_t{3}, stringVec.clone());
  testNotEqual(intVec.clone(), stringVec.clone());
  testNotEqual(doubleVec.clone(), stringVec.clone());
}

// At least one of `leftValue`, `rightValue` must be a vector, the other one may
// be a constant or also a vector. The vectors must have 9 elements each. When
// comparing The first three must be less than leftValue, the next three greater
// than leftValue, and the last three equal to leftValue
// TODO<joka921> The name of the function and the arguments are out of sync.
// The function also works for two vectors.
template <typename T, typename U>
auto testLessThanGreaterThanEqualMultipleValues(
    T leftValue, U rightValue, source_location l = source_location::current()) {
  auto trace = generateLocationTrace(
      l, "testLessThanGreaterThanEqualMultipleValues was called here");

  TestContext testContext;
  sparqlExpression::EvaluationContext* context = &testContext.context;
  AD_CHECK(rightValue.size() == 9);
  context->_beginIndex = 0;
  context->_endIndex = 9;

  auto m = [&]<auto comp>() {
    auto expression =
        makeExpression<comp>(makeCopy(leftValue), makeCopy(rightValue));
    auto resultAsVariant = expression.evaluate(context);
    auto expressionInverted =
        makeExpression<comp>(makeCopy(rightValue), makeCopy(leftValue));
    auto resultAsVariantInverted = expressionInverted.evaluate(context);
    auto& result = std::get<VectorWithMemoryLimit<Bool>>(resultAsVariant);
    auto& resultInverted =
        std::get<VectorWithMemoryLimit<Bool>>(resultAsVariantInverted);
    return std::pair{std::move(result), std::move(resultInverted)};
  };
  auto [resultLT, invertedLT] = m.template operator()<LT>();
  auto [resultLE, invertedLE] = m.template operator()<LE>();
  auto [resultEQ, invertedEQ] = m.template operator()<EQ>();
  auto [resultNE, invertedNE] = m.template operator()<NE>();
  auto [resultGE, invertedGE] = m.template operator()<GE>();
  auto [resultGT, invertedGT] = m.template operator()<GT>();

  EXPECT_EQ(resultLT.size(), 9);
  EXPECT_EQ(resultLE.size(), 9);
  EXPECT_EQ(resultEQ.size(), 9);
  EXPECT_EQ(resultNE.size(), 9);
  EXPECT_EQ(resultGE.size(), 9);
  EXPECT_EQ(resultGT.size(), 9);
  for (size_t i = 0; i < 3; ++i) {
    EXPECT_FALSE(resultLT[i]);
    EXPECT_FALSE(resultLE[i]);
    EXPECT_FALSE(resultEQ[i]);
    EXPECT_TRUE(resultNE[i]);
    EXPECT_TRUE(resultGE[i]);
    EXPECT_TRUE(resultGT[i]);

    EXPECT_TRUE(invertedLT[i]);
    EXPECT_TRUE(invertedLE[i]);
    EXPECT_FALSE(invertedEQ[i]);
    EXPECT_TRUE(invertedNE[i]);
    EXPECT_FALSE(invertedGE[i]);
    EXPECT_FALSE(invertedGT[i]);
  }
  for (size_t i = 3; i < 6; ++i) {
    EXPECT_TRUE(resultLT[i]);
    EXPECT_TRUE(resultLE[i]);
    EXPECT_FALSE(resultEQ[i]);
    EXPECT_TRUE(resultNE[i]);
    EXPECT_FALSE(resultGE[i]);
    EXPECT_FALSE(resultGT[i]);

    EXPECT_FALSE(invertedLT[i]);
    EXPECT_FALSE(invertedLE[i]);
    EXPECT_FALSE(invertedEQ[i]);
    EXPECT_TRUE(invertedNE[i]);
    EXPECT_TRUE(invertedGE[i]);
    EXPECT_TRUE(invertedGT[i]);
  }
  for (size_t i = 6; i < 9; ++i) {
    EXPECT_FALSE(resultLT[i]);
    EXPECT_TRUE(resultLE[i]);
    EXPECT_TRUE(resultEQ[i]);
    EXPECT_FALSE(resultNE[i]);
    EXPECT_TRUE(resultGE[i]);
    EXPECT_FALSE(resultGT[i]);

    EXPECT_FALSE(invertedLT[i]);
    EXPECT_TRUE(invertedLE[i]);
    EXPECT_TRUE(invertedEQ[i]);
    EXPECT_FALSE(invertedNE[i]);
    EXPECT_TRUE(invertedGE[i]);
    EXPECT_FALSE(invertedGT[i]);
  }
}

// TODO<joka921> Comment
void testWithAndWithoutValueIds(
    SingleExpressionResult auto leftValue,
    SingleExpressionResult auto rightValue,
    source_location l = source_location::current()) {
  auto trace =
      generateLocationTrace(l, "testWithAndWithoutValueIds was called here");

  auto liftToValueId = []<typename T>(const T& t) {
    if constexpr (std::is_same_v<T, double>) {
      return DoubleId(t);
    } else if constexpr (std::is_integral_v<T>) {
      return IntId(t);
    } else if constexpr (isVectorResult<T>) {
      return makeValueIdVector(t);
    } else {
      static_assert(ad_utility::alwaysFalse<T>);
    }
  };

  testLessThanGreaterThanEqualMultipleValues(makeCopy(leftValue),
                                             makeCopy(rightValue));
  testLessThanGreaterThanEqualMultipleValues(liftToValueId(leftValue),
                                             makeCopy(rightValue));
  testLessThanGreaterThanEqualMultipleValues(makeCopy(leftValue),
                                             liftToValueId(rightValue));
  testLessThanGreaterThanEqualMultipleValues(liftToValueId(leftValue),
                                             liftToValueId(rightValue));
}

// At least one of `T` `U` must be a vector type. The vectors must all have 5
// elements. The corresponding pairs of values must all fulfill the "not equal"
// relation, but none of the other relations (less, less equal, equal  , greater
// equal, greater) must be true.
// TODO<joka921> arguments out of sync.
template <typename T, typename U>
auto testNotComparable(T constant, U vector,
                       source_location l = source_location::current()) {
  auto trace = generateLocationTrace(
      l, "testLessThanGreaterThanEqualMultipleValues was called here");
  ad_utility::AllocatorWithLimit<Id> alloc{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(1000)};
  sparqlExpression::VariableToColumnAndResultTypeMap map;
  ResultTable::LocalVocab localVocab;
  IdTable table{alloc};
  sparqlExpression::EvaluationContext context{*getQec(), map, table, alloc,
                                              localVocab};
  AD_CHECK(vector.size() == 5);
  context._beginIndex = 0;
  context._endIndex = 5;

  auto m = [&]<auto comp>() {
    auto expression =
        makeExpression<comp>(makeCopy(constant), makeCopy(vector));
    auto resultAsVariant = expression.evaluate(&context);
    auto expressionInverted =
        makeExpression<comp>(makeCopy(vector), makeCopy(constant));
    auto resultAsVariantInverted = expressionInverted.evaluate(&context);
    auto& result = std::get<VectorWithMemoryLimit<Bool>>(resultAsVariant);
    auto& resultInverted =
        std::get<VectorWithMemoryLimit<Bool>>(resultAsVariantInverted);
    return std::pair{std::move(result), std::move(resultInverted)};
  };
  auto [resultLT, invertedLT] = m.template operator()<LT>();
  auto [resultLE, invertedLE] = m.template operator()<LE>();
  auto [resultEQ, invertedEQ] = m.template operator()<EQ>();
  auto [resultNE, invertedNE] = m.template operator()<NE>();
  auto [resultGE, invertedGE] = m.template operator()<GE>();
  auto [resultGT, invertedGT] = m.template operator()<GT>();

  EXPECT_EQ(resultLT.size(), 5);
  EXPECT_EQ(resultLE.size(), 5);
  EXPECT_EQ(resultEQ.size(), 5);
  EXPECT_EQ(resultNE.size(), 5);
  EXPECT_EQ(resultGE.size(), 5);
  EXPECT_EQ(resultGT.size(), 5);
  for (size_t i = 0; i < 5; ++i) {
    EXPECT_FALSE(resultLT[i]);
    EXPECT_FALSE(resultLE[i]);
    EXPECT_FALSE(resultEQ[i]);
    EXPECT_TRUE(resultNE[i]);
    EXPECT_FALSE(resultGE[i]);
    EXPECT_FALSE(resultGT[i]);
    EXPECT_FALSE(invertedLT[i]);
    EXPECT_FALSE(invertedLE[i]);
    EXPECT_FALSE(invertedEQ[i]);
    EXPECT_TRUE(invertedNE[i]);
    EXPECT_FALSE(invertedGE[i]);
    EXPECT_FALSE(invertedGT[i]);
  }
}

TEST(RelationalExpression, NumericConstantAndNumericVector) {
  VectorWithMemoryLimit<double> doubles{
      {-24.3, 0.0, 3.0, 12.8, 1235e12, 523.13, 3.8, 3.8, 3.8}, allocator};
  testWithAndWithoutValueIds(3.8, doubles.clone());

  VectorWithMemoryLimit<int64_t> ints{{-523, -15, -3, -1, 0, 12305, -2, -2, -2},
                                      allocator};
  testWithAndWithoutValueIds(-2.0, ints.clone());
  testWithAndWithoutValueIds(int64_t{-2}, ints.clone());

  // Test cases for comparison with NaN
  VectorWithMemoryLimit<double> nonNans{{1.0, 2.0, -inf, inf, 12e6}, allocator};
  VectorWithMemoryLimit<double> Nans{{NaN, NaN, NaN, NaN, NaN}, allocator};
  VectorWithMemoryLimit<int64_t> fiveInts{{-1, 0, 1, 2, 3}, allocator};

  testNotComparable(NaN, nonNans.clone());
  testNotComparable(NaN, fiveInts.clone());
  testNotComparable(DoubleId(NaN), fiveInts.clone());
  testNotComparable(NaN, makeValueIdVector(fiveInts));
  testNotComparable(3.2, Nans.clone());
  testNotComparable(int64_t{-123}, Nans.clone());
  testNotComparable(NaN, Nans.clone());
}

TEST(RelationalExpression, StringConstantsAndStringVector) {
  VectorWithMemoryLimit<std::string> vec(
      {"alpha", "alpaka", "bertram", "sigma", "zeta", "kaulquappe", "caesar",
       "caesar", "caesar"},
      allocator);
  testLessThanGreaterThanEqualMultipleValues("caesar"s, vec.clone());

  // TODO<joka921> These tests only work, when we actually use unicode
  // comparisons for the string based expressions. TODDO<joka921> Add an example
  // for strings that are bytewise different but equal on the unicode level
  // (e.g.`ä` vs `a + dots`.
  // VectorWithMemoryLimit<std::string> vec2({"AlpHa", "älpaka", "Æ", "sigma",
  // "Eta", "kaulQuappe", "Caesar", "Caesar", "Caesare"}, alloc);
  // testLessThanGreaterThanEqual<std::string, std::string>({"Alpha", "beta"},
  // {"beta", "äpfel"}, {"xxx", "xxx"});
}

TEST(RelationalExpression, IntVectorAndIntVector) {
  VectorWithMemoryLimit<int64_t> vecA{
      {1065918, 17, 3, 1, 0, -102934, 2, -3120, 0}, allocator};
  VectorWithMemoryLimit<int64_t> vecB{
      {1065000, 16, -12340, 42, 1, -102930, 2, -3120, 0}, allocator};
  testWithAndWithoutValueIds(vecA.clone(), vecB.clone());
}

TEST(RelationalExpression, DoubleVectorAndDoubleVector) {
  VectorWithMemoryLimit<double> vecA{
      {1.1e12, 0.0, 3.120, -1230, -1.3e12, 1.2e-13, -0.0, 13, -1.2e6},
      allocator};
  VectorWithMemoryLimit<double> vecB{
      {1.0e12, -0.1, -3.120e5, 1230, -1.3e10, 1.1e-12, 0.0, 13, -1.2e6},
      allocator};
  testWithAndWithoutValueIds(vecA.clone(), vecB.clone());

  VectorWithMemoryLimit<double> nanA{{NaN, 1.0, NaN, 3.2, NaN}, allocator};
  VectorWithMemoryLimit<double> nanB{{-12.3, NaN, 0.0, NaN, inf}, allocator};
  testNotComparable(nanA.clone(), nanB.clone());
}

TEST(RelationalExpression, DoubleVectorAndIntVector) {
  VectorWithMemoryLimit<int64_t> vecA{
      {1065918, 17, 3, 1, 0, -102934, 2, -3120, 0}, allocator};
  VectorWithMemoryLimit<double> vecB{{1065917.9, 1.5e1, -3.120e5, 1.0e1, 1e-12,
                                      -102934e-1, 20.0e-1, -3.120e3, -0.0},
                                     allocator};
  testWithAndWithoutValueIds(vecA.clone(), vecB.clone());

  VectorWithMemoryLimit<int64_t> fiveInts{{0, 1, 2, 3, -4}, allocator};
  VectorWithMemoryLimit<double> fiveNans{{NaN, NaN, NaN, NaN, NaN}, allocator};
  testNotComparable(fiveInts.clone(), fiveNans.clone());

  // TODO<joka921> Consistently also test `includingValueIds` for the `NaN`
  // cases.
}

TEST(RelationalExpression, StringVectorAndStringVector) {
  VectorWithMemoryLimit<std::string> vecA{
      {"alpha", "beta", "g", "epsilon", "fraud", "capitalism", "", "bo'sä30",
       "Me"},
      allocator};
  VectorWithMemoryLimit<std::string> vecB{
      {"alph", "alpha", "f", "epsiloo", "freud", "communism", "", "bo'sä30",
       "Me"},
      allocator};
  testLessThanGreaterThanEqualMultipleValues(vecA.clone(), vecB.clone());
  // TODO<joka921> Add a test case for correct unicode collation as soon as that
  // is actually supported.
}

TEST(RelationalExpression, VariableAndConstant) {
  TestContext ctx;

  auto test = [&](const auto& expression, std::vector<Bool> expected,
                  source_location l = source_location::current()) {
    auto trace = generateLocationTrace(l, "test lambda was called here");
    auto resultAsVariant = expression.evaluate(&ctx.context);
    const auto& result = std::get<VectorWithMemoryLimit<Bool>>(resultAsVariant);
    // TODO<joka921> There is a matcher for that (ElementsAre or something like
    // that).
    ASSERT_EQ(result.size(), expected.size());
    for (size_t i = 0; i < result.size(); ++i) {
      EXPECT_EQ(expected[i], result[i]);
    }
  };

  // ?ints column is `1, 0, -1`
  test(makeExpression<LT>(int64_t{0}, Variable{"?ints"}), {true, false, false});
  test(makeExpression<GE>(-0.0, Variable{"?ints"}), {false, true, true});
  test(makeExpression<GE>(Variable{"?ints"}, IntId(0)), {true, true, false});

  // ?doubles column is `0.1, -0.1, 2.8`
  test(makeExpression<LT>(-3.5, Variable{"?doubles"}), {true, true, true});
  test(makeExpression<EQ>(DoubleId(-0.1), Variable{"?doubles"}),
       {false, true, false});
  test(makeExpression<GT>(Variable{"?doubles"}, int64_t{0}),
       {true, false, true});

  // ?numeric column is 1, -0.1, 3.4
  test(makeExpression<NE>(-0.1, Variable{"?numeric"}), {true, false, true});
  test(makeExpression<GE>(Variable{"?numeric"}, int64_t{1}),
       {true, false, true});
  test(makeExpression<GT>(Variable{"?numeric"}, DoubleId(1.2)),
       {false, false, true});

  // ?vocab column is `"Beta", "alpha", "älpha"
  test(makeExpression<LE>(Variable{"?vocab"}, "\"älpha\""s),
       {false, true, true});
  test(makeExpression<GT>(Variable{"?vocab"}, "\"alpha\""s),
       {true, false, true});
  test(makeExpression<LT>("\"atm\""s, Variable{"?vocab"}),
       {true, false, false});

  // ?mixed column is `1, -0.1, A`
  test(makeExpression<GT>("\"atm\""s, Variable{"?mixed"}),
       {false, false, true});
  test(makeExpression<LT>("\"atm\""s, Variable{"?mixed"}),
       {false, false, false});

  test(makeExpression<NE>(int64_t{1}, Variable{"?mixed"}),
       {false, true, true});
  test(makeExpression<GE>(Variable{"?mixed"}, DoubleId(-0.1)),
       {true, true, false});
}

// TODO<joka921> test for "Variable + constant" (binary search case)

// TODO<joka921> Test for "Variable + Vector" or "Variable + Variable"
