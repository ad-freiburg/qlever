//  Copyright 2021, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include <limits>
#include <string>

#include "./IndexTestHelpers.h"
#include "./SparqlParserTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"
#include "gtest/gtest.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/Index.h"

using namespace sparqlExpression;
using ad_utility::source_location;
using namespace std::literals;

using enum valueIdComparators::Comparison;

namespace {

ValueId Int(int64_t i) { return ValueId::makeFromInt(i); }

ValueId Double(double d) { return ValueId::makeFromDouble(d); }

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

struct TestContext {
  QueryExecutionContext* qec = getQec();
  sparqlExpression::VariableToColumnAndResultTypeMap map;
  ResultTable::LocalVocab localVocab;
  IdTable table{qec->getAllocator()};
  sparqlExpression::EvaluationContext context{*getQec(), map, table,
                                              qec->getAllocator(), localVocab};
  TestContext() {
    // TODO<joka921> Add tests for the local vocab.
    // ?ints ?doubles ?numeric ?vocab ?mixed
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

    table.setCols(5);
    table.push_back({Int(1), Double(0.1), Int(1), Beta, Int(1)});
    table.push_back({Int(0), Double(-.1), Double(-.1), alpha, Double(-.1)});
    table.push_back({Int(-1), Double(2.8), Double(3.4), aelpha, A});

    context._beginIndex = 0;
    context._endIndex = table.size();
    map["?ints"] = {0, qlever::ResultType::KB};
    map["?doubles"] = {1, qlever::ResultType::KB};
    map["?numeric"] = {2, qlever::ResultType::KB};
    map["?vocab"] = {3, qlever::ResultType::KB};
    map["?mixed"] = {4, qlever::ResultType::KB};
  }
};

ad_utility::AllocatorWithLimit<Id> allocator{
    ad_utility::makeAllocationMemoryLeftThreadsafeObject(100'000)};

const auto inf = std::numeric_limits<double>::infinity();
const auto NaN = std::numeric_limits<double>::quiet_NaN();

VectorWithMemoryLimit<ValueId> makeValueIdVector(
    const VectorWithMemoryLimit<double>& vec) {
  VectorWithMemoryLimit<ValueId> result{allocator};
  for (double d : vec) {
    result.push_back(Double(d));
  }
  return result;
}

VectorWithMemoryLimit<ValueId> makeValueIdVector(
    const VectorWithMemoryLimit<int64_t>& vec) {
  VectorWithMemoryLimit<ValueId> result{allocator};
  for (int64_t i : vec) {
    result.push_back(Int(i));
  }
  return result;
}

}  // namespace

template <valueIdComparators::Comparison comp>
auto makeExpression(auto l, auto r) {
  auto leftChild = std::make_unique<DummyExpression>(std::move(l));
  auto rightChild = std::make_unique<DummyExpression>(std::move(r));

  return relational::RelationalExpression<comp>(
      {std::move(leftChild), std::move(rightChild)});
}

auto evaluateWithEmpyContext = [](const SparqlExpression& expression) {
  sparqlExpression::VariableToColumnAndResultTypeMap map;
  ResultTable::LocalVocab localVocab;
  IdTable table{allocator};
  sparqlExpression::EvaluationContext context{*getQec(), map, table, allocator,
                                              localVocab};

  return expression.evaluate(&context);
};

auto expectTrueBoolean = [](const SparqlExpression& expression,
                            source_location l = source_location::current()) {
  auto trace = generateLocationTrace(l, "expectTrueBoolean was called here");
  auto result = evaluateWithEmpyContext(expression);
  EXPECT_TRUE(std::get<Bool>(result));
};

auto expectFalseBoolean = [](const SparqlExpression& expression,
                             source_location l = source_location::current()) {
  auto trace = generateLocationTrace(l, "expectFalseBoolean was called here");
  auto result = evaluateWithEmpyContext(expression);
  EXPECT_FALSE(std::get<Bool>(result));
};

template <typename T, typename U>
auto testNumericConstants(std::pair<T, U> lessThan, std::pair<T, U> greaterThan,
                          std::pair<T, U> equal,
                          source_location l = source_location::current()) {
  auto trace = generateLocationTrace(l, "testNumericConstants was called here");
  auto True = expectTrueBoolean;
  auto False = expectFalseBoolean;

  True(makeExpression<LT>(lessThan.first, lessThan.second));
  True(makeExpression<LE>(lessThan.first, lessThan.second));
  False(makeExpression<EQ>(lessThan.first, lessThan.second));
  True(makeExpression<NE>(lessThan.first, lessThan.second));
  False(makeExpression<GE>(lessThan.first, lessThan.second));
  False(makeExpression<GT>(lessThan.first, lessThan.second));

  False(makeExpression<LT>(greaterThan.first, greaterThan.second));
  False(makeExpression<LE>(greaterThan.first, greaterThan.second));
  False(makeExpression<EQ>(greaterThan.first, greaterThan.second));
  True(makeExpression<NE>(greaterThan.first, greaterThan.second));
  True(makeExpression<GE>(greaterThan.first, greaterThan.second));
  True(makeExpression<GT>(greaterThan.first, greaterThan.second));

  False(makeExpression<LT>(equal.first, equal.second));
  True(makeExpression<LE>(equal.first, equal.second));
  True(makeExpression<EQ>(equal.first, equal.second));
  False(makeExpression<NE>(equal.first, equal.second));
  True(makeExpression<GE>(equal.first, equal.second));
  False(makeExpression<GT>(equal.first, equal.second));
}

// TODO<joka921> Misnomer, it is "notComparable, constant result", rename and
// comment.
auto testNumericAndString = [](auto numeric, auto s,
                               source_location l = source_location::current()) {
  auto trace = generateLocationTrace(l, "testNumericAndString was called here");
  auto True = expectTrueBoolean;
  auto False = expectFalseBoolean;

  auto clone = [](const auto& input) {
    if constexpr (requires { input.clone(); }) {
      return input.clone();
    } else {
      return input;
    }
  };

  False(makeExpression<LT>(clone(numeric), clone(s)));
  False(makeExpression<LE>(clone(numeric), clone(s)));
  False(makeExpression<EQ>(clone(numeric), clone(s)));
  True(makeExpression<NE>(clone(numeric), clone(s)));
  False(makeExpression<GT>(clone(numeric), clone(s)));
  False(makeExpression<GE>(clone(numeric), clone(s)));

  False(makeExpression<LT>(clone(s), clone(numeric)));
  False(makeExpression<LE>(clone(s), clone(numeric)));
  False(makeExpression<EQ>(clone(s), clone(numeric)));
  True(makeExpression<NE>(clone(s), clone(numeric)));
  False(makeExpression<GT>(clone(s), clone(numeric)));
  False(makeExpression<GE>(clone(s), clone(numeric)));
};
TEST(RelationalExpression, IntAndDouble) {
  testNumericConstants<int, double>({3, 3.3}, {4, -inf}, {-12, -12.0});
  // Similar tests, but on the level of `ValueIds`.
  testNumericConstants<ValueId, double>({Int(3), inf}, {Int(4), -3.1},
                                        {Int(-12), -12.0});
  testNumericConstants<int, ValueId>({3, Double(inf)}, {4, Double(-3.1)},
                                     {-12, Double(-12.0)});
  testNumericConstants<ValueId, ValueId>(
      {Int(3), Double(inf)}, {Int(4), Double(-3.1)}, {Int(-12), Double(-12.0)});

  testNumericAndString(3, NaN);
  testNumericAndString(3, Double(NaN));
}

TEST(RelationalExpression, DoubleAndInt) {
  testNumericConstants<double, int>({3.1, 4}, {4.2, -3}, {-12.0, -12});
  // Similar tests, but on the level of `ValueIds`.
  testNumericConstants<double, ValueId>({3.1, Int(4)}, {4.2, Int(-3)},
                                        {-12.0, Int(-12)});
  testNumericConstants<ValueId, int>({Double(3.1), 4}, {Double(4.2), -3},
                                     {Double(-12.0), -12});
  testNumericConstants<ValueId, ValueId>(
      {Double(3.1), Int(4)}, {Double(4.2), Int(-3)}, {Double(-12.0), Int(-12)});

  testNumericAndString(NaN, 42);
  testNumericAndString(Double(NaN), 42);
}

TEST(RelationalExpression, IntAndInt) {
  testNumericConstants<int, int>({-3, 3}, {4, 3}, {-12, -12});
  // Similar tests, but on the level of `ValueIds`.
  testNumericConstants<int, ValueId>({-3, Int(3)}, {4, Int(3)},
                                     {-12, Int(-12)});
  testNumericConstants<ValueId, int>({Int(-3), 3}, {Int(4), 3},
                                     {Int(-12), -12});
  testNumericConstants<ValueId, ValueId>({Int(-3), Int(3)}, {Int(4), Int(3)},
                                         {Int(-12), Int(-12)});
}

TEST(RelationalExpression, DoubleAndDouble) {
  testNumericConstants<double, double>({-3.1, -3.0}, {4.2, 4.1},
                                       {-12.83, -12.83});
  // Similar tests, but on the level of `ValueIds`.
  testNumericConstants<ValueId, double>(
      {Double(-3.1), -3.0}, {Double(4.2), 4.1}, {Double(-12.83), -12.83});
  testNumericConstants<double, ValueId>(
      {-3.1, Double(-3.0)}, {4.2, Double(4.1)}, {-12.83, Double(-12.83)});
  testNumericConstants<ValueId, ValueId>({Double(-3.1), Double(-3.0)},
                                         {Double(4.2), Double(4.1)},
                                         {Double(-12.83), Double(-12.83)});

  testNumericAndString(NaN, NaN);
  testNumericAndString(12.0, NaN);
  testNumericAndString(NaN, -1.23e12);
  testNumericAndString(Double(NaN), -1.23e12);
  testNumericAndString(NaN, Double(-1.23e12));
}

TEST(RelationalExpression, StringAndString) {
  testNumericConstants<std::string, std::string>(
      {"alpha", "beta"}, {"sigma", "delta"}, {"epsilon", "epsilon"});
  // TODO<joka921> These tests only work, when we actually use unicode
  // comparisons for the string based expressions. TODDO<joka921> Add an example
  // for strings that are bytewise different but equal on the unicode level
  // (e.g.`ä` vs `a + dots`.
  // testNumericConstants<std::string, std::string>({"Alpha", "beta"}, {"beta",
  // "äpfel"}, {"xxx", "xxx"});

  // TODO<joka921> To compare strings with `ValueId`s we actually need a context
  // with vocabulary etc. which has yet to be mocked.
}

TEST(RelationalExpression, NumericAndStringAreNeverEqual) {
  auto stringVec = VectorWithMemoryLimit<std::string>({"hallo",
                                                       "by"
                                                       ""},
                                                      allocator);
  auto intVec = VectorWithMemoryLimit<int64_t>({-12365, 0, 12}, allocator);
  auto doubleVec =
      VectorWithMemoryLimit<double>({-12.365, 0, 12.1e5}, allocator);
  testNumericAndString(3, "hallo"s);
  testNumericAndString(3, "3"s);
  testNumericAndString(-12.0, "hallo"s);
  testNumericAndString(-12.0, "-12.0"s);
  testNumericAndString(intVec.clone(), "someString"s);
  testNumericAndString(doubleVec.clone(), "someString"s);
  testNumericAndString(3, stringVec.clone());
  testNumericAndString(intVec.clone(), stringVec.clone());
  testNumericAndString(doubleVec.clone(), stringVec.clone());
}

// The vector must consist of 9 elements: The first three must be less than
// constant, the next three greater than constant, and the last three equal to
// constant
// TODO<joka921> The name of the function and the arguments are out of sync.
// The function also works for two vectors.
template <typename T, typename U>
auto testNumericConstantAndVector(
    T constant, U vector, source_location l = source_location::current()) {
  auto trace =
      generateLocationTrace(l, "testNumericConstantAndVector was called here");
  ad_utility::AllocatorWithLimit<Id> alloc{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(1000)};
  sparqlExpression::VariableToColumnAndResultTypeMap map;
  ResultTable::LocalVocab localVocab;
  IdTable table{alloc};
  sparqlExpression::EvaluationContext context{*getQec(), map, table, alloc,
                                              localVocab};
  AD_CHECK(vector.size() == 9);
  context._beginIndex = 0;
  context._endIndex = 9;

  auto clone = [](const auto& input) {
    if constexpr (requires { input.clone(); }) {
      return input.clone();
    } else {
      return input;
    }
  };

  auto m = [&]<auto comp>() {
    auto expression = makeExpression<comp>(clone(constant), clone(vector));
    auto resultAsVariant = expression.evaluate(&context);
    auto expressionInverted =
        makeExpression<comp>(clone(vector), clone(constant));
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

template <typename A, typename B>
void testIncludingValueIds(A a, B b,
                           source_location l = source_location::current()) {
  auto trace =
      generateLocationTrace(l, "testIncludingValueIds was called here");

  auto lift = []<typename T>(const T& t) {
    if constexpr (std::is_same_v<T, double>) {
      return Double(t);
    } else if constexpr (std::is_integral_v<T>) {
      return Int(t);
    } else if constexpr (isVectorResult<T>) {
      return makeValueIdVector(t);
    } else {
      static_assert(ad_utility::alwaysFalse<T>);
    }
  };

  auto clone = [](const auto& input) {
    if constexpr (requires { input.clone(); }) {
      return input.clone();
    } else {
      return input;
    }
  };

  testNumericConstantAndVector(clone(a), clone(b));
  testNumericConstantAndVector(lift(a), clone(b));
  testNumericConstantAndVector(clone(a), lift(b));
  testNumericConstantAndVector(lift(a), lift(b));
}

// At least one of `T` `U` must be a vector type. The vectors must all have 5
// elements. The corresponding pairs of values must all fulfill the "not equal"
// relation, but none of the other relations must be true.
template <typename T, typename U>
auto testNotComparable(T constant, U vector,
                       source_location l = source_location::current()) {
  auto trace =
      generateLocationTrace(l, "testNumericConstantAndVector was called here");
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

  auto clone = [](const auto& input) {
    if constexpr (requires { input.clone(); }) {
      return input.clone();
    } else {
      return input;
    }
  };

  auto m = [&]<auto comp>() {
    auto expression = makeExpression<comp>(clone(constant), clone(vector));
    auto resultAsVariant = expression.evaluate(&context);
    auto expressionInverted =
        makeExpression<comp>(clone(vector), clone(constant));
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
  testIncludingValueIds(3.8, doubles.clone());

  VectorWithMemoryLimit<int64_t> ints{{-523, -15, -3, -1, 0, 12305, -2, -2, -2},
                                      allocator};
  testIncludingValueIds(-2.0, ints.clone());
  testIncludingValueIds(int64_t{-2}, ints.clone());

  // Test cases for comparison with NaN
  VectorWithMemoryLimit<double> nonNans{{1.0, 2.0, -inf, inf, 12e6}, allocator};
  VectorWithMemoryLimit<double> Nans{{NaN, NaN, NaN, NaN, NaN}, allocator};
  VectorWithMemoryLimit<int64_t> fiveInts{{-1, 0, 1, 2, 3}, allocator};

  testNotComparable(NaN, nonNans.clone());
  testNotComparable(NaN, fiveInts.clone());
  testNotComparable(Double(NaN), fiveInts.clone());
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
  testNumericConstantAndVector("caesar"s, vec.clone());

  // TODO<joka921> These tests only work, when we actually use unicode
  // comparisons for the string based expressions. TODDO<joka921> Add an example
  // for strings that are bytewise different but equal on the unicode level
  // (e.g.`ä` vs `a + dots`.
  // VectorWithMemoryLimit<std::string> vec2({"AlpHa", "älpaka", "Æ", "sigma",
  // "Eta", "kaulQuappe", "Caesar", "Caesar", "Caesare"}, alloc);
  // testNumericConstants<std::string, std::string>({"Alpha", "beta"}, {"beta",
  // "äpfel"}, {"xxx", "xxx"});

  // TODO<joka921> Add tests for Strings and ValueIds as soon as we have a
  // mocked Vocabulary.
}

TEST(RelationalExpression, IntVectorAndIntVector) {
  VectorWithMemoryLimit<int64_t> vecA{
      {1065918, 17, 3, 1, 0, -102934, 2, -3120, 0}, allocator};
  VectorWithMemoryLimit<int64_t> vecB{
      {1065000, 16, -12340, 42, 1, -102930, 2, -3120, 0}, allocator};
  testIncludingValueIds(vecA.clone(), vecB.clone());
}

TEST(RelationalExpression, DoubleVectorAndDoubleVector) {
  VectorWithMemoryLimit<double> vecA{
      {1.1e12, 0.0, 3.120, -1230, -1.3e12, 1.2e-13, -0.0, 13, -1.2e6},
      allocator};
  VectorWithMemoryLimit<double> vecB{
      {1.0e12, -0.1, -3.120e5, 1230, -1.3e10, 1.1e-12, 0.0, 13, -1.2e6},
      allocator};
  testIncludingValueIds(vecA.clone(), vecB.clone());

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
  testIncludingValueIds(vecA.clone(), vecB.clone());

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
  testNumericConstantAndVector(vecA.clone(), vecB.clone());
  // TODO<joka921> Add a test case for correct unicode collation as soon as that
  // is actually supported.

  // TODO<joka921> Add comparison between string and ValueId (needs mocking of
  // the vocabulary).
}

TEST(RelationalExpression, VariableAndConstant) {
  TestContext ctx;

  auto test = [&](const auto& expression, std::vector<Bool> expected,
                  source_location l = source_location::current()) {
    auto trace = generateLocationTrace(l, "test lambda was called here");
    auto resultAsVariant = expression.evaluate(&ctx.context);
    const auto& result = std::get<VectorWithMemoryLimit<Bool>>(resultAsVariant);
    ASSERT_EQ(result.size(), expected.size());
    for (size_t i = 0; i < result.size(); ++i) {
      EXPECT_EQ(expected[i], result[i]);
    }
  };

  // ?ints column is `1, 0, -1`
  test(makeExpression<LT>(0, Variable{"?ints"}), {true, false, false});
  test(makeExpression<GE>(-0.0, Variable{"?ints"}), {false, true, true});
  test(makeExpression<GE>(Variable{"?ints"}, Int(0)), {true, true, false});

  // ?doubles column is `0.1, -0.1, 2.8`
  test(makeExpression<LT>(-3.5, Variable{"?doubles"}), {true, true, true});
  test(makeExpression<EQ>(Double(-0.1), Variable{"?doubles"}),
       {false, true, false});
  test(makeExpression<GT>(Variable{"?doubles"}, 0), {true, false, true});

  // ?numeric column is 1, -0.1, 3.4
  test(makeExpression<NE>(-0.1, Variable{"?numeric"}), {true, false, true});
  test(makeExpression<GE>(Variable{"?numeric"}, 1), {true, false, true});
  test(makeExpression<GT>(Variable{"?numeric"}, Double(1.2)),
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

  // TODO<joka921> Discuss with Hannah what the result of `"A" != 1"` should be.
  // In the current implementation of the valueIdComparators it is `false` as in
  // "this is an expression error", but we currently don't support this ternary
  // logic.
  test(makeExpression<NE>(1, Variable{"?mixed"}), {false, true, false});
  test(makeExpression<GE>(Variable{"?mixed"}, Double(-0.1)),
       {true, true, false});
}

// TODO<joka921> test for "Variable + constant" (binary search case)

// TODO<joka921> Test for "Variable + Vector" or "Variable + Variable"
