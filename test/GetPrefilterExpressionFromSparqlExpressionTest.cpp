//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures
//  Author: Hannes Baumann <baumannh@informatik.uni-freiburg.de>

#include "./PrefilterExpressionTestHelpers.h"
#include "./SparqlExpressionTestHelpers.h"
#include "util/GTestHelpers.h"

using ad_utility::testing::BlankNodeId;
using ad_utility::testing::BoolId;
using ad_utility::testing::DoubleId;
using ad_utility::testing::IntId;
using ad_utility::testing::UndefId;
using ad_utility::testing::VocabId;

using Literal = ad_utility::triple_component::Literal;
using Iri = ad_utility::triple_component::Iri;
using RelValues = std::variant<Variable, ValueId, Iri, Literal>;

namespace {

using namespace sparqlExpression;

namespace makeSparqlExpression {

//______________________________________________________________________________
const auto makeLiteralSparqlExpr =
    [](const auto& child) -> std::unique_ptr<SparqlExpression> {
  using T = std::decay_t<decltype(child)>;
  if constexpr (std::is_same_v<T, ValueId>) {
    return std::make_unique<IdExpression>(child);
  } else if constexpr (std::is_same_v<T, Variable>) {
    return std::make_unique<VariableExpression>(child);
  } else if constexpr (std::is_same_v<T, Literal>) {
    return std::make_unique<StringLiteralExpression>(child);
  } else if constexpr (std::is_same_v<T, Iri>) {
    return std::make_unique<IriExpression>(child);
  } else {
    throw std::runtime_error(
        "Can't create a LiteralExpression from provided (input) type.");
  }
};

//______________________________________________________________________________
auto getExpr = [](const auto& variantVal) -> std::unique_ptr<SparqlExpression> {
  return makeLiteralSparqlExpr(variantVal);
};

//______________________________________________________________________________
template <typename RelExpr>
std::unique_ptr<SparqlExpression> makeRelationalSparqlExprImpl(
    const RelValues& child0, const RelValues& child1) {
  return std::make_unique<RelExpr>(std::array<SparqlExpression::Ptr, 2>{
      std::visit(getExpr, child0), std::visit(getExpr, child1)});
};

//______________________________________________________________________________
std::unique_ptr<SparqlExpression> makeStringStartsWithSparqlExpression(
    const RelValues& child0, const RelValues& child1) {
  return makeStrStartsExpression(std::visit(getExpr, child0),
                                 std::visit(getExpr, child1));
};

//______________________________________________________________________________
// LESS THAN (`<`, `SparqlExpression`)
constexpr auto ltSprql = &makeRelationalSparqlExprImpl<LessThanExpression>;
// LESS EQUAL (`<=`, `SparqlExpression`)
constexpr auto leSprql = &makeRelationalSparqlExprImpl<LessEqualExpression>;
// EQUAL (`==`, `SparqlExpression`)
constexpr auto eqSprql = &makeRelationalSparqlExprImpl<EqualExpression>;
// NOT EQUAL (`!=`, `SparqlExpression`)
constexpr auto neqSprql = &makeRelationalSparqlExprImpl<NotEqualExpression>;
// GREATER EQUAL (`>=`, `SparqlExpression`)
constexpr auto geSprql = &makeRelationalSparqlExprImpl<GreaterEqualExpression>;
// GREATER THAN (`>`, `SparqlExpression`)
constexpr auto gtSprql = &makeRelationalSparqlExprImpl<GreaterThanExpression>;
// AND (`&&`, `SparqlExpression`)
constexpr auto andSprqlExpr = &makeAndExpression;
// OR (`||`, `SparqlExpression`)
constexpr auto orSprqlExpr = &makeOrExpression;
// NOT (`!`, `SparqlExpression`)
constexpr auto notSprqlExpr = &makeUnaryNegateExpression;

//______________________________________________________________________________
// Create SparqlExpression `STRSTARTS`.
constexpr auto strStartsSprql = &makeStringStartsWithSparqlExpression;

}  // namespace makeSparqlExpression

//______________________________________________________________________________
// make `Literal`
const auto L = [](const std::string& content) -> Literal {
  return Literal::fromStringRepresentation(content);
};

//______________________________________________________________________________
// make `Iri`
const auto I = [](const std::string& content) -> Iri {
  return Iri::fromIriref(content);
};

//______________________________________________________________________________
struct TestDates {
  const Id referenceDate1 = DateId(DateParser, "1999-11-11");
  const Id referenceDate2 = DateId(DateParser, "2005-02-27");
};

//______________________________________________________________________________
// ASSERT EQUALITY
//______________________________________________________________________________
const auto equalityCheckPrefilterVectors =
    [](const std::vector<PrefilterExprVariablePair>& result,
       const std::vector<PrefilterExprVariablePair>& expected) -> void {
  ASSERT_EQ(result.size(), expected.size());
  const auto isEqualImpl = [](const PrefilterExprVariablePair& resPair,
                              const PrefilterExprVariablePair& expPair) {
    if (*resPair.first != *expPair.first || resPair.second != expPair.second) {
      std::stringstream stream;
      stream << "The following value pairs don't match:"
             << "\nRESULT: " << *resPair.first << "EXPECTED: " << *expPair.first
             << "RESULT: VARIABLE" << resPair.second.name()
             << "\nEXPECTED: VARIABLE" << expPair.second.name() << std::endl;
      ADD_FAILURE() << stream.str();
      return false;
    }
    return true;
  };
  ASSERT_TRUE(
      std::equal(result.begin(), result.end(), expected.begin(), isEqualImpl));
};

//______________________________________________________________________________
// `evalAndEqualityCheck` evaluates the provided `SparqlExpression` and checks
// in the following if the resulting vector contains the same
// `<PrefilterExpression, Variable>` pairs in the correct order. If no
// `<PrefilterExpression, Variable>` pair is provided, the expected value for
// the `SparqlExpression` is an empty vector.
const auto evalAndEqualityCheck =
    [](std::unique_ptr<SparqlExpression> sparqlExpr,
       std::convertible_to<PrefilterExprVariablePair> auto&&... prefilterArgs) {
      std::vector<PrefilterExprVariablePair> prefilterVarPair = {};
      if constexpr (sizeof...(prefilterArgs) > 0) {
        (prefilterVarPair.emplace_back(
             std::forward<PrefilterExprVariablePair>(prefilterArgs)),
         ...);
      }
      equalityCheckPrefilterVectors(
          sparqlExpr->getPrefilterExpressionForMetadata(),
          std::move(prefilterVarPair));
    };

//______________________________________________________________________________
// Construct a `PAIR` with the given `PrefilterExpression` and `Variable` value.
auto pr =
    [](std::unique_ptr<prefilterExpressions::PrefilterExpression> expr,
       const Variable& var) -> sparqlExpression::PrefilterExprVariablePair {
  return {std::move(expr), var};
};

}  // namespace

using namespace makeSparqlExpression;
using namespace makeFilterExpression;
using namespace makeSparqlExpression;

//______________________________________________________________________________
// Test coverage for the default implementation of
// getPrefilterExpressionForMetadata.
TEST(GetPrefilterExpressionFromSparqlExpression,
     testGetPrefilterExpressionDefault) {
  evalAndEqualityCheck(
      makeUnaryMinusExpression(makeLiteralSparqlExpr(IntId(0))));
  evalAndEqualityCheck(makeMultiplyExpression(
      makeLiteralSparqlExpr(DoubleId(11)), makeLiteralSparqlExpr(DoubleId(3))));
  evalAndEqualityCheck(
      makeStrEndsExpression(makeLiteralSparqlExpr(L("\"Freiburg\"")),
                            makeLiteralSparqlExpr(L("\"burg\""))));
  evalAndEqualityCheck(
      makeIsIriExpression(makeLiteralSparqlExpr(I("<IriIri>"))));
  evalAndEqualityCheck(makeLogExpression(makeLiteralSparqlExpr(DoubleId(8))));
  evalAndEqualityCheck(
      makeStrIriDtExpression(makeLiteralSparqlExpr(L("\"test\"")),
                             makeLiteralSparqlExpr(I("<test_iri>"))));
}

//______________________________________________________________________________
// Check that the (Sparql) RelationalExpression returns the expected
// PrefilterExpression.
TEST(GetPrefilterExpressionFromSparqlExpression,
     getPrefilterExpressionFromSparqlRelational) {
  const TestDates dt{};
  const Variable var = Variable{"?x"};
  // ?x == BooldId(true) (RelationalExpression Sparql)
  // expected: <(== BoolId(true)), ?x> (PrefilterExpression, Variable)
  evalAndEqualityCheck(eqSprql(var, BoolId(true)), pr(eq(BoolId(true)), var));
  // For BoolId(true) == ?x we expect the same PrefilterExpression pair.
  evalAndEqualityCheck(eqSprql(BoolId(true), var), pr(eq(BoolId(true)), var));
  // ?x != BooldId(true) (RelationalExpression Sparql)
  // expected: <(!= BoolId(true)), ?x> (PrefilterExpression, Variable)
  evalAndEqualityCheck(neqSprql(var, BoolId(false)),
                       pr(neq(BoolId(false)), var));
  // Same expected value for BoolId(true) != ?x.
  evalAndEqualityCheck(neqSprql(BoolId(false), var),
                       pr(neq(BoolId(false)), var));
  // ?x >= IntId(1)
  // expected: <(>= IntId(1)), ?x>
  evalAndEqualityCheck(geSprql(var, IntId(1)), pr(ge(IntId(1)), var));
  // IntId(1) <= ?x
  // expected: <(>= IntId(1)), ?x>
  evalAndEqualityCheck(leSprql(IntId(1), var), pr(ge(IntId(1)), var));
  // ?x > IntId(1)
  // expected: <(> IntId(1)), ?x>
  evalAndEqualityCheck(gtSprql(var, IntId(1)), pr(gt(IntId(1)), var));
  // VocabId(10) != ?x
  // expected: <(!= VocabId(10)), ?x>
  evalAndEqualityCheck(neqSprql(VocabId(10), var), pr(neq(VocabId(10)), var));
  // BlankNodeId(1) > ?x
  // expected: <(< BlankNodeId(1)), ?x>
  evalAndEqualityCheck(geSprql(BlankNodeId(1), var),
                       pr(le(BlankNodeId(1)), var));
  // ?x < BlankNodeId(1)
  // expected: <(< BlankNodeId(1)), ?x>
  evalAndEqualityCheck(ltSprql(var, BlankNodeId(1)),
                       pr(lt(BlankNodeId(1)), var));
  // ?x <= referenceDate1
  // expected: <(<= referenceDate1), ?x>
  evalAndEqualityCheck(leSprql(var, dt.referenceDate1),
                       pr(le(dt.referenceDate1), var));
  // referenceDate1 >= ?x
  // expected: <(<= referenceDate1), ?x>
  evalAndEqualityCheck(geSprql(dt.referenceDate1, var),
                       pr(le(dt.referenceDate1), var));
  // DoubleId(10.2) < ?x
  // expected: <(> DoubleId(10.2)), ?x>
  evalAndEqualityCheck(ltSprql(DoubleId(10.2), var),
                       pr(gt(DoubleId(10.2)), var));
  // ?x > DoubleId(10.2)
  // expected: <(> DoubleId(10.2)), ?x>
  evalAndEqualityCheck(gtSprql(var, DoubleId(10.2)),
                       pr(gt(DoubleId(10.2)), var));
}

//______________________________________________________________________________
// More complex relational SparqlExpressions for which
// getPrefilterExpressionForMetadata should yield a vector containing the actual
// corresponding PrefilterExpression values.
TEST(GetPrefilterExpressionFromSparqlExpression,
     getPrefilterExpressionsToComplexSparqlExpressions) {
  const Variable varX = Variable{"?x"};
  const Variable varY = Variable{"?y"};
  const Variable varZ = Variable{"?z"};
  // ?x >= 10 AND ?x != 20
  // expected prefilter pairs:
  // {<((>= 10) AND (!= 20)), ?x>}
  evalAndEqualityCheck(
      andSprqlExpr(geSprql(varX, IntId(10)), neqSprql(varX, IntId(20))),
      pr(andExpr(ge(IntId(10)), neq(IntId(20))), varX));
  // ?z > VocabId(0) AND ?y > 0 AND ?x < 30.00
  // expected prefilter pairs
  // {<(< 30.00), ?x>, <(> 0), ?y>, <(> VocabId(0)), ?z>}
  evalAndEqualityCheck(andSprqlExpr(andSprqlExpr(gtSprql(varZ, VocabId(0)),
                                                 gtSprql(varY, IntId(0))),
                                    ltSprql(varX, DoubleId(30.00))),
                       pr(lt(DoubleId(30.00)), varX), pr(gt(IntId(0)), varY),
                       pr(gt(VocabId(0)), varZ));

  // ?x == VocabId(10) AND ?y >= VocabId(10)
  // expected prefilter pairs:
  // {<(== VocabId(10)), ?x>, <(>= VocabId(10)), ?y>}
  evalAndEqualityCheck(
      andSprqlExpr(eqSprql(varX, VocabId(10)), geSprql(varY, VocabId(10))),
      pr(eq(VocabId(10)), varX), pr(ge(VocabId(10)), varY));
  // !(?x >= 10 OR ?x <= 0)
  // expected prefilter pairs:
  // {<!(?x >= 10 OR ?x <= 0), ?x>}
  evalAndEqualityCheck(notSprqlExpr(orSprqlExpr(geSprql(varX, IntId(10)),
                                                leSprql(varX, IntId(0)))),
                       pr(notExpr(orExpr(ge(IntId(10)), le(IntId(0)))), varX));
  // !(?z == VocabId(10) AND ?z >= VocabId(20))
  // expected prefilter pairs:
  // {<!(?z == VocabId(10) AND ?z >= VocabId(20)) , ?z>}
  evalAndEqualityCheck(
      notSprqlExpr(
          andSprqlExpr(eqSprql(varZ, VocabId(10)), geSprql(varZ, VocabId(20)))),
      pr(notExpr(andExpr(eq(VocabId(10)), ge(VocabId(20)))), varZ));
  // (?x == VocabId(10) AND ?z == VocabId(0)) AND ?y != DoubleId(22.1)
  // expected prefilter pairs:
  // {<(==VocabId(10)) , ?x>, <(!=DoubleId(22.1)), ?y>, <(==VocabId(0)), ?z>}
  evalAndEqualityCheck(andSprqlExpr(andSprqlExpr(eqSprql(VocabId(10), varX),
                                                 eqSprql(varZ, VocabId(0))),
                                    neqSprql(DoubleId(22.1), varY)),
                       pr(eq(VocabId(10)), varX), pr(neq(DoubleId(22.1)), varY),
                       pr(eq(VocabId(0)), varZ));
  // (?z >= 1000 AND ?x == VocabId(10)) OR ?z >= 10000
  // expected prefilter pairs:
  // {<((>=1000) OR (>= 10000)), ?z>}
  evalAndEqualityCheck(orSprqlExpr(andSprqlExpr(geSprql(varZ, IntId(1000)),
                                                eqSprql(varX, VocabId(10))),
                                   geSprql(varZ, IntId(10000))),
                       pr(orExpr(ge(IntId(1000)), ge(IntId(10000))), varZ));
  // !((?z <= VocabId(10) OR ?y <= VocabId(10)) OR ?x <= VocabId(10))
  // expected prefilter pairs:
  // {<!(<= VocabId(10)), ?x>, <!(<= VocabId(10)), ?y>, <!(<= VocabId(10)), ?z>}
  evalAndEqualityCheck(
      notSprqlExpr(orSprqlExpr(
          orSprqlExpr(leSprql(varZ, VocabId(10)), leSprql(varY, VocabId(10))),
          leSprql(varX, VocabId(10)))),
      pr(notExpr(le(VocabId(10))), varX), pr(notExpr(le(VocabId(10))), varY),
      pr(notExpr(le(VocabId(10))), varZ));
  // ?x >= 10 AND ?y >= 10
  // expected prefilter pairs:
  // {<(>= 10), ?x>, <(>= 10), ?y>}
  evalAndEqualityCheck(
      andSprqlExpr(geSprql(varX, IntId(10)), geSprql(varY, IntId(10))),
      pr(ge(IntId(10)), varX), pr(ge(IntId(10)), varY));
  // ?x <= 0 AND ?y <= 0
  // expected prefilter pairs:
  // {<(<= 0), ?x>, <(<= 0), ?y>}
  evalAndEqualityCheck(
      andSprqlExpr(leSprql(varX, IntId(0)), leSprql(varY, IntId(0))),
      pr(le(IntId(0)), varX), pr(le(IntId(0)), varY));
  // (?x >= 10 AND ?y >= 10) OR (?x <= 0 AND ?y <= 0)
  // expected prefilter pairs:
  // {<((>= 10) OR (<= 0)), ?x> <(>= 10) OR (<= 0)), ?y>}
  evalAndEqualityCheck(
      orSprqlExpr(
          andSprqlExpr(geSprql(varX, IntId(10)), geSprql(varY, IntId(10))),
          andSprqlExpr(leSprql(varX, IntId(0)), leSprql(varY, IntId(0)))),
      pr(orExpr(ge(IntId(10)), le(IntId(0))), varX),
      pr(orExpr(ge(IntId(10)), le(IntId(0))), varY));
  // !(?x >= 10 OR ?y >= 10) OR !(?x <= 0 OR ?y <= 0)
  // expected prefilter pairs:
  // {<((!(>= 10) OR !(<= 0))), ?x> <(!(>= 10) OR !(<= 0))), ?y>}
  evalAndEqualityCheck(
      orSprqlExpr(notSprqlExpr(orSprqlExpr(geSprql(varX, IntId(10)),
                                           geSprql(varY, IntId(10)))),
                  notSprqlExpr(orSprqlExpr(leSprql(varX, IntId(0)),
                                           leSprql(varY, IntId(0))))),
      pr(orExpr(notExpr(ge(IntId(10))), notExpr(le(IntId(0)))), varX),
      pr(orExpr(notExpr(ge(IntId(10))), notExpr(le(IntId(0)))), varY));
  // !(?x == VocabId(10) OR ?x == VocabId(20)) AND !(?z >= 10.00 OR ?y == false)
  // expected prefilter pairs:
  // {<!((== VocabId(10)) OR (== VocabId(20))), ?x>, <!(== false), ?y>,
  // <!(>= 10), ?z>}
  evalAndEqualityCheck(
      andSprqlExpr(notSprqlExpr(orSprqlExpr(eqSprql(varX, VocabId(10)),
                                            eqSprql(varX, VocabId(20)))),
                   notSprqlExpr(orSprqlExpr(geSprql(varZ, DoubleId(10)),
                                            eqSprql(varY, BoolId(false))))),
      pr(notExpr(orExpr(eq(VocabId(10)), eq(VocabId(20)))), varX),
      pr(notExpr(eq(BoolId(false))), varY),
      pr(notExpr(ge(DoubleId(10))), varZ));
  // !(!(?x >= 10 AND ?y >= 10)) OR !(!(?x <= 0 AND ?y <= 0))
  // expected prefilter pairs:
  // {<(!!(>= 10) OR !!(<= 0)), ?x>, <(!!(>= 10) OR !!(<= 0)) ,?y>}
  evalAndEqualityCheck(
      orSprqlExpr(notSprqlExpr(notSprqlExpr(andSprqlExpr(
                      geSprql(varX, IntId(10)), geSprql(varY, IntId(10))))),
                  notSprqlExpr(notSprqlExpr(andSprqlExpr(
                      leSprql(varX, IntId(0)), leSprql(varY, IntId(0)))))),
      pr(orExpr(notExpr(notExpr(ge(IntId(10)))),
                notExpr(notExpr(le(IntId(0))))),
         varX),
      pr(orExpr(notExpr(notExpr(ge(IntId(10)))),
                notExpr(notExpr(le(IntId(0))))),
         varY));
  // !((?x >= VocabId(0) AND ?x <= VocabId(10)) OR !(?x != VocabId(99)))
  // expected prefilter pairs:
  // {<!(((>= VocabId(0)) AND (<= VocabId(10))) OR !(!= VocabId(99))) , ?x>}
  evalAndEqualityCheck(
      notSprqlExpr(orSprqlExpr(
          andSprqlExpr(geSprql(varX, VocabId(0)), leSprql(varX, VocabId(10))),
          notSprqlExpr(neqSprql(varX, VocabId(99))))),
      pr(notExpr(orExpr(andExpr(ge(VocabId(0)), le(VocabId(10))),
                        notExpr(neq(VocabId(99))))),
         varX));
  // !((?y >= 10 AND ?y <= 100) OR !(?x >= VocabId(99)))
  // expected prefilter pairs:
  // {<!((>= VocabId(0)) AND (<= VocabId(10)), ?y>, <!(!(>= VocabId(99))), ?x>}
  evalAndEqualityCheck(
      notSprqlExpr(orSprqlExpr(
          andSprqlExpr(geSprql(varY, VocabId(0)), leSprql(varY, VocabId(10))),
          notSprqlExpr(geSprql(varX, VocabId(99))))),
      pr(notExpr(notExpr(ge(VocabId(99)))), varX),
      pr(notExpr(andExpr(ge(VocabId(0)), le(VocabId(10)))), varY));
  // ?z >= 10 AND ?z <= 100 AND ?x >= 10 AND ?x != 50 AND !(?y <= 10) AND
  // !(?city <= VocabId(1000) OR ?city == VocabId(1005))
  // expected prefilter pairs:
  // {<!((<= VocabId(1000)) OR (== VocabId(1005))), ?city>, <((>= 10) AND (!=
  // 50)), ?x>, <!(<= 10), ?y>, <((>= 10) AND (<= 100)), ?z>}
  evalAndEqualityCheck(
      andSprqlExpr(
          andSprqlExpr(
              andSprqlExpr(geSprql(varZ, IntId(10)), leSprql(varZ, IntId(100))),
              andSprqlExpr(andSprqlExpr(geSprql(varX, IntId(10)),
                                        neqSprql(varX, IntId(50))),
                           notSprqlExpr(leSprql(varY, IntId(10))))),
          notSprqlExpr(orSprqlExpr(leSprql(Variable{"?city"}, VocabId(1000)),
                                   eqSprql(Variable{"?city"}, VocabId(1005))))),
      pr(notExpr(orExpr(le(VocabId(1000)), eq(VocabId(1005)))),
         Variable{"?city"}),
      pr(andExpr(ge(IntId(10)), neq(IntId(50))), varX),
      pr(notExpr(le(IntId(10))), varY),
      pr(andExpr(ge(IntId(10)), le(IntId(100))), varZ));
  // ?x >= 10 OR (?x >= -10 AND ?x < 0.00)
  // expected prefilter pairs:
  // {<((>= 10) OR ((>= -10) AND (< 0.00))), ?x>}
  evalAndEqualityCheck(
      orSprqlExpr(geSprql(varX, IntId(10)),
                  andSprqlExpr(geSprql(varX, IntId(-10)),
                               ltSprql(varX, DoubleId(0.00)))),
      pr(orExpr(ge(IntId(10)), andExpr(ge(IntId(-10)), lt(DoubleId(0.00)))),
         varX));
  // !(!(?x >= 10) OR !!(?x >= -10 AND ?x < 0.00))
  // expected prefilter pairs:
  // {<!(!(>= 10) OR !!((>= -10) AND (< 0.00))), ?x>}
  evalAndEqualityCheck(
      notSprqlExpr(orSprqlExpr(
          notSprqlExpr(geSprql(varX, IntId(10))),
          notSprqlExpr(notSprqlExpr(andSprqlExpr(
              geSprql(varX, IntId(-10)), ltSprql(varX, DoubleId(0.00))))))),
      pr(notExpr(orExpr(
             notExpr(ge(IntId(10))),
             notExpr(notExpr(andExpr(ge(IntId(-10)), lt(DoubleId(0.00))))))),
         varX));
  // ?y != ?x AND ?x >= 10
  // expected prefilter pairs:
  // {<(>= 10), ?x>}
  evalAndEqualityCheck(
      andSprqlExpr(neqSprql(varY, varX), geSprql(varX, IntId(10))),
      pr(ge(IntId(10)), varX));
  evalAndEqualityCheck(
      andSprqlExpr(geSprql(varX, IntId(10)), neqSprql(varY, varX)),
      pr(ge(IntId(10)), varX));
}

//______________________________________________________________________________
// For this test we expect that no PrefilterExpression is available.
TEST(GetPrefilterExpressionFromSparqlExpression,
     getEmptyPrefilterFromSparqlRelational) {
  const Variable var = Variable{"?x"};
  const Iri iri = I("<Iri>");
  const Literal lit = L("\"lit\"");
  evalAndEqualityCheck(leSprql(var, var));
  evalAndEqualityCheck(neqSprql(iri, var));
  evalAndEqualityCheck(eqSprql(var, iri));
  evalAndEqualityCheck(neqSprql(IntId(10), DoubleId(23.3)));
  evalAndEqualityCheck(gtSprql(DoubleId(10), lit));
  evalAndEqualityCheck(ltSprql(VocabId(10), BoolId(10)));
  evalAndEqualityCheck(geSprql(lit, lit));
  evalAndEqualityCheck(eqSprql(iri, iri));
  evalAndEqualityCheck(orSprqlExpr(eqSprql(var, var), gtSprql(var, IntId(0))));
  evalAndEqualityCheck(orSprqlExpr(eqSprql(var, var), gtSprql(var, var)));
  evalAndEqualityCheck(andSprqlExpr(eqSprql(var, var), gtSprql(var, var)));
}

//______________________________________________________________________________
// For the following more complex SparqlExpression trees, we also expect an
// empty PrefilterExpression vector.
TEST(GetPrefilterExpressionFromSparqlExpression,
     getEmptyPrefilterForMoreComplexSparqlExpressions) {
  const Variable varX = Variable{"?x"};
  const Variable varY = Variable{"?y"};
  const Variable varZ = Variable{"?z"};
  // ?x <= 10.00 OR ?y > 10
  evalAndEqualityCheck(
      orSprqlExpr(leSprql(DoubleId(10), varX), gtSprql(IntId(10), varY)));
  // ?x >= VocabId(23) OR ?z == VocabId(1)
  evalAndEqualityCheck(
      orSprqlExpr(geSprql(varX, VocabId(23)), eqSprql(varZ, VocabId(1))));
  // (?x < VocabId(10) OR ?z <= VocabId(4)) OR ?z != 5.00
  evalAndEqualityCheck(orSprqlExpr(
      orSprqlExpr(ltSprql(varX, VocabId(10)), leSprql(VocabId(4), varZ)),
      neqSprql(varZ, DoubleId(5))));
  // !(?z > 10.20 AND ?x < 0.001)
  // is equal to
  // ?z <= 10.20 OR ?x >= 0.001
  evalAndEqualityCheck(notSprqlExpr(andSprqlExpr(
      gtSprql(DoubleId(10.2), varZ), ltSprql(DoubleId(0.001), varX))));
  // !(?x > 10.20 AND ?z != VocabId(22))
  // is equal to
  // ?x <= 10.20 OR ?z == VocabId(22)
  evalAndEqualityCheck(notSprqlExpr(andSprqlExpr(gtSprql(DoubleId(10.2), varX),
                                                 neqSprql(VocabId(22), varZ))));
  // !(!((?x < VocabId(10) OR ?x <= VocabId(4)) OR ?z != 5.00))
  // is equal to
  // (?x < VocabId(10) OR ?x <= VocabId(4)) OR ?z != 5.00
  evalAndEqualityCheck(notSprqlExpr(notSprqlExpr(orSprqlExpr(
      orSprqlExpr(ltSprql(varX, VocabId(10)), leSprql(VocabId(4), varX)),
      neqSprql(varZ, DoubleId(5))))));
  // !(?x != 10 AND !(?y >= 10.00 OR ?z <= 10))
  // is equal to
  // ?x == 10 OR ?y >= 10.00 OR ?z <= 10
  evalAndEqualityCheck(notSprqlExpr(
      andSprqlExpr(neqSprql(varX, IntId(10)),
                   notSprqlExpr(orSprqlExpr(geSprql(varY, DoubleId(10.00)),
                                            leSprql(varZ, IntId(10)))))));
  // !((?x != 10 AND ?z != 10) AND (?y == 10 AND ?x >= 20))
  // is equal to
  //?x == 10 OR ?z == 10 OR ?y != 10 OR ?x < 20
  evalAndEqualityCheck(notSprqlExpr(andSprqlExpr(
      andSprqlExpr(neqSprql(varX, IntId(10)), neqSprql(varZ, IntId(10))),
      andSprqlExpr(eqSprql(varY, IntId(10)), geSprql(varX, DoubleId(20))))));
  // !(?z >= 40 AND (?z != 10.00 AND ?y != VocabId(1)))
  // is equal to
  // ?z <= 40 OR ?z == 10.00 OR ?y == VocabId(1)
  evalAndEqualityCheck(notSprqlExpr(andSprqlExpr(
      geSprql(varZ, IntId(40)), andSprqlExpr(neqSprql(varZ, DoubleId(10.00)),
                                             neqSprql(varY, VocabId(1))))));
  // ?z <= true OR !(?x == 10 AND ?y == 10)
  // is equal to
  // ?z <= true OR ?x != 10 OR ?y != 10
  evalAndEqualityCheck(
      orSprqlExpr(leSprql(varZ, BoolId(true)),
                  notSprqlExpr(andSprqlExpr(eqSprql(varX, IntId(10)),
                                            eqSprql(IntId(10), varY)))));
  // !(!(?z <= true OR !(?x == 10 AND ?y == 10)))
  // is equal to
  // ?z <= true OR ?x != 10 OR ?y != 10
  evalAndEqualityCheck(notSprqlExpr(notSprqlExpr(
      orSprqlExpr(leSprql(varZ, BoolId(true)),
                  notSprqlExpr(andSprqlExpr(eqSprql(varX, IntId(10)),
                                            eqSprql(IntId(10), varY)))))));
  // !(!(?x != 10 OR !(?y >= 10.00 AND ?z <= 10)))
  // is equal to
  // ?x != 10 OR ?y < 10.00 OR ?z > 10
  evalAndEqualityCheck(notSprqlExpr(notSprqlExpr(
      orSprqlExpr(neqSprql(varX, IntId(10)),
                  notSprqlExpr(andSprqlExpr(geSprql(varY, DoubleId(10.00)),
                                            leSprql(varZ, IntId(10))))))));
  // !(!(?x == VocabId(10) OR ?y >= 25) AND !(!(?z == true AND ?country ==
  // VocabId(20))))
  // is equal to
  // ?x == VocabId(10) OR ?y >= 25 OR ?z == true AND ?country == VocabId(20)
  evalAndEqualityCheck(notSprqlExpr(andSprqlExpr(
      notSprqlExpr(
          orSprqlExpr(eqSprql(varX, VocabId(10)), geSprql(varY, IntId(25)))),
      notSprqlExpr(notSprqlExpr(
          andSprqlExpr(eqSprql(varZ, BoolId(true)),
                       eqSprql(Variable{"?country"}, VocabId(20))))))));
}

// Test PrefilterExpression creation for SparqlExpression STRSTARTS
//______________________________________________________________________________
TEST(GetPrefilterExpressionFromSparqlExpression,
     getPrefilterExprForStrStartsExpr) {
  const auto varX = Variable{"?x"};
  const auto varY = Variable{"?y"};
  evalAndEqualityCheck(strStartsSprql(varX, VocabId(0)),
                       pr(ge(VocabId(0)), varX));
  evalAndEqualityCheck(strStartsSprql(VocabId(0), varX),
                       pr(le(VocabId(0)), varX));
  evalAndEqualityCheck(strStartsSprql(varX, varY));
  evalAndEqualityCheck(strStartsSprql(VocabId(0), VocabId(10)));
}

// Test that the conditions required for a correct merge of child
// PrefilterExpressions are properly checked during the PrefilterExpression
// construction procedure. This check is applied in the SparqlExpression (for
// NOT, AND and OR) counter-expressions, while constructing their corresponding
// PrefilterExpression.
//______________________________________________________________________________
TEST(GetPrefilterExpressionFromSparqlExpression,
     checkPropertiesForPrefilterConstruction) {
  namespace pd = prefilterExpressions::detail;
  const Variable varX = Variable{"?x"};
  const Variable varY = Variable{"?y"};
  const Variable varZ = Variable{"?z"};
  const Variable varW = Variable{"?w"};
  std::vector<PrefilterExprVariablePair> vec{};
  vec.push_back(pr(andExpr(lt(IntId(5)), gt(DoubleId(-0.01))), varX));
  vec.push_back(pr(gt(VocabId(0)), varY));
  EXPECT_NO_THROW(pd::checkPropertiesForPrefilterConstruction(vec));
  vec.push_back(pr(eq(VocabId(33)), varZ));
  EXPECT_NO_THROW(pd::checkPropertiesForPrefilterConstruction(vec));
  // Add a pair <PrefilterExpression, Variable> with duplicate Variable.
  vec.push_back(pr(gt(VocabId(0)), varZ));
  AD_EXPECT_THROW_WITH_MESSAGE(
      pd::checkPropertiesForPrefilterConstruction(vec),
      ::testing::HasSubstr("For each relevant Variable must exist exactly "
                           "one <PrefilterExpression, Variable> pair."));
  // Remove the last two pairs and add a pair <PrefilterExpression, Variable>
  // which violates the order on Variable(s).
  vec.pop_back();
  vec.pop_back();
  vec.push_back(pr(eq(VocabId(0)), varW));
  AD_EXPECT_THROW_WITH_MESSAGE(
      pd::checkPropertiesForPrefilterConstruction(vec),
      ::testing::HasSubstr(
          "The vector must contain the <PrefilterExpression, Variable> "
          "pairs in sorted order w.r.t. Variable value."));
}
