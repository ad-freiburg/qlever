//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Hannah Bast <bast@cs.uni-freiburg.de>

#include <absl/hash/hash.h>
#include <gmock/gmock.h>

#include <sstream>
#include <string>

#include "./util/GTestHelpers.h"
#include "./util/LocalVocabTestHelpers.h"
#include "./util/TripleComponentTestHelpers.h"
#include "backports/StartsWithAndEndsWith.h"
#include "engine/Bind.h"
#include "engine/CountAvailablePredicates.h"
#include "engine/Distinct.h"
#include "engine/Filter.h"
#include "engine/GroupBy.h"
#include "engine/HasPredicateScan.h"
#include "engine/Join.h"
#include "engine/Minus.h"
#include "engine/MultiColumnJoin.h"
#include "engine/OptionalJoin.h"
#include "engine/OrderBy.h"
#include "engine/QueryExecutionTree.h"
#include "engine/Result.h"
#include "engine/Sort.h"
#include "engine/TransitivePathBase.h"
#include "engine/Union.h"
#include "engine/Values.h"
#include "engine/sparqlExpressions/GroupConcatExpression.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "global/Id.h"
#include "global/Pattern.h"
#include "util/IndexTestHelpers.h"
#include "util/LocalVocabEntryTestHelpers.h"
#include "util/OperationTestHelpers.h"

using ad_utility::testing::makeLocalVocab;

using ad_utility::testing::colIndices;
using ad_utility::testing::iriLocalVocabEntry;
using ad_utility::testing::localVocabEntry;

namespace {
// Get test collection of words of a given size. The words are all distinct.

using TestWords = std::vector<LocalVocabEntry>;

TestWords getTestCollectionOfWords(size_t size,
                                   const LocalVocabContext& context) {
  using namespace ad_utility::triple_component;
  TestWords testCollectionOfWords;
  for (size_t i = 0; i < size; ++i) {
    testCollectionOfWords.push_back(LocalVocabEntry::literalWithoutQuotes(
        std::to_string(i * 7635475567ULL), context));
  }
  return testCollectionOfWords;
}

// _____________________________________________________________________________
auto lit(std::string_view s) {
  return ad_utility::triple_component::Literal::literalWithoutQuotes(s);
}
}  // namespace

// _____________________________________________________________________________
TEST(LocalVocab, constructionAndAccess) {
  auto* qec = ad_utility::testing::getQec();
  // Test collection of words.
  TestWords testWords =
      getTestCollectionOfWords(1000, qec->getLocalVocabContext());

  // Create empty local vocabulary.
  LocalVocab localVocab = makeLocalVocab();
  ASSERT_TRUE(localVocab.empty());

  // Add the words from our test vocabulary and check that they get the expected
  // local vocab indexes.
  for (size_t i = 0; i < testWords.size(); ++i) {
    ASSERT_EQ(*localVocab.getIndexAndAddIfNotContained(testWords[i]),
              testWords[i]);
  }
  size_t localVocabSize = localVocab.size();
  ASSERT_EQ(localVocab.size(), testWords.size());

  // Check that we get the same indexes if we do this again, but that no new
  // words will be added.
  for (size_t i = 0; i < testWords.size(); ++i) {
    ASSERT_EQ(*localVocab.getIndexAndAddIfNotContained(testWords[i]),
              testWords[i]);
    ASSERT_EQ(localVocab.size(), localVocabSize);
  }

  // Check again that we get the right indexes, but with `getIndexOrNullopt`.
  for (size_t i = 0; i < testWords.size(); ++i) {
    std::optional<LocalVocabIndex> localVocabIndex =
        localVocab.getIndexOrNullopt(testWords[i]);
    ASSERT_TRUE(localVocabIndex.has_value());
    ASSERT_EQ(*localVocabIndex.value(), testWords[i]);
  }

  // Check that `getIndexOrNullopt` returns `std::nullopt` for words that are
  // not contained in the local vocab. This makes use of the fact that the words
  // in our test vocabulary only contain digits as letters, see above.
  for (size_t i = 0; i < testWords.size(); ++i) {
    std::string content{testWords[i].getContent()};
    auto illegalWord = LocalVocabEntry::literalWithoutQuotes(
        content + "A", qec->getLocalVocabContext());
    ASSERT_FALSE(localVocab.getIndexOrNullopt(illegalWord));
  }

  // Check that a move gives the expected result.
  localVocabSize = localVocab.size();
  auto localVocabMoved = std::move(localVocab);
  // TODO<joka921> Should we reset the pointer?
  // ASSERT_EQ(localVocab.size(), 0);
  ASSERT_EQ(localVocabMoved.size(), testWords.size());
  // Check that we get the same indexes if we do this again, but that no new
  // words will be added.
  for (size_t i = 0; i < testWords.size(); ++i) {
    ASSERT_EQ(*localVocabMoved.getIndexAndAddIfNotContained(testWords[i]),
              testWords[i]);
    ASSERT_EQ(localVocabMoved.size(), localVocabSize);
  }
}

// _____________________________________________________________________________
TEST(LocalVocab, getIdAndAddIfNotContained) {
  using namespace ad_utility::testing;
  // In this index, `<subject>` is part of the vocabulary and IRIs starting with
  // `http://example.org/` are encoded directly in an `Id`.
  TestIndexConfig config{"<subject> <predicate> <http://example.org/42>"};
  config.encodedPrefixesWithoutAngleBrackets = {"http://example.org/"};
  auto* qec = getQec(std::move(config));
  const auto& context = qec->getLocalVocabContext();
  LocalVocab localVocab = makeLocalVocab();

  // A word in the vocabulary of the index yields the corresponding `VocabIndex`
  // `Id` and is not stored in the local vocab.
  auto inVocab = iriLocalVocabEntry("<subject>", context);
  auto idInVocab = localVocab.getIdAndAddIfNotContained(inVocab);
  auto [lower, upper] = qec->getIndex().getVocab().getPositionOfWord(
      inVocab.toStringRepresentation());
  ASSERT_NE(lower, upper);
  EXPECT_EQ(idInVocab.getBits(), Id::makeFromVocabIndex(lower).getBits());
  EXPECT_TRUE(localVocab.empty());

  // A word that can be encoded directly in an `Id` can't be stored in a
  // `LocalVocabEntry` at all, `toIdOrLocalVocabEntry` yields the encoded `Id`.
  auto encodable = toIdOrLocalVocabEntry(
      ad_utility::triple_component::LiteralOrIri{
          ad_utility::triple_component::Iri::fromIriref(
              "<http://example.org/69>")},
      context);
  ASSERT_TRUE(std::holds_alternative<Id>(encodable));
  EXPECT_EQ(std::get<Id>(encodable).getBits(),
            qec->getIndex()
                .encodedIriManager()
                .encode("<http://example.org/69>")
                .value()
                .getBits());

  // A word that is neither in the vocabulary nor encodable is stored in the
  // local vocab, just like with `getIndexAndAddIfNotContained`.
  auto notInVocab = iriLocalVocabEntry("<notInVocab>", context);
  auto idNotInVocab = localVocab.getIdAndAddIfNotContained(notInVocab);
  ASSERT_EQ(idNotInVocab.getDatatype(), Datatype::LocalVocabIndex);
  EXPECT_EQ(*idNotInVocab.getLocalVocabIndex(), notInVocab);
  EXPECT_EQ(localVocab.size(), 1);
  // Asking a second time doesn't add the word again.
  EXPECT_EQ(localVocab.getIdAndAddIfNotContained(notInVocab).getBits(),
            idNotInVocab.getBits());
  EXPECT_EQ(localVocab.size(), 1);
}

// _____________________________________________________________________________
TEST(LocalVocab, clone) {
  auto* qec = ad_utility::testing::getQec();
  // Create a small local vocabulary.
  size_t localVocabSize = 100;
  LocalVocab localVocabOriginal = makeLocalVocab();
  std::vector<LocalVocabIndex> indices;
  auto inputWords =
      getTestCollectionOfWords(localVocabSize, qec->getLocalVocabContext());
  for (const auto& word : inputWords) {
    indices.push_back(localVocabOriginal.getIndexAndAddIfNotContained(word));
  }
  ASSERT_EQ(localVocabOriginal.size(), localVocabSize);

  // Clone it and test that the clone contains the same words.
  LocalVocab localVocabClone = localVocabOriginal.clone();
  ASSERT_EQ(localVocabOriginal.size(), localVocabSize);
  ASSERT_EQ(localVocabClone.size(), localVocabSize);
  ASSERT_THAT(localVocabClone.getAllWordsForTesting(),
              ::testing::UnorderedElementsAreArray(
                  localVocabOriginal.getAllWordsForTesting()));

  // Test that the indices are still valid after the original vocabulary was
  // destroyed.
  localVocabOriginal = makeLocalVocab();

  for (size_t i = 0; i < inputWords.size(); ++i) {
    EXPECT_EQ(*indices[i], inputWords[i]);
  }

  // Test that a BlankNodeIndex obtained by a `LocalVocab` is also contained
  // in the clone.
  ad_utility::BlankNodeManager bnm;
  LocalVocab v = makeLocalVocab();
  auto id = v.getBlankNodeIndex(&bnm);
  LocalVocab vClone = v.clone();
  EXPECT_TRUE(vClone.isBlankNodeIndexContained(id));
}
// _____________________________________________________________________________
TEST(LocalVocab, merge) {
  // Create a small local vocabulary.
  std::vector<LocalVocabIndex> indices;
  LocalVocab vocA = makeLocalVocab();
  LocalVocab vocB = makeLocalVocab();
  auto* qec = ad_utility::testing::getQec();
  const auto& localVocabContext = qec->getLocalVocabContext();
  constexpr auto lit = [](std::string_view s) {
    return ad_utility::triple_component::LiteralOrIri::literalWithoutQuotes(s);
  };
  indices.push_back(vocA.getIndexAndAddIfNotContained(
      localVocabEntry(lit("oneA"), localVocabContext)));
  indices.push_back(vocA.getIndexAndAddIfNotContained(
      localVocabEntry(lit("twoA"), localVocabContext)));
  indices.push_back(vocA.getIndexAndAddIfNotContained(
      localVocabEntry(lit("oneB"), localVocabContext)));
  indices.push_back(vocA.getIndexAndAddIfNotContained(
      localVocabEntry(lit("twoB"), localVocabContext)));

  // Clone it and test that the clone contains the same words.
  auto vocabs = std::vector{&std::as_const(vocA), &std::as_const(vocB)};
  LocalVocab localVocabMerged =
      LocalVocab::merge(vocabs, ad_utility::testing::makeAllocator());
  ASSERT_EQ(localVocabMerged.size(), 4u);
  ASSERT_THAT(localVocabMerged.getAllWordsForTesting(),
              ::testing::UnorderedElementsAre(lit("oneA"), lit("twoA"),
                                              lit("oneB"), lit("twoB")));
  vocA = makeLocalVocab();
  vocB = makeLocalVocab();
  EXPECT_EQ(*indices[0], lit("oneA"));
  EXPECT_EQ(*indices[1], lit("twoA"));
  EXPECT_EQ(*indices[2], lit("oneB"));
  EXPECT_EQ(*indices[3], lit("twoB"));

  // Test that the `LocalBlankNodeManager` of vocabs is merged correctly.
  ad_utility::BlankNodeManager bnm;
  LocalVocab localVocabMerged2 = makeLocalVocab();
  BlankNodeIndex id;
  {
    LocalVocab vocC = makeLocalVocab();
    LocalVocab vocD = makeLocalVocab();
    id = vocC.getBlankNodeIndex(&bnm);
    auto vocabs2 = std::vector{&std::as_const(vocC), &std::as_const(vocD)};
    localVocabMerged2 =
        LocalVocab::merge(vocabs2, ad_utility::testing::makeAllocator());
  }
  EXPECT_TRUE(localVocabMerged2.isBlankNodeIndexContained(id));

  LocalVocab vocE = makeLocalVocab();
  LocalVocab vocF = makeLocalVocab();
  auto id2 = vocE.getBlankNodeIndex(&bnm);
  auto vocabs3 =
      std::vector{&std::as_const(localVocabMerged2), &std::as_const(vocF)};
  vocE.mergeWith(vocabs3 | ql::views::transform(
                               [](const LocalVocab* l) -> const LocalVocab& {
                                 return *l;
                               }));
  EXPECT_TRUE(vocE.isBlankNodeIndexContained(id));
  EXPECT_TRUE(localVocabMerged2.isBlankNodeIndexContained(id));
  EXPECT_TRUE(vocE.isBlankNodeIndexContained(id2));
}

// Test `numSets` and `isLocalVocabIndexContained` for indices in the primary
// set, in another set, and in neither.
TEST(LocalVocab, numSetsAndIsLocalVocabIndexContained) {
  const auto& context = ad_utility::testing::getQec()->getLocalVocabContext();
  LocalVocab vocA = makeLocalVocab();
  LocalVocab vocB = makeLocalVocab();
  LocalVocab vocC = makeLocalVocab();
  auto idxA =
      vocA.getIndexAndAddIfNotContained(iriLocalVocabEntry("<a>", context));
  auto idxB =
      vocB.getIndexAndAddIfNotContained(iriLocalVocabEntry("<b>", context));
  auto idxC =
      vocC.getIndexAndAddIfNotContained(iriLocalVocabEntry("<c>", context));

  // Before the merge, only the primary set exists.
  EXPECT_EQ(vocA.numSets(), 1u);
  EXPECT_TRUE(vocA.isLocalVocabIndexContained(idxA));
  EXPECT_FALSE(vocA.isLocalVocabIndexContained(idxB));

  // Merging an empty vocab doesn't add a set.
  vocA.mergeWith(makeLocalVocab());
  EXPECT_EQ(vocA.numSets(), 1u);

  // After the merge, the words of `vocB` are contained in another set.
  vocA.mergeWith(vocB);
  EXPECT_EQ(vocA.numSets(), 2u);
  EXPECT_TRUE(vocA.isLocalVocabIndexContained(idxA));
  EXPECT_TRUE(vocA.isLocalVocabIndexContained(idxB));
  EXPECT_FALSE(vocA.isLocalVocabIndexContained(idxC));
}

// _____________________________________________________________________________
TEST(LocalVocab, propagation) {
  // Query execution context (with small test index), see `IndexTestHelpers.h`.
  using ad_utility::AllocatorWithLimit;
  QueryExecutionContext* testQec = ad_utility::testing::getQec();

  // Lambda that checks the contents of the local vocabulary after the specified
  // operation.
  //
  // NOTE: This cannot be `const Operation&` because `Operation::getResult()` is
  // not `const`.
  auto checkLocalVocab =
      [&](Operation& operation,
          const std::vector<std::string>& expectedWordsAsStrings,
          ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) -> void {
    auto t = generateLocationTrace(loc);
    TestWords expectedWords;
    const auto& localVocabContext = testQec->getLocalVocabContext();
    auto toLitOrIri = [&localVocabContext](const auto& word) {
      using namespace ad_utility::triple_component;
      if (ql::starts_with(word, '<')) {
        return iriLocalVocabEntry(word, localVocabContext);
      } else {
        return LocalVocabEntry::literalWithoutQuotes(word, localVocabContext);
      }
    };
    ql::ranges::transform(expectedWordsAsStrings,
                          std::back_inserter(expectedWords), toLitOrIri);
    std::shared_ptr<const Result> resultTable = operation.getResult();
    ASSERT_TRUE(resultTable)
        << "Operation: " << operation.getDescriptor() << std::endl;
    TestWords localVocabWords =
        resultTable->localVocab().getAllWordsForTesting();
    // We currently allow the local vocab to have multiple IDs for the same
    // word, so we have to deduplicate first.
    ql::ranges::sort(localVocabWords);
    localVocabWords.erase(std::ranges::unique(localVocabWords).begin(),
                          localVocabWords.end());
    ASSERT_THAT(localVocabWords,
                ::testing::UnorderedElementsAreArray(expectedWords))
        << "Operation: " << operation.getDescriptor() << std::endl;
  };

  // Lambda that returns a `std::shared_ptr` to a `QueryExecutionTree` with only
  // the given operation (and using `testQec`).
  auto qet = [&](const auto& operation) -> std::shared_ptr<QueryExecutionTree> {
    return std::make_shared<QueryExecutionTree>(
        testQec, std::make_shared<Values>(operation));
  };

  // VALUES operation with two variables and two rows. Adds four new literals.
  //
  // Note 1: It is important to pass a copy of `values1` (and `values2` below)
  // to `checkLocalVocab` because when the operation is executed, the parsed
  // values are moved out by `Values::writeValues` and would then no longer be
  // there in the subsequent uses of `values1` and `values2` if it were not
  // copied.
  //
  // Note 2: For literals, the quotes are part of the name, so if we wanted the
  // literal "x", we would have to write TripleComponent{"\"x\""}. For the
  // purposes of this test, we just want something that's not yet in the index,
  // so "x" etc. is just fine (and also different from the "<x>" below).
  auto iri = ad_utility::testing::iri;
  Values values1(
      testQec,
      {{Variable{"?x"}, Variable{"?y"}},
       {{TripleComponent{iri("<xN1>")}, TripleComponent{iri("<yN1>")}},
        {TripleComponent{iri("<xN1>")}, TripleComponent{iri("<yN2>")}}}});
  Values values1copy = values1;
  std::vector<std::string> localVocab1{"<xN1>", "<yN1>", "<yN2>"};
  checkLocalVocab(values1copy, localVocab1);

  // VALUES operation that uses an existing literal (from the test index).
  Values values2(
      testQec, {{Variable{"?x"}, Variable{"?y"}},
                {{TripleComponent{iri("<x>")}, TripleComponent{iri("<y>")}}}});
  Values values2copy = values2;
  checkLocalVocab(values2copy, std::vector<std::string>{});

  // Contains local vocab words that are (at least partially) disjoint from the
  // words in `values1`.
  Values values3(
      testQec,
      {{Variable{"?x"}, Variable{"?y"}},
       {{TripleComponent{iri("<xN1>")}, TripleComponent{iri("<yN1>")}},
        {TripleComponent{iri("<xN2>")}, TripleComponent{iri("<yN3>")}}}});
  std::vector<std::string> localVocab13{"<xN1>", "<yN1>", "<yN2>", "<xN2>",
                                        "<yN3>"};

  // JOIN operation with exactly one non-empty local vocab and with two
  // non-empty local vocabs (the last two arguments are the two join columns).
  Join join1(testQec, qet(values1), qet(values2), 0, 0);
  checkLocalVocab(join1, localVocab1);
  Join join2(testQec, qet(values1), qet(values3), 0, 0);
  checkLocalVocab(join2, localVocab13);

  // OPTIONAL JOIN operation with exactly one non-empty local vocab.
  OptionalJoin optJoin1(testQec, qet(values1), qet(values2));
  checkLocalVocab(optJoin1, localVocab1);

  // OPTIONAL JOIN operation with two non-empty local vocab.
  OptionalJoin optJoin2(testQec, qet(values1), qet(values3));
  checkLocalVocab(optJoin2, localVocab13);

  // MULTI-COLUMN JOIN operation with exactly one non-empty local vocab and with
  // two non-empty local vocabs.
  MultiColumnJoin multiJoin1(testQec, qet(values1), qet(values2));
  checkLocalVocab(multiJoin1, localVocab1);
  MultiColumnJoin multiJoin2(testQec, qet(values1), qet(values3));
  checkLocalVocab(multiJoin2, localVocab13);

  // ORDER BY operation (the third argument are the indices of the columns to be
  // sorted, and the sort order; not important for this test).
  OrderBy orderBy(testQec, qet(values1), {{0, true}, {1, true}});
  checkLocalVocab(orderBy, localVocab1);

  // SORT operation (the third operation is the sort column).
  Sort sort(testQec, qet(values1), colIndices({0}));
  checkLocalVocab(sort, localVocab1);

  // DISTINCT operation (the third argument are the indices of the input columns
  // that are considered for the output; not important for this test).
  Distinct distinct1(testQec, qet(values1), colIndices({0, 1}));
  checkLocalVocab(distinct1, localVocab1);

  // GROUP BY operation.
  auto groupConcatExpression =
      [](std::string variableName,
         std::string separator) -> sparqlExpression::SparqlExpressionPimpl {
    // NOTE: The last argument is a string representation of the expression,
    // which does not matter for the purposes of this test.
    return {std::make_unique<sparqlExpression::GroupConcatExpression>(
                false,
                std::make_unique<sparqlExpression::VariableExpression>(
                    Variable{variableName}),
                separator),
            "GROUP_CONCAT"};
  };
  Values values1b(
      testQec, {{Variable{"?x"}, Variable{"?y"}},
                {{TripleComponent{lit("xN1")}, TripleComponent{lit("yN1")}},
                 {TripleComponent{lit("xN1")}, TripleComponent{lit("yN2")}}}});
  GroupBy groupBy(
      testQec, {Variable{"?x"}},
      {Alias{groupConcatExpression("?y", "|"), Variable{"?concat"}}},
      qet(values1b));
  checkLocalVocab(groupBy,
                  std::vector<std::string>{"xN1", "yN1", "yN2", "yN1|yN2"});

  // DISTINCT again, but after something has been added to the local vocabulary
  // (to check that the "y1|y2" added by the GROUP BY does not also appear here,
  // as it did before GroupBy::computeResult copied it's local vocabulary).
  Distinct distinct2(testQec, qet(values1), colIndices({0}));
  checkLocalVocab(distinct2, localVocab1);

  // UNION operation with exactly one non-empty local vocab and with two
  // non-empy local vocabs.
  Union union1(testQec, qet(values1), qet(values2));
  checkLocalVocab(union1, localVocab1);
  Union union2(testQec, qet(values1), qet(values3));
  checkLocalVocab(union2, localVocab13);

  // MINUS operation with exactly one non-empty local vocab and with
  // two non-empty local vocabs.
  Minus minus1(testQec, qet(values1), qet(values2));
  checkLocalVocab(minus1, localVocab1);
  Minus minus2(testQec, qet(values1), qet(values3));
  checkLocalVocab(minus2, localVocab1);

  // FILTER operation (the third argument is an expression; which one doesn't
  // matter for this test).
  Filter filter(
      testQec, qet(values1),
      {std::make_unique<sparqlExpression::VariableExpression>(Variable{"?x"}),
       "Expression ?x"});
  checkLocalVocab(filter, localVocab1);

  // BIND operation (the third argument is a `parsedQuery::Bind` object).
  Bind bind(
      testQec, qet(values1),
      {{std::make_unique<sparqlExpression::VariableExpression>(Variable{"?x"}),
        "Expression ?x"},
       Variable{"?z"}});
  checkLocalVocab(bind, localVocab1);

  // TRANSITIVE PATH operation.
  //
  // NOTE: As part of an actual query, the child of a transitive path
  // operation is always an index scan, which has a non-empty
  // local-vocabulary. Still, it doesn't harm to test this.
  TransitivePathSide left(std::nullopt, 0, Variable{"?x"});
  TransitivePathSide right(std::nullopt, 1, Variable{"?y"});
  auto transitivePath = TransitivePathBase::makeTransitivePath(
      testQec, qet(values1), std::move(left), std::move(right), 1, 1);
  checkLocalVocab(*transitivePath, localVocab1);

  // PATTERN TRICK operations.
  HasPredicateScan hasPredicateScan(testQec, qet(values1), 0, Variable{"?z"});
  checkLocalVocab(hasPredicateScan, localVocab1);
  Values valuesPatternTrick(
      testQec,
      {{Variable{"?x"}, Variable{"?y"}},
       {{TripleComponent{iri("<xN1>")}, TripleComponent{Pattern::NoPattern}},
        {TripleComponent{iri("<xN1>")}, TripleComponent{Pattern::NoPattern}}}});
  CountAvailablePredicates countAvailablePredictes(
      testQec, qet(valuesPatternTrick), 0, Variable{"?y"}, Variable{"?count"});
  checkLocalVocab(countAvailablePredictes, {"<xN1>"});
  // TEXT operations.
  //
  // TODO<joka921> Maybe add tests for the new TextIndexScanFor... classes,
  // they never introduce any local vocab.
}

// _____________________________________________________________________________
TEST(LocalVocab, getBlankNodeIndex) {
  ad_utility::BlankNodeManager bnm(0);
  LocalVocab v = makeLocalVocab();
  BlankNodeIndex a = v.getBlankNodeIndex(&bnm);
  BlankNodeIndex b = v.getBlankNodeIndex(&bnm);
  EXPECT_NE(a, b);
}

// _____________________________________________________________________________
TEST(LocalVocab, otherWordSetIsTransitivelyPropagated) {
  using ad_utility::triple_component::LiteralOrIri;
  auto* qec = ad_utility::testing::getQec();
  const auto& localVocabContext = qec->getLocalVocabContext();
  LocalVocab original = makeLocalVocab();
  original.getIndexAndAddIfNotContained(
      LocalVocabEntry::literalWithoutQuotes("test", localVocabContext));

  LocalVocab clone = original.clone();
  LocalVocab mergeCandidate = makeLocalVocab();
  mergeCandidate.mergeWith(clone);

  EXPECT_EQ(mergeCandidate.size(), 1);
  EXPECT_THAT(mergeCandidate.getAllWordsForTesting(),
              ::testing::UnorderedElementsAre(
                  LiteralOrIri::literalWithoutQuotes("test")));
}

// _____________________________________________________________________________
TEST(LocalVocab, sizeIsProperlyUpdatedOnMerge) {
  using ad_utility::triple_component::LiteralOrIri;
  using ::testing::UnorderedElementsAre;
  auto* qec = ad_utility::testing::getQec();
  const auto& localVocabContext = qec->getLocalVocabContext();
  LocalVocab original = makeLocalVocab();
  original.getIndexAndAddIfNotContained(
      LocalVocabEntry::literalWithoutQuotes("test", localVocabContext));

  LocalVocab clone1 = original.clone();
  LocalVocab clone2 = original.clone();
  clone2.mergeWith(original);
  original.mergeWith(clone1);

  // Make sure we deduplicate `otherWordSets_` with the primary word set.
  EXPECT_EQ(original.size(), 1);
  EXPECT_THAT(original.getAllWordsForTesting(),
              UnorderedElementsAre(LiteralOrIri::literalWithoutQuotes("test")));

  EXPECT_EQ(clone2.size(), 1);
  EXPECT_THAT(clone2.getAllWordsForTesting(),
              UnorderedElementsAre(LiteralOrIri::literalWithoutQuotes("test")));

  LocalVocab clone3 = original.clone();
  EXPECT_EQ(clone3.size(), 1);
  EXPECT_THAT(clone3.getAllWordsForTesting(),
              UnorderedElementsAre(LiteralOrIri::literalWithoutQuotes("test")));
}

// _____________________________________________________________________________
TEST(LocalVocab, modificationIsBlockedAfterCloneOrMerge) {
  using ad_utility::triple_component::LiteralOrIri;
  auto* qec = ad_utility::testing::getQec();
  const auto& localVocabContext = qec->getLocalVocabContext();
  auto literal = LiteralOrIri::literalWithoutQuotes("test");
  auto otherLiteral = LiteralOrIri::literalWithoutQuotes("other");
  {
    LocalVocab original = makeLocalVocab();
    original.getIndexAndAddIfNotContained(
        localVocabEntry(literal, localVocabContext));
    (void)original.clone();
    EXPECT_NE(
        original.getIndexOrNullopt(localVocabEntry(literal, localVocabContext)),
        std::nullopt);
    EXPECT_THROW(original.getIndexAndAddIfNotContained(
                     localVocabEntry(literal, localVocabContext)),
                 ad_utility::Exception);
    EXPECT_THROW(original.getIndexAndAddIfNotContained(
                     localVocabEntry(otherLiteral, localVocabContext)),
                 ad_utility::Exception);
    EXPECT_EQ(original.size(), 1);
  }
  {
    LocalVocab original = makeLocalVocab();
    LocalVocab other = makeLocalVocab();
    original.getIndexAndAddIfNotContained(
        localVocabEntry(literal, localVocabContext));
    other.mergeWith(original);
    EXPECT_NE(
        original.getIndexOrNullopt(localVocabEntry(literal, localVocabContext)),
        std::nullopt);
    EXPECT_THROW(original.getIndexAndAddIfNotContained(
                     localVocabEntry(literal, localVocabContext)),
                 ad_utility::Exception);
    EXPECT_THROW(original.getIndexAndAddIfNotContained(
                     localVocabEntry(otherLiteral, localVocabContext)),
                 ad_utility::Exception);
    EXPECT_EQ(original.size(), 1);
  }
}

// _____________________________________________________________________________
TEST(LocalVocab, modificationIsNotBlockedAfterAcquiringHolder) {
  using ad_utility::triple_component::LiteralOrIri;
  auto* qec = ad_utility::testing::getQec();
  const auto& localVocabContext = qec->getLocalVocabContext();
  auto literal = LiteralOrIri::literalWithoutQuotes("test");
  auto otherLiteral = LiteralOrIri::literalWithoutQuotes("other");
  std::optional<LocalVocab::LifetimeExtender> extender = std::nullopt;
  LocalVocabIndex encodedTest;
  LocalVocabIndex encodedOther;
  {
    LocalVocab original = makeLocalVocab();
    encodedTest = original.getIndexAndAddIfNotContained(
        localVocabEntry(literal, localVocabContext));
    extender = original.getLifetimeExtender();

    EXPECT_EQ(
        original.getIndexOrNullopt(localVocabEntry(literal, localVocabContext)),
        std::optional{encodedTest});

    EXPECT_EQ(original.getIndexAndAddIfNotContained(
                  localVocabEntry(literal, localVocabContext)),
              encodedTest);
    EXPECT_EQ(original.size(), 1);

    encodedOther = original.getIndexAndAddIfNotContained(
        localVocabEntry(otherLiteral, localVocabContext));
    EXPECT_EQ(original.size(), 2);
  }
  // The `extender` keeps the `LocalVocabIndex`es valid even though the
  // corresponding `LocalVocab` has already been destroyed.
  (void)extender;
  EXPECT_EQ(*encodedTest, literal);
  EXPECT_EQ(*encodedOther, otherLiteral);
}

// _____________________________________________________________________________
TEST(LocalVocab, getOwnedLocalBlankNodeBlocks_EmptyCase) {
  LocalVocab vocab = makeLocalVocab();
  auto blocks = vocab.getOwnedLocalBlankNodeBlocks();
  EXPECT_TRUE(blocks.empty());
}

// _____________________________________________________________________________
TEST(LocalVocab, getOwnedLocalBlankNodeBlocks_WithBlankNodes) {
  ad_utility::BlankNodeManager bnm;
  LocalVocab vocab = makeLocalVocab();

  // Allocate some blank node IDs.
  auto id1 = vocab.getBlankNodeIndex(&bnm);
  auto id2 = vocab.getBlankNodeIndex(&bnm);
  auto id3 = vocab.getBlankNodeIndex(&bnm);

  // Get the owned blocks.
  auto blocks = vocab.getOwnedLocalBlankNodeBlocks();

  // Should have exactly one entry (primary blocks).
  EXPECT_FALSE(blocks.empty());
  EXPECT_EQ(blocks.size(), 1);

  // Verify the structure contains block indices.
  EXPECT_FALSE(blocks[0].blockIndices_.empty());

  // Verify the blank node IDs are still valid.
  EXPECT_TRUE(vocab.isBlankNodeIndexContained(id1));
  EXPECT_TRUE(vocab.isBlankNodeIndexContained(id2));
  EXPECT_TRUE(vocab.isBlankNodeIndexContained(id3));
}

// _____________________________________________________________________________
TEST(LocalVocab, reserveBlankNodeBlocksFromExplicitIndices_EmptyIndices) {
  ad_utility::BlankNodeManager bnm;
  LocalVocab vocab = makeLocalVocab();

  // Call with empty vector - should not crash and should do nothing.
  std::vector<
      ad_utility::BlankNodeManager::LocalBlankNodeManager::OwnedBlocksEntry>
      emptyIndices;
  EXPECT_NO_THROW(
      vocab.reserveBlankNodeBlocksFromExplicitIndices(emptyIndices, &bnm));

  // Verify still no blank node manager.
  auto blocks = vocab.getOwnedLocalBlankNodeBlocks();
  EXPECT_TRUE(blocks.empty());
}

// _____________________________________________________________________________
TEST(LocalVocab, reserveBlankNodeBlocksFromExplicitIndices_RestoresBlocks) {
  ad_utility::BlankNodeManager bnm;

  // Step 1: Create original vocab with blank nodes.
  LocalVocab originalVocab = makeLocalVocab();
  auto id1 = originalVocab.getBlankNodeIndex(&bnm);
  auto id2 = originalVocab.getBlankNodeIndex(&bnm);
  auto id3 = originalVocab.getBlankNodeIndex(&bnm);

  // Step 2: Serialize the blank node blocks.
  auto serializedBlocks = originalVocab.getOwnedLocalBlankNodeBlocks();
  ASSERT_FALSE(serializedBlocks.empty());

  // Step 3: Create new vocab and restore from serialized data.
  LocalVocab restoredVocab = makeLocalVocab();
  restoredVocab.reserveBlankNodeBlocksFromExplicitIndices(serializedBlocks,
                                                          &bnm);

  // Step 4: Verify the restored vocab contains the same blank node indices.
  EXPECT_TRUE(restoredVocab.isBlankNodeIndexContained(id1));
  EXPECT_TRUE(restoredVocab.isBlankNodeIndexContained(id2));
  EXPECT_TRUE(restoredVocab.isBlankNodeIndexContained(id3));

  // Step 5: Verify we can still allocate new blank nodes in the restored vocab.
  auto newId = restoredVocab.getBlankNodeIndex(&bnm);
  EXPECT_TRUE(restoredVocab.isBlankNodeIndexContained(newId));

  // The new ID should be different from the old ones.
  EXPECT_NE(newId, id1);
  EXPECT_NE(newId, id2);
  EXPECT_NE(newId, id3);
}

// _____________________________________________________________________________
TEST(LocalVocab, reserveBlankNodeBlocksFromExplicitIndices_PreconditionCheck) {
  ad_utility::BlankNodeManager bnm;
  LocalVocab vocab = makeLocalVocab();

  // Allocate a blank node, which creates the localBlankNodeManager_.
  [[maybe_unused]] auto id = vocab.getBlankNodeIndex(&bnm);

  // Try to reserve blocks when localBlankNodeManager_ already exists.
  // This should violate the AD_CONTRACT_CHECK.
  ad_utility::BlankNodeManager::LocalBlankNodeManager::OwnedBlocksEntry entry;
  entry.uuid_ = boost::uuids::random_generator()();
  entry.blockIndices_ = {1, 2, 3};
  std::vector<
      ad_utility::BlankNodeManager::LocalBlankNodeManager::OwnedBlocksEntry>
      indices{entry};

  AD_EXPECT_THROW_WITH_MESSAGE(
      vocab.reserveBlankNodeBlocksFromExplicitIndices(indices, &bnm),
      ::testing::HasSubstr("Assertion"));
}

// Test that the addresses of the words of a `LocalVocab` (which are the
// `LocalVocabIndex`es) stay stable when the arena and the hash table grow.
TEST(LocalVocab, wordsHaveStableAddresses) {
  // Many words (and a single word that is much larger than the initial blocks
  // of the arena), such that the arena and the hash table of the words have to
  // grow several times, which must not change the addresses of the words.
  const auto& context = ad_utility::testing::getQec()->getLocalVocabContext();
  LocalVocab localVocab = makeLocalVocab();
  std::vector<std::pair<LocalVocabIndex, std::string>> indices;
  auto add = [&](std::string content) {
    auto word = LocalVocabEntry::literalWithoutQuotes(content, context);
    indices.emplace_back(localVocab.getIndexAndAddIfNotContained(word),
                         std::string{word.toStringRepresentation()});
  };
  for (size_t i = 0; i < 20'000; ++i) {
    add(std::to_string(i));
    if (i == 10'000) {
      add(std::string(1'000'000, 'x'));
    }
  }
  ASSERT_EQ(localVocab.size(), indices.size());
  for (const auto& [index, representation] : indices) {
    EXPECT_EQ(index->toStringRepresentation(), representation);
    EXPECT_EQ(localVocab.getIndexOrNullopt(*index), index);
  }
  // The words of the vocab are views into characters that are owned by the
  // vocab (and not by the words that were added).
  for (const auto& [index, representation] : indices) {
    EXPECT_NE(index->toStringRepresentation().data(), representation.data());
  }
}

// Test that a word that is stored in one `LocalVocab` can be added to another
// `LocalVocab`, which then owns a copy of it.
TEST(LocalVocab, addWordsOfOtherLocalVocab) {
  auto* qec = ad_utility::testing::getQec(
      ad_utility::testing::TestIndexConfig{"<a> <b> <c> ."});
  const auto& context = qec->getLocalVocabContext();
  LocalVocab vocab1 = makeLocalVocab();
  auto word = ad_utility::testing::iriLocalVocabEntry("<bb>", context);
  LocalVocabIndex index1 = vocab1.getIndexAndAddIfNotContained(word);
  // Compute (and thereby cache) the position of the stored word.
  auto position = index1->positionInVocab();

  // Adding the stored word to another vocab makes a copy of it (including the
  // cached position), adding it again doesn't.
  LocalVocab vocab2 = makeLocalVocab();
  LocalVocabIndex index2 = vocab2.getIndexAndAddIfNotContained(*index1);
  EXPECT_NE(index1, index2);
  EXPECT_NE(index1->toStringRepresentation().data(),
            index2->toStringRepresentation().data());
  EXPECT_EQ(*index1, *index2);
  EXPECT_EQ(index2->positionInVocab(), position);
  EXPECT_EQ(vocab2.getIndexAndAddIfNotContained(*index1), index2);
  EXPECT_EQ(vocab2.getIndexAndAddIfNotContained(word), index2);
  EXPECT_EQ(vocab2.getIndexOrNullopt(*index1), index2);
  EXPECT_EQ(vocab2.size(), 1);
  EXPECT_EQ(vocab2.getIdAndAddIfNotContained(*index1),
            Id::makeFromLocalVocabIndex(index2));

  // The word lives as long as `vocab2`, even if `vocab1` is destroyed.
  vocab1 = makeLocalVocab();
  EXPECT_EQ(index2->toStringRepresentation(), word.toStringRepresentation());
}

// Test that `LocalVocabEntry`s and `LocalVocabEntryView`s compare and hash
// alike, and that a view can be converted back to an owning entry.
TEST(LocalVocab, viewsAndOwningEntries) {
  const auto& context = ad_utility::testing::getQec()->getLocalVocabContext();
  auto a = LocalVocabEntry::literalWithoutQuotes("a", context);
  auto b = LocalVocabEntry::literalWithoutQuotes("b", context);
  LocalVocab localVocab = makeLocalVocab();
  const LocalVocabEntryView& viewOfA =
      *localVocab.getIndexAndAddIfNotContained(a);
  // A view of an entry that refers to the characters of the entry itself.
  LocalVocabEntryView viewOfB{b};
  EXPECT_EQ(viewOfB.toStringRepresentation().data(),
            b.toStringRepresentation().data());

  // Owning entries and views can be compared with each other.
  EXPECT_EQ(viewOfA, a);
  EXPECT_EQ(a, viewOfA);
  EXPECT_NE(viewOfA, b);
  EXPECT_NE(viewOfA, viewOfB);
  EXPECT_EQ(viewOfA.compareThreeWay(b), ql::strong_ordering::less);
  EXPECT_EQ(b.compareThreeWay(viewOfA), ql::strong_ordering::greater);
  EXPECT_EQ(viewOfA.compareThreeWay(a), ql::strong_ordering::equal);
  EXPECT_LT(viewOfA, viewOfB);
  EXPECT_EQ(absl::HashOf(viewOfA), absl::HashOf(a));

  // An owning copy of a view.
  LocalVocabEntry copyOfA = viewOfA.toOwned();
  EXPECT_EQ(copyOfA, a);
  EXPECT_NE(copyOfA.toStringRepresentation().data(),
            viewOfA.toStringRepresentation().data());
  EXPECT_EQ(&copyOfA.getContextForTesting(), &context);
  EXPECT_THAT(localVocab.getAllWordsForTesting(), ::testing::ElementsAre(a));
}

// Test that a stored word keeps the structure of the entry it was created from
// (and is not parsed again from its string representation).
TEST(LocalVocab, wordsAreStoredWithTheirStructure) {
  using ad_utility::triple_component::Iri;
  using ad_utility::triple_component::Literal;
  using ad_utility::triple_component::LiteralView;
  const auto& context = ad_utility::testing::getQec()->getLocalVocabContext();
  // A literal with a datatype that contains a quote, for which the position of
  // the datatype can't be determined from the string representation alone.
  LocalVocabEntry word{Literal::literalWithoutQuotes(
                           "x", Iri::fromStringRepresentation("<a\"b>")),
                       context};
  EXPECT_ANY_THROW(
      LiteralView::fromStringRepresentation(word.toStringRepresentation()));

  LocalVocab localVocab = makeLocalVocab();
  const LocalVocabEntryView& stored =
      *localVocab.getIndexAndAddIfNotContained(word);
  EXPECT_NE(stored.toStringRepresentation().data(),
            word.toStringRepresentation().data());
  EXPECT_EQ(stored, word);
  EXPECT_EQ(stored.getLiteral().getContent(), "x");
  EXPECT_EQ(stored.getLiteral().getDatatype(), "a\"b");

  // The same holds for a view of the entry itself.
  LocalVocabEntryView view{word};
  EXPECT_EQ(view.getLiteral().getDatatype(), "a\"b");
}

// Test that the word sets of a `LocalVocab` count toward the memory limit of
// its allocator, also after cloning and merging.
TEST(LocalVocab, memoryIsTracked) {
  using namespace ad_utility::memory_literals;
  const auto& context = ad_utility::testing::getQec()->getLocalVocabContext();
  auto allocator = ad_utility::makeAllocatorWithLimit<Id>(10_MB);
  auto memoryUsed = [&allocator]() {
    return 10_MB - allocator.amountMemoryLeft();
  };
  auto word = [&context](std::string content) {
    return LocalVocabEntry::literalWithoutQuotes(content, context);
  };

  auto localVocab = std::make_unique<LocalVocab>(allocator);
  EXPECT_TRUE(localVocab->allocator() == allocator);
  localVocab->getIndexAndAddIfNotContained(word(std::string(100'000, 'a')));
  auto usedForOneWord = memoryUsed();
  EXPECT_GE(usedForOneWord, 100_kB);

  // A clone uses the same allocator.
  LocalVocab clone = localVocab->clone();
  EXPECT_TRUE(clone.allocator() == allocator);
  clone.getIndexAndAddIfNotContained(word(std::string(100'000, 'b')));
  EXPECT_GE(memoryUsed(), usedForOneWord + 100_kB);

  // The words of `localVocab` stay alive (and are still counted) as long as
  // `clone` refers to them.
  localVocab.reset();
  EXPECT_GE(memoryUsed(), usedForOneWord + 100_kB);

  // The set of the other word sets is also allocated with the allocator.
  auto usedBeforeMerge = memoryUsed();
  std::vector<LocalVocab> others;
  for (size_t i = 0; i < 100; ++i) {
    others.emplace_back(ad_utility::testing::makeAllocator());
    others.back().getIndexAndAddIfNotContained(word(std::to_string(i)));
  }
  clone.mergeWith(others);
  EXPECT_GT(memoryUsed(), usedBeforeMerge);

  clone = makeLocalVocab();
  EXPECT_EQ(memoryUsed(), 0_B);
}

// Test that adding a word that would exceed the memory limit throws and leaves
// the `LocalVocab` unchanged.
TEST(LocalVocab, exceedingTheMemoryLimitThrows) {
  using namespace ad_utility::memory_literals;
  const auto& context = ad_utility::testing::getQec()->getLocalVocabContext();
  auto word = [&context](std::string content) {
    return LocalVocabEntry::literalWithoutQuotes(content, context);
  };
  LocalVocab localVocab{ad_utility::makeAllocatorWithLimit<Id>(100_kB)};
  auto a = word("a");
  LocalVocabIndex indexOfA = localVocab.getIndexAndAddIfNotContained(a);
  EXPECT_THROW(localVocab.getIndexAndAddIfNotContained(
                   word(std::string(1'000'000, 'x'))),
               ad_utility::detail::AllocationExceedsLimitException);
  EXPECT_EQ(localVocab.size(), 1);
  EXPECT_EQ(localVocab.getIndexOrNullopt(a), indexOfA);
  EXPECT_THAT(localVocab.getAllWordsForTesting(), ::testing::ElementsAre(a));

  // Words that fit into the memory limit can still be added, until the limit
  // is reached.
  auto b = word("b");
  LocalVocabIndex indexOfB = localVocab.getIndexAndAddIfNotContained(b);
  EXPECT_EQ(localVocab.size(), 2);
  EXPECT_EQ(*indexOfB, b);
  EXPECT_THROW(
      {
        for (size_t i = 0; i < 1'000'000; ++i) {
          localVocab.getIndexAndAddIfNotContained(word(std::to_string(i)));
        }
      },
      ad_utility::detail::AllocationExceedsLimitException);
  EXPECT_EQ(localVocab.size(), localVocab.getAllWordsForTesting().size());
  EXPECT_EQ(localVocab.getIndexOrNullopt(a), indexOfA);
  EXPECT_EQ(localVocab.getIndexOrNullopt(b), indexOfB);
}
