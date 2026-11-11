// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Prashanth Premakumar <prashanthp0703@gmail.com>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include "engine/Join.h"
#include "engine/TripleTermScan.h"
#include "engine/binaryExport/StringMapping.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "index/ExportIds.h"
#include "index/TripleComponentConversions.h"
#include "util/GTestHelpers.h"
#include "util/IndexTestHelpers.h"

TEST(TripleTermExecution, UsesTheOrdinaryJoinOperation) {
  using namespace qlever::tripleTermPrototype;
  auto* qec = ad_utility::testing::getQec();
  auto v = [](uint64_t value) {
    return Id::makeFromVocabIndex(VocabIndex::make(value));
  };
  TripleTermIndexBuilder dictionary;
  auto bob = dictionary.internId({v(1), v(2), v(3)});
  auto charlie = dictionary.internId({v(1), v(2), v(4)});
  auto inner = makeTripleTermScan(qec, dictionary, v(2), v(3),
                                  Variable{"?person"}, Variable{"?term"});
  auto table = std::make_shared<IdTable>(2, qec->getAllocator());
  auto reportA = v(10);
  auto reportB = v(11);
  auto reportC = v(12);
  // Same payload as the first term, but a different datatype. It must not
  // join. Put it first to preserve the declared order of the join column.
  table->push_back({v(13), Id::makeFromInt(bob.getTripleTermIndex().get())});
  table->push_back({reportA, bob});
  table->push_back({reportB, bob});
  table->push_back({reportC, charlie});
  VariableToColumnMap variables{
      {Variable{"?report"}, makeAlwaysDefinedColumn(0)},
      {Variable{"?term"}, makeAlwaysDefinedColumn(1)}};
  auto outer = ad_utility::makeExecutionTree<ExplicitIdTableOperation>(
      qec, std::shared_ptr<const IdTable>{table}, std::move(variables),
      std::vector<ColumnIndex>{1}, LocalVocab{}, "TripleTermExecution:reports");
  auto join = ad_utility::makeExecutionTree<Join>(qec, outer, inner, 1, 1);
  const auto result = join->getResult();
  const auto& rows = result->idTableView();
  ASSERT_EQ(rows.numRows(), 2);
  const auto& columns = join->getVariableColumns();
  auto report = columns.at(Variable{"?report"}).columnIndex_;
  auto person = columns.at(Variable{"?person"}).columnIndex_;
  EXPECT_EQ(rows(0, report), reportA);
  EXPECT_EQ(rows(1, report), reportB);
  EXPECT_EQ(rows(0, person), v(1));
  EXPECT_EQ(rows(1, person), v(1));
}

TEST(TripleTermExecution, ReferencePayloadIsNotANumber) {
  using namespace sparqlExpression::detail;
  auto id = Id::makeFromTripleTermIndex(TripleTermIndex::make(17));
  EXPECT_TRUE(
      std::holds_alternative<NotNumeric>(NumericValueGetter{}(id, nullptr)));
  EXPECT_TRUE(std::holds_alternative<NotNumeric>(
      NumericOrDateValueGetter{}(id, nullptr)));
  EXPECT_FALSE(DatatypeValueGetter{}(id, nullptr).has_value());
}

TEST(TripleTermExecution, BinaryExportDoesNotLeakIndexLocalReferences) {
  auto* qec = ad_utility::testing::getQec();
  qlever::binary_export::StringMapping mapping{qec->getIndex()};
  auto id = Id::makeFromTripleTermIndex(TripleTermIndex::make(17));
  EXPECT_THROW(mapping.remapId(id), std::runtime_error);
  EXPECT_EQ(mapping.size(), 0);
}

TEST(TripleTermExecution, ExportOfUnknownOrContextFreeTermReferences) {
  // The default test index has no triple terms.
  auto* qec = ad_utility::testing::getQec();
  auto id = Id::makeFromTripleTermIndex(TripleTermIndex::make(17));
  // A reference that is not in the index is a bug, not a user error.
  AD_EXPECT_THROW_WITH_MESSAGE(
      ql::exportIds::idToStringAndType(qec->getIndex(), id, LocalVocab{}),
      ::testing::HasSubstr("Triple-term reference not found in the index"));
  // Without an index, a reference cannot be converted (`std::runtime_error`,
  // not the generic `AD_FAIL` exception, shows that the datatype is rejected
  // deliberately rather than treated as unreachable).
  EXPECT_THROW(ql::exportIds::idToStringAndTypeForEncodedValue(id),
               std::runtime_error);
  // The literal conversions keep returning "no literal" without throwing.
  EXPECT_FALSE(
      ql::exportIds::idToLiteral(qec->getIndex().getImpl(), id, LocalVocab{})
          .has_value());
}

// Outside of index building (which handles triple-term objects itself), a term
// must fail loudly instead of being converted like an IRI or a literal.
TEST(TripleTermExecution, TermsHaveNoContextFreeId) {
  using Iri = TripleComponent::Iri;
  TripleComponent term{TripleComponent::TripleTerm{
      Iri::fromIriref("<http://s>"), Iri::fromIriref("<http://p>"),
      Iri::fromIriref("<http://o>")}};
  EncodedIriManager manager;
  EXPECT_THROW(std::ignore = toValueIdIfNotString(term, &manager),
               std::runtime_error);
  // It has no string form that could be put into the vocabulary.
  EXPECT_FALSE(toRdfLiteralView(term).has_value());
}
