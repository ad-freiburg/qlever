// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include <cmath>
#include <limits>
#include <string>
#include <vector>

#include "./util/IdTestHelpers.h"
#include "./util/IndexTestHelpers.h"
#include "backports/StartsWithAndEndsWith.h"
#include "engine/OrderByComparator.h"
#include "global/Constants.h"
#include "index/LocalVocabEntry.h"

using namespace ad_utility::testing;
using TermKind = OrderByComparator::TermKind;

// Test that `getRangeOfIrisInVocabulary` separates the IRIs from the other
// words of the vocabulary.
TEST(OrderByComparator, rangeOfIrisInVocabulary) {
  auto* qec = getQec();
  const auto& index = qec->getIndex();
  auto [begin, end] = qec->getLocalVocabContext().getRangeOfIrisInVocabulary();
  size_t vocabSize = index.getVocab().size();
  // The vocabulary of the test index has both literals and IRIs.
  EXPECT_GT(begin.get(), 0);
  EXPECT_LT(begin.get(), end.get());
  EXPECT_LE(end.get(), vocabSize);
  for (size_t i = 0; i < vocabSize; ++i) {
    const auto& word = index.indexToString(VocabIndex::make(i));
    EXPECT_EQ(begin.get() <= i && i < end.get(), ql::starts_with(word, '<'))
        << i << ' ' << word;
  }
}

// Test that the WKT literals of a geo-split vocabulary, whose indices lie
// above the range of the IRIs, are literals.
TEST(OrderByComparator, getTermKindWithGeoSplitVocabulary) {
  std::string wktLiteral =
      "\"LINESTRING(7 48, 8 49)\"^^<http://www.opengis.net/ont/"
      "geosparql#wktLiteral>";
  TestIndexConfig config{
      absl::StrCat("<a> <p> ", wktLiteral, " . <a> <p> 3 .")};
  config.vocabularyType = ad_utility::VocabularyType::OnDiskCompressedGeoSplit;
  auto* qec = getQec(config);
  auto getId = makeGetId(qec->getIndex());
  OrderByComparator comparator{qec->getLocalVocabContext()};

  Id wkt = getId(wktLiteral);
  auto [begin, end] = qec->getLocalVocabContext().getRangeOfIrisInVocabulary();
  ASSERT_EQ(wkt.getDatatype(), Datatype::VocabIndex);
  ASSERT_GE(wkt.getVocabIndex(), end);
  EXPECT_EQ(comparator.getTermKind(wkt), TermKind::Literal);
  EXPECT_EQ(comparator.getTermKind(getId("<a>")), TermKind::Iri);
  // In the order of `ORDER BY`, the IRI comes before the number, which comes
  // before the WKT literal (by its datatype).
  EXPECT_LT(comparator.compare(getId("<a>"), IntId(3)), 0);
  EXPECT_LT(comparator.compare(IntId(3), wkt), 0);
}

// _____________________________________________________________________________
TEST(OrderByComparator, getTermKind) {
  auto* qec = getQec();
  const auto& context = qec->getLocalVocabContext();
  auto getId = makeGetId(qec->getIndex());
  OrderByComparator comparator{context};
  auto kind = [&comparator](Id id) { return comparator.getTermKind(id); };

  EXPECT_EQ(kind(UndefId()), TermKind::Undefined);

  EXPECT_EQ(kind(BlankNodeId(3)), TermKind::BlankNode);
  auto blankNodeOfBnodeFunction = LocalVocabEntry::fromStringRepresentation(
      absl::StrCat(QLEVER_INTERNAL_BLANK_NODE_IRI_PREFIX, "_:un_label_0>"),
      context);
  EXPECT_EQ(kind(Id::makeFromLocalVocabIndex(&blankNodeOfBnodeFunction)),
            TermKind::BlankNode);

  EXPECT_EQ(kind(getId("<x>")), TermKind::Iri);
  auto localIri = LocalVocabEntry::fromStringRepresentation("<a:1>", context);
  EXPECT_EQ(kind(Id::makeFromLocalVocabIndex(&localIri)), TermKind::Iri);
  EXPECT_EQ(kind(Id::makeFromEncodedVal(5)), TermKind::Iri);
  EXPECT_EQ(kind(Id::makeFromSecondaryVocabIndex(SecondaryVocabIndex::make(2))),
            TermKind::Iri);

  EXPECT_EQ(kind(getId("\"alpha\"")), TermKind::Literal);
  auto localLiteral = LocalVocabEntry::literalWithoutQuotes("alx", context);
  EXPECT_EQ(kind(Id::makeFromLocalVocabIndex(&localLiteral)),
            TermKind::Literal);
  EXPECT_EQ(kind(BoolId(true)), TermKind::Literal);
  EXPECT_EQ(kind(IntId(-3)), TermKind::Literal);
  EXPECT_EQ(kind(DoubleId(2.5)), TermKind::Literal);
  EXPECT_EQ(kind(DateId(DateYearOrDuration::parseXsdDate, "2000-01-01")),
            TermKind::Literal);
  EXPECT_EQ(kind(Id::makeFromGeoPoint(GeoPoint{1.0, 2.0})), TermKind::Literal);
  EXPECT_EQ(kind(TextRecordId(4)), TermKind::Literal);
  EXPECT_EQ(kind(Id::makeFromWordVocabIndex(WordVocabIndex::make(4))),
            TermKind::Literal);

  // An `Id` with an invalid datatype.
  static_assert(static_cast<int>(Datatype::MaxValue) < 15);
  EXPECT_ANY_THROW(kind(Id::fromBits(Id::T{15} << Id::numDataBits)));
}

// Test the order on groups of `Id`s: the `Id`s of one group are equivalent, and
// each group comes before the next one.
TEST(OrderByComparator, compare) {
  auto* qec = getQec();
  const auto& context = qec->getLocalVocabContext();
  auto getId = makeGetId(qec->getIndex());
  auto local = [](const LocalVocabEntry& entry) {
    return Id::makeFromLocalVocabIndex(&entry);
  };
  auto blankNodeOfBnodeFunction = LocalVocabEntry::fromStringRepresentation(
      absl::StrCat(QLEVER_INTERNAL_BLANK_NODE_IRI_PREFIX, "_:un_label_0>"),
      context);
  auto a1 = LocalVocabEntry::fromStringRepresentation("<a:1>", context);
  auto a2 = LocalVocabEntry::fromStringRepresentation("<a:2>", context);
  auto alx = LocalVocabEntry::literalWithoutQuotes("alx", context);

  constexpr int64_t twoToThe53 = int64_t{1} << 53;
  auto d53 = static_cast<double>(twoToThe53);
  constexpr double inf = std::numeric_limits<double>::infinity();
  constexpr double NaN = std::numeric_limits<double>::quiet_NaN();
  auto I = IntId;
  auto D = DoubleId;

  // NOTE: A `Double` `ValueId` has only 49 significant bits, so above `d53` it
  // can only represent multiples of 32, like `d53 + 64`.
  std::vector<std::vector<Id>> groups{
      {UndefId()},
      // Blank nodes. Those of the `BNODE` function come first, because their
      // datatype (`LocalVocabIndex`) is smaller.
      {local(blankNodeOfBnodeFunction)},
      {BlankNodeId(3)},
      {BlankNodeId(7)},
      // IRIs.
      {local(a1)},
      {local(a2)},
      {getId("<x>")},
      {getId("<y>")},
      // Literals: booleans, numbers, strings, dates (by their datatypes).
      {BoolId(false)},
      {BoolId(true)},
      {D(-inf)},
      {I(-twoToThe53 - 1)},
      {I(-twoToThe53), D(-d53)},
      {I(-1)},
      {I(0), D(0.0), D(-0.0)},
      {D(0.5)},
      {I(5), D(5.0)},
      {I(twoToThe53), D(d53)},
      {I(twoToThe53 + 1)},
      {D(d53 + 64)},
      {I(ValueId::maxInt)},
      {D(inf)},
      {D(NaN)},
      {getId("\"alpha\"")},
      {local(alx)},
      {getId("\"Beta\"")},
      {DateId(DateYearOrDuration::parseXsdDate, "2000-01-01")}};

  OrderByComparator comparator{context};
  auto sign = [](int i) { return (i > 0) - (i < 0); };
  for (size_t i = 0; i < groups.size(); ++i) {
    for (size_t j = 0; j < groups.size(); ++j) {
      int expected = sign(static_cast<int>(i) - static_cast<int>(j));
      for (Id a : groups[i]) {
        for (Id b : groups[j]) {
          EXPECT_EQ(sign(comparator.compare(a, b)), expected) << a << ' ' << b;
          EXPECT_EQ(comparator(a, b), expected < 0) << a << ' ' << b;
        }
      }
    }
  }
}
