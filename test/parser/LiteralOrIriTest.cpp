// Copyright 2023 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../util/GTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "index/IndexImpl.h"
#include "parser/LiteralOrIri.h"
#include "parser/NormalizedString.h"
#include "rdfTypes/Iri.h"
#include "rdfTypes/Literal.h"
#include "util/HashSet.h"

using namespace ad_utility::triple_component;

TEST(IriTest, IriCreation) {
  Iri iri = Iri::fromIriref("<http://www.wikidata.org/entity/Q3138>");

  EXPECT_THAT("http://www.wikidata.org/entity/Q3138",
              asStringViewUnsafe(iri.getContent()));
}

TEST(IriTest, getBaseIri) {
  // Helper lambda that calls `Iri::getBaseIri` and returns the result as a
  // string (including the angle brackets).
  auto getBaseIri = [](std::string_view iriSv, bool domainOnly) {
    return Iri::fromIriref(iriSv)
        .getBaseIri(domainOnly)
        .toStringRepresentation();
  };
  // IRI with path.
  EXPECT_EQ(getBaseIri("<http://purl.uniprot.org/uniprot/>", false),
            "<http://purl.uniprot.org/uniprot/>");
  EXPECT_EQ(getBaseIri("<http://purl.uniprot.org/uniprot>", false),
            "<http://purl.uniprot.org/uniprot/>");
  EXPECT_EQ(getBaseIri("<http://purl.uniprot.org/uniprot/>", true),
            "<http://purl.uniprot.org/>");
  EXPECT_EQ(getBaseIri("<http://purl.uniprot.org/uniprot>", true),
            "<http://purl.uniprot.org/>");
  // IRI with domain only.
  EXPECT_EQ(getBaseIri("<http://purl.uniprot.org/>", false),
            "<http://purl.uniprot.org/>");
  EXPECT_EQ(getBaseIri("<http://purl.uniprot.org>", false),
            "<http://purl.uniprot.org/>");
  EXPECT_EQ(getBaseIri("<http://purl.uniprot.org/>", true),
            "<http://purl.uniprot.org/>");
  EXPECT_EQ(getBaseIri("<http://purl.uniprot.org>", true),
            "<http://purl.uniprot.org/>");
  // IRI without scheme.
  EXPECT_EQ(getBaseIri("<blabla>", false), "<blabla/>");
  EXPECT_EQ(getBaseIri("<blabla>", true), "<blabla/>");
}

TEST(IriTest, emptyIri) {
  EXPECT_TRUE(Iri{}.empty());
  EXPECT_FALSE(Iri::fromIriref("<http://www.wikidata.org>").empty());
}

TEST(IriTest, fromIrirefConsiderBase) {
  // Helper lambda that calls `Iri::fromIrirefConsiderBase` with the two base
  // IRIs and returns the results as a string (including the angle brackets).
  Iri baseForRelativeIris;
  Iri baseForAbsoluteIris;
  auto fromIrirefConsiderBase = [&baseForRelativeIris, &baseForAbsoluteIris](
                                    std::string_view iriStringWithBrackets) {
    return Iri::fromIrirefConsiderBase(iriStringWithBrackets,
                                       baseForRelativeIris, baseForAbsoluteIris)
        .toStringRepresentation();
  };

  // Check that it works for "real" base IRIs.
  baseForRelativeIris = Iri::fromIriref("<http://.../uniprot/>");
  baseForAbsoluteIris = Iri::fromIriref("<http://.../>");
  EXPECT_EQ(fromIrirefConsiderBase("<http://purl.uniprot.org/uniprot/>"),
            "<http://purl.uniprot.org/uniprot/>");
  EXPECT_EQ(fromIrirefConsiderBase("<UPI001AF4585D>"),
            "<http://.../uniprot/UPI001AF4585D>");
  EXPECT_EQ(fromIrirefConsiderBase("</prosite/PS51927>"),
            "<http://.../prosite/PS51927>");

  // Check that with the default base, all IRIs remain unchanged.
  baseForRelativeIris = Iri{};
  baseForAbsoluteIris = Iri{};
  EXPECT_THAT(fromIrirefConsiderBase("<http://purl.uniprot.org/uniprot/>"),
              "<http://purl.uniprot.org/uniprot/>");
  EXPECT_THAT(fromIrirefConsiderBase("</a>"), "</a>");
  EXPECT_THAT(fromIrirefConsiderBase("<a>"), "<a>");
  EXPECT_THAT(fromIrirefConsiderBase("<>"), "<>");
}

TEST(LiteralTest, LiteralTest) {
  Literal literal = Literal::literalWithoutQuotes("Hello World");

  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  EXPECT_THAT("Hello World", asStringViewUnsafe(literal.getContent()));
  EXPECT_THROW(literal.getLanguageTag(), ad_utility::Exception);
  EXPECT_THROW(literal.getDatatype(), ad_utility::Exception);
}

TEST(LiteralTest, LiteralTestWithDatatype) {
  Literal literal = Literal::literalWithoutQuotes(
      "Hello World",
      Iri::fromIriref("<http://www.w3.org/2001/XMLSchema#string>"));

  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_TRUE(literal.hasDatatype());
  EXPECT_THAT("Hello World", asStringViewUnsafe(literal.getContent()));
  EXPECT_THROW(literal.getLanguageTag(), ad_utility::Exception);
  EXPECT_THAT("http://www.w3.org/2001/XMLSchema#string",
              asStringViewUnsafe(literal.getDatatype()));
}

TEST(LiteralTest, LiteralTestWithLanguagetag) {
  Literal literal = Literal::literalWithoutQuotes("Hallo Welt", "@de");

  EXPECT_TRUE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  EXPECT_THAT("Hallo Welt", asStringViewUnsafe(literal.getContent()));
  EXPECT_THAT("de", asStringViewUnsafe(literal.getLanguageTag()));
  EXPECT_THROW(literal.getDatatype(), ad_utility::Exception);
}

TEST(LiteralOrIri, LiteralOrIriWithIri) {
  auto* qec = ad_utility::testing::getQec();
  const IndexImpl* index = &qec->getIndex().getImpl();

  LiteralOrIri iri =
      LiteralOrIri::iriref("<http://www.wikidata.org/entity/Q3138>", index);

  EXPECT_TRUE(iri.isIri());
  EXPECT_THAT("http://www.wikidata.org/entity/Q3138",
              asStringViewUnsafe(iri.getIriContent()));
  EXPECT_FALSE(iri.isLiteral());
  EXPECT_THROW(iri.hasLanguageTag(), ad_utility::Exception);
  EXPECT_THROW(iri.hasDatatype(), ad_utility::Exception);
  EXPECT_THROW(iri.getLiteralContent(), ad_utility::Exception);
  EXPECT_THROW(iri.getLanguageTag(), ad_utility::Exception);
  EXPECT_THROW(iri.getDatatype(), ad_utility::Exception);
}

TEST(LiteralOrIri, LiteralOrIriWithPrefixedIri) {
  auto* qec = ad_utility::testing::getQec();
  const IndexImpl* index = &qec->getIndex().getImpl();

  LiteralOrIri iri = LiteralOrIri::prefixedIri(
      Iri::fromIriref("<http://www.wikidata.org/entity/>"), "Q3138", index);

  EXPECT_TRUE(iri.isIri());
  EXPECT_THAT("http://www.wikidata.org/entity/Q3138",
              asStringViewUnsafe(iri.getIriContent()));
  EXPECT_FALSE(iri.isLiteral());
  EXPECT_THROW(iri.hasLanguageTag(), ad_utility::Exception);
  EXPECT_THROW(iri.hasDatatype(), ad_utility::Exception);
  EXPECT_THROW(iri.getLiteralContent(), ad_utility::Exception);
  EXPECT_THROW(iri.getLanguageTag(), ad_utility::Exception);
  EXPECT_THROW(iri.getDatatype(), ad_utility::Exception);
}

TEST(LiteralOrIri, LiteralOrIriWithLiteral) {
  auto* qec = ad_utility::testing::getQec();
  const IndexImpl* index = &qec->getIndex().getImpl();

  LiteralOrIri literal = LiteralOrIri::literalWithoutQuotes("Hello World", index);

  EXPECT_FALSE(literal.isIri());
  EXPECT_THROW(literal.getIriContent(), ad_utility::Exception);
  EXPECT_TRUE(literal.isLiteral());
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  EXPECT_THAT("Hello World", asStringViewUnsafe(literal.getLiteralContent()));
  EXPECT_THROW(literal.getLanguageTag(), ad_utility::Exception);
  EXPECT_THROW(literal.getDatatype(), ad_utility::Exception);
}

TEST(LiteralOrIri, LiteralOrIriWithLiteralWithQuotes) {
  auto* qec = ad_utility::testing::getQec();
  const IndexImpl* index = &qec->getIndex().getImpl();

  LiteralOrIri literal = LiteralOrIri::literalWithQuotes("\"Hello World\"", index);

  EXPECT_FALSE(literal.isIri());
  EXPECT_THROW(literal.getIriContent(), ad_utility::Exception);
  EXPECT_TRUE(literal.isLiteral());
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  EXPECT_THAT("Hello World", asStringViewUnsafe(literal.getLiteralContent()));
  EXPECT_THROW(literal.getLanguageTag(), ad_utility::Exception);
  EXPECT_THROW(literal.getDatatype(), ad_utility::Exception);
}

TEST(LiteralOrIri, LiteralOrIriWithLiteralAndDatatype) {
  auto* qec = ad_utility::testing::getQec();
  const IndexImpl* index = &qec->getIndex().getImpl();

  LiteralOrIri literal = LiteralOrIri::literalWithoutQuotes(
      "Hello World", index, Iri::fromIriref("<http://www.w3.org/2001/XMLSchema#string>"));

  EXPECT_FALSE(literal.isIri());
  EXPECT_THROW(literal.getIriContent(), ad_utility::Exception);
  EXPECT_TRUE(literal.isLiteral());
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_TRUE(literal.hasDatatype());
  EXPECT_THAT("Hello World", asStringViewUnsafe(literal.getLiteralContent()));
  EXPECT_THROW(literal.getLanguageTag(), ad_utility::Exception);
  EXPECT_THAT("http://www.w3.org/2001/XMLSchema#string",
              asStringViewUnsafe(literal.getDatatype()));
}

TEST(LiteralOrIri, LiteralOrIriWithLiteralWithQuotesAndDatatype) {
  auto* qec = ad_utility::testing::getQec();
  const IndexImpl* index = &qec->getIndex().getImpl();

  LiteralOrIri literal = LiteralOrIri::literalWithQuotes(
      "\"Hello World\"", index, Iri::fromIriref("<http://www.w3.org/2001/XMLSchema#string>"));

  EXPECT_FALSE(literal.isIri());
  EXPECT_THROW(literal.getIriContent(), ad_utility::Exception);
  EXPECT_TRUE(literal.isLiteral());
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_TRUE(literal.hasDatatype());
  EXPECT_THAT("Hello World", asStringViewUnsafe(literal.getLiteralContent()));
  EXPECT_THROW(literal.getLanguageTag(), ad_utility::Exception);
  EXPECT_THAT("http://www.w3.org/2001/XMLSchema#string",
              asStringViewUnsafe(literal.getDatatype()));
}

TEST(LiteralOrIri, LiteralOrIriWithLiteralAndLanguageTag) {
  auto* qec = ad_utility::testing::getQec();
  const IndexImpl* index = &qec->getIndex().getImpl();

  LiteralOrIri literal =
      LiteralOrIri::literalWithoutQuotes("Hej världen", index, "@se");

  EXPECT_FALSE(literal.isIri());
  EXPECT_THROW(literal.getIriContent(), ad_utility::Exception);
  EXPECT_TRUE(literal.isLiteral());
  EXPECT_TRUE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  EXPECT_THAT("Hej världen", asStringViewUnsafe(literal.getLiteralContent()));
  EXPECT_THAT("se", asStringViewUnsafe(literal.getLanguageTag()));
  EXPECT_THROW(literal.getDatatype(), ad_utility::Exception);
}

TEST(LiteralOrIri, LiteralOrIriWithLiteralWithQuotesAndLanguageTag) {
  auto* qec = ad_utility::testing::getQec();
  const IndexImpl* index = &qec->getIndex().getImpl();

  LiteralOrIri literal =
      LiteralOrIri::literalWithQuotes("'''Hej världen'''", index, "@se");

  EXPECT_FALSE(literal.isIri());
  EXPECT_THROW(literal.getIriContent(), ad_utility::Exception);
  EXPECT_TRUE(literal.isLiteral());
  EXPECT_TRUE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  EXPECT_THAT("Hej världen", asStringViewUnsafe(literal.getLiteralContent()));
  EXPECT_THAT("se", asStringViewUnsafe(literal.getLanguageTag()));
  EXPECT_THROW(literal.getDatatype(), ad_utility::Exception);
}

TEST(LiteralOrIri, GetContent) {
  auto* qec = ad_utility::testing::getQec();
  const IndexImpl* index = &qec->getIndex().getImpl();

  LiteralOrIri iri = LiteralOrIri::iriref("<https://example.org/books/book1>", index);
  LiteralOrIri literalWithLanguageTag =
      LiteralOrIri::literalWithoutQuotes("Hello World", index, "@de");
  LiteralOrIri literalWithDatatype = LiteralOrIri::literalWithoutQuotes(
      "ABC", index, Iri::fromIriref("<https://example.org>"));

  EXPECT_THAT("https://example.org/books/book1",
              asStringViewUnsafe(iri.getContent()));
  EXPECT_THAT("Hello World",
              asStringViewUnsafe(literalWithLanguageTag.getContent()));
  EXPECT_THAT("ABC", asStringViewUnsafe(literalWithDatatype.getContent()));
}

TEST(LiteralOrIri, EnsureLiteralsAreEncoded) {
  auto* qec = ad_utility::testing::getQec();
  const IndexImpl* index = &qec->getIndex().getImpl();

  LiteralOrIri literal1 =
      LiteralOrIri::literalWithQuotes(R"("This is to be \"\\ encoded")", index);
  EXPECT_THAT(R"(This is to be "\ encoded)",
              asStringViewUnsafe(literal1.getContent()));

  LiteralOrIri literal2 =
      LiteralOrIri::literalWithoutQuotes(R"(This is to be \"\\ encoded)", index);
  EXPECT_THAT(R"(This is to be "\ encoded)",
              asStringViewUnsafe(literal2.getContent()));
}

TEST(LiteralOrIri, Printing) {
  auto* qec = ad_utility::testing::getQec();
  const IndexImpl* index = &qec->getIndex().getImpl();

  LiteralOrIri literal1 = LiteralOrIri::literalWithoutQuotes("hallo", index);
  std::stringstream str;
  PrintTo(literal1, &str);
  EXPECT_EQ(str.str(), "\"hallo\"");
}

TEST(LiteralOrIri, Hashing) {
  auto* qec = ad_utility::testing::getQec();
  const IndexImpl* index = &qec->getIndex().getImpl();

  auto lit = LiteralOrIri::literalWithoutQuotes("bimbamm", index);
  auto iri = LiteralOrIri::iriref("<bimbamm>", index);
  ad_utility::HashSet<LiteralOrIri> set{lit, iri};
  EXPECT_THAT(set, ::testing::UnorderedElementsAre(lit, iri));
}

// _______________________________________________________________________
TEST(LiteralTest, isPlain) {
  auto* qec = ad_utility::testing::getQec();
  const IndexImpl* index = &qec->getIndex().getImpl();

  LiteralOrIri literal = LiteralOrIri::literalWithoutQuotes("Hello World!", index);
  EXPECT_TRUE(literal.getLiteral().isPlain());
  literal = LiteralOrIri::literalWithoutQuotes(
      "Hello World!", index, Iri::fromIriref("<http://www.w3.org/2001/XMLSchema#string>"));
  EXPECT_FALSE(literal.getLiteral().isPlain());
  literal = LiteralOrIri::literalWithoutQuotes("Hello World!", index, "@en");
  EXPECT_FALSE(literal.getLiteral().isPlain());
}

// _______________________________________________________________________
TEST(LiteralTest, SetSubstr) {
  auto* qec = ad_utility::testing::getQec();
  const IndexImpl* index = &qec->getIndex().getImpl();

  LiteralOrIri literal = LiteralOrIri::literalWithoutQuotes(
      "Hello World!", index, Iri::fromIriref("<http://www.w3.org/2001/XMLSchema#string>"));
  literal.getLiteral().setSubstr(0, 5);
  EXPECT_THAT("Hello", asStringViewUnsafe(literal.getContent()));
  EXPECT_THAT("http://www.w3.org/2001/XMLSchema#string",
              asStringViewUnsafe(literal.getDatatype()));

  literal = LiteralOrIri::literalWithoutQuotes(
      "Hello World!", index, Iri::fromIriref("<http://www.w3.org/2001/XMLSchema#string>"));
  literal.getLiteral().setSubstr(6, 5);
  EXPECT_THAT("World", asStringViewUnsafe(literal.getContent()));
  EXPECT_THAT("http://www.w3.org/2001/XMLSchema#string",
              asStringViewUnsafe(literal.getDatatype()));

  // Substring works at the byte level (not the UTF-8 character level).
  literal = LiteralOrIri::literalWithoutQuotes("Äpfel", index);
  literal.getLiteral().setSubstr(0, 2);
  EXPECT_THAT("Ä", asStringViewUnsafe(literal.getContent()));

  // Test with invalid values.
  literal = LiteralOrIri::literalWithoutQuotes(
      "Hello World!", index, Iri::fromIriref("<http://www.w3.org/2001/XMLSchema#string>"));
  EXPECT_THROW(literal.getLiteral().setSubstr(12, 1), ad_utility::Exception);
  EXPECT_THROW(literal.getLiteral().setSubstr(6, 7), ad_utility::Exception);
}

TEST(LiteralOrIriTest, GetIri) {
  auto* qec = ad_utility::testing::getQec();
  const IndexImpl* index = &qec->getIndex().getImpl();

  LiteralOrIri iri = LiteralOrIri::iriref("<https://example.org/books/book1>", index);
  EXPECT_THAT("https://example.org/books/book1",
              asStringViewUnsafe(iri.getIriContent()));

  LiteralOrIri literal = LiteralOrIri::literalWithoutQuotes("Hello World!", index);
  EXPECT_THROW(literal.getIri(), ad_utility::Exception);
}

// _______________________________________________________________________
TEST(LiteralTest, removeDatatypeOrLanguageTag) {
  auto* qec = ad_utility::testing::getQec();
  const IndexImpl* index = &qec->getIndex().getImpl();

  LiteralOrIri literal = LiteralOrIri::literalWithoutQuotes(
      "Hello World!", index, Iri::fromIriref("<http://www.w3.org/2001/XMLSchema#string>"));
  literal.getLiteral().removeDatatypeOrLanguageTag();
  EXPECT_THAT("Hello World!", asStringViewUnsafe(literal.getContent()));
  EXPECT_FALSE(literal.hasDatatype());
  EXPECT_THROW(literal.getDatatype(), ad_utility::Exception);

  literal = LiteralOrIri::literalWithoutQuotes("Hello World!", index, "@en");
  literal.getLiteral().removeDatatypeOrLanguageTag();
  EXPECT_THAT("Hello World!", asStringViewUnsafe(literal.getContent()));
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_THROW(literal.getLanguageTag(), ad_utility::Exception);

  literal = LiteralOrIri::literalWithoutQuotes("Hello World!", index);
  literal.getLiteral().removeDatatypeOrLanguageTag();
  EXPECT_THAT("Hello World!", asStringViewUnsafe(literal.getContent()));
}

// _______________________________________________________________________
TEST(LiteralTest, replaceContent) {
  auto* qec = ad_utility::testing::getQec();
  const IndexImpl* index = &qec->getIndex().getImpl();

  LiteralOrIri literal = LiteralOrIri::literalWithoutQuotes(
      "Hello!", index, Iri::fromIriref("<http://www.w3.org/2001/XMLSchema#string>"));
  literal.getLiteral().replaceContent("Thüss!");
  EXPECT_THAT("Thüss!", asStringViewUnsafe(literal.getContent()));
  EXPECT_THAT("http://www.w3.org/2001/XMLSchema#string",
              asStringViewUnsafe(literal.getDatatype()));
  literal.getLiteral().replaceContent("Hi!");
  EXPECT_THAT("Hi!", asStringViewUnsafe(literal.getContent()));
  EXPECT_THAT("http://www.w3.org/2001/XMLSchema#string",
              asStringViewUnsafe(literal.getDatatype()));
  literal.getLiteral().replaceContent("Hello World!");
  EXPECT_THAT("Hello World!", asStringViewUnsafe(literal.getContent()));
  EXPECT_THAT("http://www.w3.org/2001/XMLSchema#string",
              asStringViewUnsafe(literal.getDatatype()));
  literal = LiteralOrIri::literalWithoutQuotes("Hello!", index);
  literal.getLiteral().replaceContent("Hi!");
  EXPECT_THAT("Hi!", asStringViewUnsafe(literal.getContent()));
  literal.getLiteral().replaceContent("Hello World!");
  EXPECT_THAT("Hello World!", asStringViewUnsafe(literal.getContent()));
}
// _______________________________________________________________________
TEST(LiteralTest, concat) {
  auto* qec = ad_utility::testing::getQec();
  const IndexImpl* index = &qec->getIndex().getImpl();

  LiteralOrIri literal = LiteralOrIri::literalWithoutQuotes("Hello ", index);
  LiteralOrIri literalOther = LiteralOrIri::literalWithoutQuotes("World!", index);
  literal.getLiteral().concat(literalOther.getLiteral());
  EXPECT_THAT("Hello World!", asStringViewUnsafe(literal.getContent()));
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  literal = LiteralOrIri::literalWithoutQuotes(
      "Hello ", index, Iri::fromIriref("<http://www.w3.org/2001/XMLSchema#string>"));
  literalOther = LiteralOrIri::literalWithoutQuotes(
      "World!", index, Iri::fromIriref("<http://www.w3.org/2001/XMLSchema#string>"));
  literal.getLiteral().concat(literalOther.getLiteral());
  EXPECT_THAT("Hello World!", asStringViewUnsafe(literal.getContent()));
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_TRUE(literal.hasDatatype());
  EXPECT_THAT("http://www.w3.org/2001/XMLSchema#string",
              asStringViewUnsafe(literal.getDatatype()));
  literal = LiteralOrIri::literalWithoutQuotes("Hello World!", index, "@en");
  literalOther = LiteralOrIri::literalWithoutQuotes("Bye!", index, "@en");
  literal.getLiteral().concat(literalOther.getLiteral());
  EXPECT_THAT("Hello World!Bye!", asStringViewUnsafe(literal.getContent()));
  EXPECT_TRUE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  EXPECT_THAT("en", asStringViewUnsafe(literal.getLanguageTag()));
  literal = LiteralOrIri::literalWithoutQuotes(
      "Hello ", index, Iri::fromIriref("<http://www.w3.org/2001/XMLSchema#string>"));
  literalOther = LiteralOrIri::literalWithoutQuotes("World!", index);
  literal.getLiteral().concat(literalOther.getLiteral());
  EXPECT_THAT("Hello World!", asStringViewUnsafe(literal.getContent()));
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  literal = LiteralOrIri::literalWithoutQuotes("Hello World!", index, "@en");
  literalOther = LiteralOrIri::literalWithoutQuotes("Bye!", index);
  literal.getLiteral().concat(literalOther.getLiteral());
  EXPECT_THAT("Hello World!Bye!", asStringViewUnsafe(literal.getContent()));
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  literal = LiteralOrIri::literalWithoutQuotes("Hello World!", index, "@en");
  literalOther = LiteralOrIri::literalWithoutQuotes("Thüss!", index, "@de");
  literal.getLiteral().concat(literalOther.getLiteral());
  EXPECT_THAT("Hello World!Thüss!", asStringViewUnsafe(literal.getContent()));
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
}

TEST(LiteralTest, spaceshipOperatorLangtagLiteral) {
  using namespace ad_utility::testing;
  auto* qec = getQec(TestIndexConfig{});
  const IndexImpl* index = &qec->getIndex().getImpl();

  LiteralOrIri l1 = LiteralOrIri::fromStringRepresentation(
      "\"Comparative evaluation of the protective effect of sodium "
      "valproate, phenazepam and ionol in stress-induced liver damage in "
      "rats\"@nl", index);
  LiteralOrIri l2 = LiteralOrIri::fromStringRepresentation(
      "\"Comparative evaluation of the protective effect of sodium "
      "valproate, phenazepam and ionol in stress-induced liver damage in "
      "rats\"@en", index);
  EXPECT_THAT(l1, testing::Not(testing::Eq(l2)));
  EXPECT_THAT(ql::compareThreeWay(l1, l2),
              testing::Not(testing::Eq(ql::strong_ordering::equal)));
}
