// Copyright 2023 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../util/GTestHelpers.h"
#include "parser/Iri.h"
#include "parser/Literal.h"
#include "parser/LiteralOrIri.h"
#include "parser/NormalizedString.h"
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
  LiteralOrIri iri =
      LiteralOrIri::iriref("<http://www.wikidata.org/entity/Q3138>");

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
  LiteralOrIri iri = LiteralOrIri::prefixedIri(
      Iri::fromIriref("<http://www.wikidata.org/entity/>"), "Q3138");

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
  LiteralOrIri literal = LiteralOrIri::literalWithoutQuotes("Hello World");

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
  LiteralOrIri literal = LiteralOrIri::literalWithQuotes("\"Hello World\"");

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
  LiteralOrIri literal = LiteralOrIri::literalWithoutQuotes(
      "Hello World",
      Iri::fromIriref("<http://www.w3.org/2001/XMLSchema#string>"));

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
  LiteralOrIri literal = LiteralOrIri::literalWithQuotes(
      "\"Hello World\"",
      Iri::fromIriref("<http://www.w3.org/2001/XMLSchema#string>"));

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
  LiteralOrIri literal =
      LiteralOrIri::literalWithoutQuotes("Hej världen", "@se");

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
  LiteralOrIri literal =
      LiteralOrIri::literalWithQuotes("'''Hej världen'''", "@se");

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
  LiteralOrIri iri = LiteralOrIri::iriref("<https://example.org/books/book1>");
  LiteralOrIri literalWithLanguageTag =
      LiteralOrIri::literalWithoutQuotes("Hello World", "@de");
  LiteralOrIri literalWithDatatype = LiteralOrIri::literalWithoutQuotes(
      "ABC", Iri::fromIriref("<https://example.org>"));

  EXPECT_THAT("https://example.org/books/book1",
              asStringViewUnsafe(iri.getContent()));
  EXPECT_THAT("Hello World",
              asStringViewUnsafe(literalWithLanguageTag.getContent()));
  EXPECT_THAT("ABC", asStringViewUnsafe(literalWithDatatype.getContent()));
}

TEST(LiteralOrIri, EnsureLiteralsAreEncoded) {
  LiteralOrIri literal1 =
      LiteralOrIri::literalWithQuotes(R"("This is to be \"\\ encoded")");
  EXPECT_THAT(R"(This is to be "\ encoded)",
              asStringViewUnsafe(literal1.getContent()));

  LiteralOrIri literal2 =
      LiteralOrIri::literalWithoutQuotes(R"(This is to be \"\\ encoded)");
  EXPECT_THAT(R"(This is to be "\ encoded)",
              asStringViewUnsafe(literal2.getContent()));
}

TEST(LiteralOrIri, Printing) {
  LiteralOrIri literal1 = LiteralOrIri::literalWithoutQuotes("hallo");
  std::stringstream str;
  PrintTo(literal1, &str);
  EXPECT_EQ(str.str(), "\"hallo\"");
}

TEST(LiteralOrIri, Hashing) {
  auto lit = LiteralOrIri::literalWithoutQuotes("bimbamm");
  auto iri = LiteralOrIri::iriref("<bimbamm>");
  ad_utility::HashSet<LiteralOrIri> set{lit, iri};
  EXPECT_THAT(set, ::testing::UnorderedElementsAre(lit, iri));
}
