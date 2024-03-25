// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../util/GTestHelpers.h"
#include "parser/Iri.h"
#include "parser/Literal.h"
#include "parser/LiteralOrIri.h"
#include "parser/NormalizedString.h"

using namespace ad_utility::triple_component;

TEST(IriTest, IriCreation) {
  Iri iri = Iri::fromIriref("<http://www.wikidata.org/entity/Q3138>");

  EXPECT_THAT("http://www.wikidata.org/entity/Q3138",
              asStringViewUnsafe(iri.getContent()));
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
