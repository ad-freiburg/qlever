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

TEST(IriTest, IriCreation) {
  Iri iri = Iri::iriref("<http://www.wikidata.org/entity/Q3138>");

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
  Literal literal = Literal::literalWithoutQuotesWithDatatype(
      "Hello World", Iri::iriref("<http://www.w3.org/2001/XMLSchema#string>"));

  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_TRUE(literal.hasDatatype());
  EXPECT_THAT("Hello World", asStringViewUnsafe(literal.getContent()));
  EXPECT_THROW(literal.getLanguageTag(), ad_utility::Exception);
  EXPECT_THAT("http://www.w3.org/2001/XMLSchema#string",
              asStringViewUnsafe(literal.getDatatype().getContent()));
}

TEST(LiteralTest, LiteralTestWithLanguagetag) {
  Literal literal =
      Literal::literalWithoutQuotesWithLanguageTag("Hallo Welt", "@de");

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
      Iri::iriref("<http://www.wikidata.org/entity/>"), "Q3138");

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
  LiteralOrIri literal = LiteralOrIri::literalWithoutQuotesWithDatatype(
      "Hello World", Iri::iriref("<http://www.w3.org/2001/XMLSchema#string>"));

  EXPECT_FALSE(literal.isIri());
  EXPECT_THROW(literal.getIriContent(), ad_utility::Exception);
  EXPECT_TRUE(literal.isLiteral());
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_TRUE(literal.hasDatatype());
  EXPECT_THAT("Hello World", asStringViewUnsafe(literal.getLiteralContent()));
  EXPECT_THROW(literal.getLanguageTag(), ad_utility::Exception);
  EXPECT_THAT("http://www.w3.org/2001/XMLSchema#string",
              asStringViewUnsafe(literal.getDatatype().getContent()));
}

TEST(LiteralOrIri, LiteralOrIriWithLiteralWithQuotesAndDatatype) {
  LiteralOrIri literal = LiteralOrIri::literalWithQuotesWithDatatype(
      "\"Hello World\"",
      Iri::iriref("<http://www.w3.org/2001/XMLSchema#string>"));

  EXPECT_FALSE(literal.isIri());
  EXPECT_THROW(literal.getIriContent(), ad_utility::Exception);
  EXPECT_TRUE(literal.isLiteral());
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_TRUE(literal.hasDatatype());
  EXPECT_THAT("Hello World", asStringViewUnsafe(literal.getLiteralContent()));
  EXPECT_THROW(literal.getLanguageTag(), ad_utility::Exception);
  EXPECT_THAT("http://www.w3.org/2001/XMLSchema#string",
              asStringViewUnsafe(literal.getDatatype().getContent()));
}

TEST(LiteralOrIri, LiteralOrIriWithLiteralAndLanguageTag) {
  LiteralOrIri literal =
      LiteralOrIri::literalWithoutQuotesWithLanguageTag("Hej världen", "@se");

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
  LiteralOrIri literal = LiteralOrIri::literalWithQuotesWithLanguageTag(
      "'''Hej världen'''", "@se");

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
      LiteralOrIri::literalWithoutQuotesWithLanguageTag("Hello World", "@de");
  LiteralOrIri literalWithDatatype =
      LiteralOrIri::literalWithoutQuotesWithDatatype(
          "ABC", Iri::iriref("<https://example.org>"));

  EXPECT_THAT("https://example.org/books/book1",
              asStringViewUnsafe(iri.getContent()));
  EXPECT_THAT("Hello World",
              asStringViewUnsafe(literalWithLanguageTag.getContent()));
  EXPECT_THAT("ABC", asStringViewUnsafe(literalWithDatatype.getContent()));
}
