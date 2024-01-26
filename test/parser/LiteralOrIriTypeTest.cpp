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
#include "parser/RdfEscaping.h"

TEST(IriTypeTest, IriTypeCreation) {
  Iri iri(RdfEscaping::normalizeIriWithBrackets(
      "<http://www.wikidata.org/entity/Q3138>"));

  EXPECT_THAT("http://www.wikidata.org/entity/Q3138",
              asStringViewUnsafe(iri.getContent()));
}

TEST(LiteralTypeTest, LiteralTypeTest) {
  Literal literal(RdfEscaping::normalizeLiteralWithoutQuotes("Hello World"));

  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  EXPECT_THAT("Hello World", asStringViewUnsafe(literal.getContent()));
  EXPECT_THROW(literal.getLanguageTag(), ad_utility::Exception);
  EXPECT_THROW(literal.getDatatype(), ad_utility::Exception);
}

TEST(LiteralTypeTest, LiteralTypeTestWithDatatype) {
  Literal literal(RdfEscaping::normalizeLiteralWithoutQuotes("Hello World"),
                  Iri(RdfEscaping::normalizeIriWithBrackets(
                      "<http://www.w3.org/2001/XMLSchema#string>")));

  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_TRUE(literal.hasDatatype());
  EXPECT_THAT("Hello World", asStringViewUnsafe(literal.getContent()));
  EXPECT_THROW(literal.getLanguageTag(), ad_utility::Exception);
  EXPECT_THAT("http://www.w3.org/2001/XMLSchema#string",
              asStringViewUnsafe(literal.getDatatype().getContent()));
}

TEST(LiteralTypeTest, LiteralTypeTestWithLanguagetag) {
  Literal literal(RdfEscaping::normalizeLiteralWithoutQuotes("Hallo Welt"),
                  RdfEscaping::normalizeLanguageTag("@de"));

  EXPECT_TRUE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  EXPECT_THAT("Hallo Welt", asStringViewUnsafe(literal.getContent()));
  EXPECT_THAT("de", asStringViewUnsafe(literal.getLanguageTag()));
  EXPECT_THROW(literal.getDatatype(), ad_utility::Exception);
}

TEST(LiteralOrIriType, LiteralOrIriTypeWithIri) {
  LiteralOrIri iri(Iri(RdfEscaping::normalizeIriWithoutBrackets(
      "http://www.wikidata.org/entity/Q3138")));

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

TEST(LiteralOrIriType, LiteralOrIriTypeWithLiteral) {
  LiteralOrIri literal(
      Literal(RdfEscaping::normalizeLiteralWithoutQuotes("Hello World")));

  EXPECT_FALSE(literal.isIri());
  EXPECT_THROW(literal.getIriContent(), ad_utility::Exception);
  EXPECT_TRUE(literal.isLiteral());
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  EXPECT_THAT("Hello World", asStringViewUnsafe(literal.getLiteralContent()));
  EXPECT_THROW(literal.getLanguageTag(), ad_utility::Exception);
  EXPECT_THROW(literal.getDatatype(), ad_utility::Exception);
}

TEST(LiteralOrIriType, LiteralOrIriTypeWithLiteralAndDatatype) {
  LiteralOrIri literal(
      Literal(RdfEscaping::normalizeLiteralWithoutQuotes("Hello World"),
              Iri(RdfEscaping::normalizeIriWithoutBrackets(
                  "http://www.w3.org/2001/XMLSchema#string"))));

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

TEST(LiteralOrIriType, LiteralOrIriTypeWithLiteralAndLanguageTag) {
  LiteralOrIri literal(
      Literal(RdfEscaping::normalizeLiteralWithoutQuotes("Hej världen"),
              RdfEscaping::normalizeLanguageTag("se")));

  EXPECT_FALSE(literal.isIri());
  EXPECT_THROW(literal.getIriContent(), ad_utility::Exception);
  EXPECT_TRUE(literal.isLiteral());
  EXPECT_TRUE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  EXPECT_THAT("Hej världen", asStringViewUnsafe(literal.getLiteralContent()));
  EXPECT_THAT("se", asStringViewUnsafe(literal.getLanguageTag()));
  EXPECT_THROW(literal.getDatatype(), ad_utility::Exception);
}
