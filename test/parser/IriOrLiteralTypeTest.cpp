// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../util/GTestHelpers.h"
#include "parser/IriType.h"
#include "parser/LiteralOrIriType.h"
#include "parser/LiteralType.h"

TEST(IriTypeTest, IriTypeCreation) {
  IriType iri("http://www.wikidata.org/entity/Q3138");

  EXPECT_THAT("http://www.wikidata.org/entity/Q3138", iri.getIri());
}

TEST(LiteralTypeTest, LiteralTypeTest) {
  LiteralType literal("Hello World");

  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  EXPECT_THAT("Hello World", literal.getContent());
  EXPECT_THROW(literal.getLanguageTag(), ad_utility::Exception);
  EXPECT_THROW(literal.getDatatype(), ad_utility::Exception);
}

TEST(LiteralTypeTest, LiteralTypeTestWithDatatype) {
  LiteralType literal("Hello World", "xsd:string", LiteralDescriptor::DATATYPE);

  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_TRUE(literal.hasDatatype());
  EXPECT_THAT("Hello World", literal.getContent());
  EXPECT_THROW(literal.getLanguageTag(), ad_utility::Exception);
  EXPECT_THAT("xsd:string", literal.getDatatype());
}

TEST(LiteralTypeTest, LiteralTypeTestWithLanguagetag) {
  LiteralType literal("Hallo Welt", "de", LiteralDescriptor::LANGUAGE_TAG);

  EXPECT_TRUE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  EXPECT_THAT("Hallo Welt", literal.getContent());
  EXPECT_THAT("de", literal.getLanguageTag());
  EXPECT_THROW(literal.getDatatype(), ad_utility::Exception);
}

TEST(LiteralOrIriType, LiteralOrIriTypeWithIri) {
  LiteralOrIriType iri(IriType("http://www.wikidata.org/entity/Q3138"));

  EXPECT_TRUE(iri.isIri());
  EXPECT_NO_THROW(iri.getIriTypeObject());
  EXPECT_THAT("http://www.wikidata.org/entity/Q3138", iri.getIriString());
  EXPECT_FALSE(iri.isLiteral());
  EXPECT_THROW(iri.getLiteralTypeObject(), ad_utility::Exception);
  EXPECT_THROW(iri.hasLanguageTag(), ad_utility::Exception);
  EXPECT_THROW(iri.hasDatatype(), ad_utility::Exception);
  EXPECT_THROW(iri.getLiteralContent(), ad_utility::Exception);
  EXPECT_THROW(iri.getLanguageTag(), ad_utility::Exception);
  EXPECT_THROW(iri.getDatatype(), ad_utility::Exception);
}

TEST(LiteralOrIriType, LiteralOrIriTypeWithLiteral) {
  LiteralOrIriType literal(LiteralType("Hello World"));

  EXPECT_FALSE(literal.isIri());
  EXPECT_THROW(literal.getIriTypeObject(), ad_utility::Exception);
  EXPECT_THROW(literal.getIriString(), ad_utility::Exception);
  EXPECT_TRUE(literal.isLiteral());
  EXPECT_NO_THROW(literal.getLiteralTypeObject());
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  EXPECT_THAT("Hello World", literal.getLiteralContent());
  EXPECT_THROW(literal.getLanguageTag(), ad_utility::Exception);
  EXPECT_THROW(literal.getDatatype(), ad_utility::Exception);
}

TEST(LiteralOrIriType, LiteralOrIriTypeWithLiteralAndDatatype) {
  LiteralOrIriType literal(
      LiteralType("Hello World", "xsd:string", LiteralDescriptor::DATATYPE));

  EXPECT_FALSE(literal.isIri());
  EXPECT_THROW(literal.getIriTypeObject(), ad_utility::Exception);
  EXPECT_THROW(literal.getIriString(), ad_utility::Exception);
  EXPECT_TRUE(literal.isLiteral());
  EXPECT_NO_THROW(literal.getLiteralTypeObject());
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_TRUE(literal.hasDatatype());
  EXPECT_THAT("Hello World", literal.getLiteralContent());
  EXPECT_THROW(literal.getLanguageTag(), ad_utility::Exception);
  EXPECT_THAT("xsd:string", literal.getDatatype());
}

TEST(LiteralOrIriType, LiteralOrIriTypeWithLiteralAndLanguageTag) {
  LiteralOrIriType literal(
      LiteralType("Hej världen", "se", LiteralDescriptor::LANGUAGE_TAG));

  EXPECT_FALSE(literal.isIri());
  EXPECT_THROW(literal.getIriTypeObject(), ad_utility::Exception);
  EXPECT_THROW(literal.getIriString(), ad_utility::Exception);
  EXPECT_TRUE(literal.isLiteral());
  EXPECT_NO_THROW(literal.getLiteralTypeObject());
  EXPECT_TRUE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  EXPECT_THAT("Hej världen", literal.getLiteralContent());
  EXPECT_THAT("se", literal.getLanguageTag());
  EXPECT_THROW(literal.getDatatype(), ad_utility::Exception);
}
