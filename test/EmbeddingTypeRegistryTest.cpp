// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Sebastian Walter <sebastian.walter98@gmail.com>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "index/EmbeddingTypeRegistry.h"
#include "index/Index.h"
#include "util/GTestHelpers.h"
#include "util/IndexTestHelpers.h"

using namespace ad_utility::testing;
using ::testing::HasSubstr;

namespace {
// The QLever-owned `emb:` prefix that the embedding-type metadata vocabulary
// lives under (deliberately not under `builtin-functions/`, so the declaration
// triples survive index building; see `Constants.h`).
constexpr std::string_view kEmbPrefix =
    "@prefix emb: <http://qlever.cs.uni-freiburg.de/embeddings/> .\n"
    "@prefix ex: <http://example.org/> .\n";

// A complete, valid single-type declaration: the type `ex:T` is recognized
// because it is the object of an `emb:type` triple, and it carries the three
// mandatory metadata properties.
std::string oneValidType() {
  return absl::StrCat(kEmbPrefix,
                      "ex:T emb:hasMetric emb:cosine ; emb:hasDimension 3 ; "
                      "emb:hasPrecision emb:fp32 .\n"
                      "ex:n1 emb:type ex:T .\n");
}
}  // namespace

// _____________________________________________________________________________
TEST(EmbeddingTypeRegistry, ParsesValidDeclaration) {
  auto index = makeTestIndex(oneValidType());
  const auto& registry = index.getEmbeddingTypeRegistry();

  EXPECT_FALSE(registry.empty());
  EXPECT_EQ(registry.numTypes(), 1);

  auto getId = makeGetId(index);
  Id typeId = getId("<http://example.org/T>");
  const EmbeddingTypeConfig* config = registry.getConfig(typeId);
  ASSERT_NE(config, nullptr);
  EXPECT_EQ(config->dimension_, 3u);
  EXPECT_EQ(config->precision_, "fp32");
  EXPECT_EQ(config->metric_, EmbeddingMetric::Cosine);

  // A non-type IRI is not a known embedding type.
  EXPECT_EQ(registry.getConfig(getId("<http://example.org/n1>")), nullptr);
}

// _____________________________________________________________________________
TEST(EmbeddingTypeRegistry, AllSupportedMetricsAreParsed) {
  auto check = [](std::string_view metricLocalName, EmbeddingMetric expected) {
    std::string turtle =
        absl::StrCat(kEmbPrefix, "ex:T emb:hasMetric emb:", metricLocalName,
                     " ; emb:hasDimension 3 ; emb:hasPrecision emb:fp32 .\n"
                     "ex:n1 emb:type ex:T .\n");
    auto index = makeTestIndex(std::move(turtle));
    const auto& registry = index.getEmbeddingTypeRegistry();
    auto getId = makeGetId(index);
    const auto* config = registry.getConfig(getId("<http://example.org/T>"));
    ASSERT_NE(config, nullptr);
    EXPECT_EQ(config->metric_, expected);
  };
  check("cosine", EmbeddingMetric::Cosine);
  check("l2", EmbeddingMetric::L2);
  check("squaredL2", EmbeddingMetric::SquaredL2);
  check("dotProduct", EmbeddingMetric::DotProduct);
}

// _____________________________________________________________________________
TEST(EmbeddingTypeRegistry, MultipleTypesAreRegistered) {
  std::string turtle =
      absl::StrCat(kEmbPrefix,
                   "ex:T1 emb:hasMetric emb:cosine ; emb:hasDimension 3 ; "
                   "emb:hasPrecision emb:fp32 .\n"
                   "ex:T2 emb:hasMetric emb:l2 ; emb:hasDimension 5 ; "
                   "emb:hasPrecision emb:fp32 .\n"
                   "ex:n1 emb:type ex:T1 .\nex:n2 emb:type ex:T2 .\n");
  auto index = makeTestIndex(std::move(turtle));
  const auto& registry = index.getEmbeddingTypeRegistry();

  EXPECT_EQ(registry.numTypes(), 2);

  auto getId = makeGetId(index);
  const auto* t1 = registry.getConfig(getId("<http://example.org/T1>"));
  const auto* t2 = registry.getConfig(getId("<http://example.org/T2>"));
  ASSERT_NE(t1, nullptr);
  ASSERT_NE(t2, nullptr);
  EXPECT_EQ(t1->dimension_, 3u);
  EXPECT_EQ(t1->metric_, EmbeddingMetric::Cosine);
  EXPECT_EQ(t2->dimension_, 5u);
  EXPECT_EQ(t2->metric_, EmbeddingMetric::L2);
}

// _____________________________________________________________________________
TEST(EmbeddingTypeRegistry, NoEmbeddingTypes) {
  auto index = makeTestIndex(
      "<http://example.org/a> <http://example.org/b> "
      "<http://example.org/c> .");
  const auto& registry = index.getEmbeddingTypeRegistry();
  EXPECT_TRUE(registry.empty());
  EXPECT_EQ(registry.numTypes(), 0);
}

// _____________________________________________________________________________
TEST(EmbeddingTypeRegistry, MissingMetricIsLoadError) {
  std::string turtle =
      absl::StrCat(kEmbPrefix,
                   "ex:T emb:hasDimension 3 ; emb:hasPrecision emb:fp32 .\n"
                   "ex:n1 emb:type ex:T .\n");
  AD_EXPECT_THROW_WITH_MESSAGE(
      makeTestIndex(std::move(turtle)),
      HasSubstr("missing the mandatory field emb:hasMetric"));
}

// _____________________________________________________________________________
TEST(EmbeddingTypeRegistry, MissingDimensionIsLoadError) {
  std::string turtle = absl::StrCat(
      kEmbPrefix,
      "ex:T emb:hasMetric emb:cosine ; emb:hasPrecision emb:fp32 .\n"
      "ex:n1 emb:type ex:T .\n");
  AD_EXPECT_THROW_WITH_MESSAGE(
      makeTestIndex(std::move(turtle)),
      HasSubstr("missing the mandatory field emb:hasDimension"));
}

// _____________________________________________________________________________
TEST(EmbeddingTypeRegistry, MissingPrecisionIsLoadError) {
  std::string turtle =
      absl::StrCat(kEmbPrefix,
                   "ex:T emb:hasMetric emb:cosine ; emb:hasDimension 3 .\n"
                   "ex:n1 emb:type ex:T .\n");
  AD_EXPECT_THROW_WITH_MESSAGE(
      makeTestIndex(std::move(turtle)),
      HasSubstr("missing the mandatory field emb:hasPrecision"));
}

// _____________________________________________________________________________
TEST(EmbeddingTypeRegistry, UnsupportedMetricIsLoadError) {
  std::string turtle =
      absl::StrCat(kEmbPrefix,
                   "ex:T emb:hasMetric emb:euclidean ; emb:hasDimension 3 ; "
                   "emb:hasPrecision emb:fp32 .\n"
                   "ex:n1 emb:type ex:T .\n");
  AD_EXPECT_THROW_WITH_MESSAGE(makeTestIndex(std::move(turtle)),
                               HasSubstr("unsupported emb:hasMetric"));
}

// _____________________________________________________________________________
TEST(EmbeddingTypeRegistry, UnsupportedPrecisionIsLoadError) {
  std::string turtle =
      absl::StrCat(kEmbPrefix,
                   "ex:T emb:hasMetric emb:cosine ; emb:hasDimension 3 ; "
                   "emb:hasPrecision emb:fp16 .\n"
                   "ex:n1 emb:type ex:T .\n");
  AD_EXPECT_THROW_WITH_MESSAGE(makeTestIndex(std::move(turtle)),
                               HasSubstr("unsupported emb:hasPrecision"));
}

// _____________________________________________________________________________
TEST(EmbeddingTypeRegistry, MetricAsStringLiteralIsLoadError) {
  // The metric must be one of the predefined `emb:` IRIs, not a string literal.
  std::string turtle =
      absl::StrCat(kEmbPrefix,
                   "ex:T emb:hasMetric \"cosine\" ; emb:hasDimension 3 ; "
                   "emb:hasPrecision emb:fp32 .\n"
                   "ex:n1 emb:type ex:T .\n");
  AD_EXPECT_THROW_WITH_MESSAGE(
      makeTestIndex(std::move(turtle)),
      HasSubstr("expected to be one of the predefined IRIs"));
}

// _____________________________________________________________________________
TEST(EmbeddingTypeRegistry, InvalidDimensionIsLoadError) {
  std::string turtle =
      absl::StrCat(kEmbPrefix,
                   "ex:T emb:hasMetric emb:cosine ; emb:hasDimension 0 ; "
                   "emb:hasPrecision emb:fp32 .\n"
                   "ex:n1 emb:type ex:T .\n");
  AD_EXPECT_THROW_WITH_MESSAGE(makeTestIndex(std::move(turtle)),
                               HasSubstr("invalid emb:hasDimension"));
}
