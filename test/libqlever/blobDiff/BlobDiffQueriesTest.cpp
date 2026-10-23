// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>

#include <string>

#include "../../util/GTestHelpers.h"
#include "../QleverTestHelpers.h"
#include "libqlever/Qlever.h"
#include "libqlever/blobDiff/BlobDiffQueries.h"
#include "util/File.h"
#include "util/FilesystemHelpers.h"

using namespace qlever;
using namespace qlever::blobDiff;
using namespace testing;

namespace {
// Build a small test index (named after the running test) and return the
// config to load it. The index files are deleted by the returned cleanup.
auto buildIndexWithCleanup() {
  auto config = ad_utility::testing::buildTestIndex(
      "<a> <p> <b> . <a> <q> \"x\" . <c> <p> <d> .");
  auto cleanup = absl::MakeCleanup([baseName = config.baseName_] {
    for (const auto& file :
         qlever::util::filesWithBaseNameAndSuffix(baseName, "")) {
      ad_utility::deleteFile(file);
    }
  });
  return std::pair{std::move(config), std::move(cleanup)};
}
}  // namespace

// _____________________________________________________________________________
TEST(BlobDiffQueries, prologueLength) {
  EXPECT_EQ(prologueLength(""), 0u);
  EXPECT_EQ(prologueLength("SELECT * { ?s ?p ?o }"), 0u);
  // The whitespace in front of the query body is part of the prologue.
  EXPECT_EQ(prologueLength("  SELECT * { ?s ?p ?o }"), 2u);
  std::string query =
      "PREFIX ex: <http://ex.org/>\n base <http://base/> "
      "SELECT * { ?s ex:p ?o }";
  EXPECT_EQ(prologueLength(query), query.find("SELECT"));
  // The keywords are case-insensitive.
  query = "Prefix ex: <http://ex.org/> pReFiX ex2: <http://ex2/> ASK {}";
  EXPECT_EQ(prologueLength(query), query.find("ASK"));
  // A declaration without a closing `>` ends the prologue.
  query = "PREFIX ex: <http://ex.org/> PREFIX broken: <http";
  EXPECT_EQ(prologueLength(query), query.find("PREFIX broken"));
  // A keyword that is only a prefix of the remaining query does not match.
  EXPECT_EQ(prologueLength("PREFI"), 0u);
}

// _____________________________________________________________________________
TEST(BlobDiffQueries, withCanonicalSortOrder) {
  auto [config, cleanup] = buildIndexWithCleanup();
  Qlever qlever{EngineConfig{config}};

  // The variables are sorted by name and deduplicated, the prologue is moved
  // in front of the wrapping query.
  EXPECT_EQ(
      withCanonicalSortOrder(qlever,
                             "PREFIX ex: <http://ex/> SELECT ?y ?x WHERE { ?x "
                             "ex:p ?y }"),
      "PREFIX ex: <http://ex/>  SELECT * WHERE { { SELECT ?y ?x WHERE "
      "{ ?x ex:p ?y } } } INTERNAL SORT BY ?x ?y");
  EXPECT_EQ(withCanonicalSortOrder(qlever, "SELECT * { ?s <p> ?o }"),
            " SELECT * WHERE { { SELECT * { ?s <p> ?o } } } INTERNAL SORT BY "
            "?o ?s");

  // A query without variables is returned unchanged.
  std::string noVariables = "SELECT * { <a> <p> <b> }";
  EXPECT_EQ(withCanonicalSortOrder(qlever, noVariables), noVariables);

  // Only `SELECT` queries can be pinned.
  AD_EXPECT_THROW_WITH_MESSAGE(
      withCanonicalSortOrder(qlever, "ASK { ?s ?p ?o }"),
      HasSubstr("Only SELECT queries can be pinned"));
}

// _____________________________________________________________________________
TEST(BlobDiffQueries, pinAllQueries) {
  auto [indexConfig, cleanup] = buildIndexWithCleanup();
  Qlever qlever{EngineConfig{indexConfig}};
  auto configWithQueries = [](std::vector<std::string> names) {
    BlobDiffConfig config;
    for (auto& name : names) {
      config.namedQueries_.push_back({std::move(name),
                                      "SELECT ?o ?s { ?s <p> ?o }",
                                      std::nullopt, std::nullopt});
    }
    return config;
  };

  pinAllQueries(qlever, configWithQueries({"first", "second"}));
  EXPECT_TRUE(qlever.namedResultCache().contains("first"));
  EXPECT_TRUE(qlever.namedResultCache().contains("second"));
  // The pinned result is sorted by all of its variables.
  auto readPinned = [&qlever](const std::string& name) {
    return qlever.query(
        absl::StrCat("SELECT ?s ?o { SERVICE ql:cached-result-with-name-", name,
                     " {} }"),
        ad_utility::MediaType::tsv);
  };
  EXPECT_EQ(readPinned("first"), "?s\t?o\n<a>\t<b>\n<c>\t<d>\n");

  // Pinning again drops the results that are no longer in the config.
  pinAllQueries(qlever, configWithQueries({"third"}));
  EXPECT_FALSE(qlever.namedResultCache().contains("first"));
  EXPECT_FALSE(qlever.namedResultCache().contains("second"));
  EXPECT_TRUE(qlever.namedResultCache().contains("third"));
}
