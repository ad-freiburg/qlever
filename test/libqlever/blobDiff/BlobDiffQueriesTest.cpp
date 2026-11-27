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
  // Comments and whitespace between and after the declarations belong to the
  // prologue, an IRI may contain a `#`.
  query =
      "# A comment.\nPREFIX ex: <http://ex.org/#> # PREFIX x: <y>\n"
      "SELECT * {}";
  EXPECT_EQ(prologueLength(query), query.find("SELECT"));
  // Multibyte characters are counted in bytes.
  query = "PREFIX ex: <http://ex.org/\u00e4\u00f6> SELECT * {}";
  EXPECT_EQ(prologueLength(query), query.find("SELECT"));
  // A keyword that is only a prefix of the remaining query does not match.
  EXPECT_EQ(prologueLength("PREFI"), 0u);
  // A malformed prologue is rejected.
  EXPECT_ANY_THROW(
      prologueLength("PREFIX ex: <http://ex.org/> PREFIX broken: <http"));
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

  // The keys of an explicit `INTERNAL SORT BY` come first (deduplicated), the
  // remaining variables follow in the order of their names.
  EXPECT_EQ(withCanonicalSortOrder(qlever,
                                   "SELECT ?a ?b ?c { ?a <p> ?b . ?a <p> ?c } "
                                   "INTERNAL SORT BY ?c ?b ?c"),
            " SELECT * WHERE { { SELECT ?a ?b ?c { ?a <p> ?b . ?a <p> ?c } "
            "INTERNAL SORT BY ?c ?b ?c } } INTERNAL SORT BY ?c ?b ?a");
  // A semantic `ORDER BY` is not kept.
  EXPECT_EQ(withCanonicalSortOrder(
                qlever, "SELECT ?x ?y { ?x <p> ?y } ORDER BY DESC(?y)"),
            " SELECT * WHERE { { SELECT ?x ?y { ?x <p> ?y } ORDER BY DESC(?y) "
            "} } INTERNAL SORT BY ?x ?y");
  // The keys of an `INTERNAL SORT BY` have to be selected.
  AD_EXPECT_THROW_WITH_MESSAGE(
      withCanonicalSortOrder(qlever,
                             "SELECT ?x { ?x <p> ?y } INTERNAL SORT BY ?y"),
      HasSubstr("The variable ?y of the `INTERNAL SORT BY` clause"));

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

  // An explicit `INTERNAL SORT BY` of a pinned query is respected (without it,
  // the result would be sorted by `?o` first).
  BlobDiffConfig explicitlySorted;
  explicitlySorted.namedQueries_.push_back(
      {"sorted",
       "SELECT ?s ?o { VALUES (?s ?o) { (<a> <d>) (<c> <b>) } } INTERNAL SORT "
       "BY ?s",
       std::nullopt, std::nullopt});
  pinAllQueries(qlever, explicitlySorted);
  EXPECT_EQ(readPinned("sorted"), "?s\t?o\n<a>\t<d>\n<c>\t<b>\n");
}
