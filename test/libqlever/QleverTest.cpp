// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <gmock/gmock.h>

#include "../util/GTestHelpers.h"
#include "libqlever/Qlever.h"

using namespace qlever;
using namespace testing;

// _____________________________________________________________________________
TEST(LibQlever, buildIndexAndRunQuery) {
  std::string filename = "testIndexForLibQlever.ttl";
  {
    auto ofs = ad_utility::makeOfstream(filename);
    ofs << "<s> <p> <o>. <s2> <p> \"kartoffel und salat\".";
  }

  IndexBuilderConfig c;
  c.inputFiles_.emplace_back(filename, Filetype::Turtle);
  c.baseName_ = "testIndexForLibQlever";

  // Test the activation of the memory limit
  c.memoryLimit_ = ad_utility::MemorySize::bytes(0);
  EXPECT_ANY_THROW(Qlever::buildIndex(c));

  c.memoryLimit_ = std::nullopt;

  c.parserBufferSize_ = ad_utility::MemorySize::bytes(0);
  EXPECT_ANY_THROW(Qlever::buildIndex(c));

  c.parserBufferSize_ = std::nullopt;
  EXPECT_NO_THROW(Qlever::buildIndex(c));
  {
    EngineConfig ec{c};
    Qlever engine{ec};
    // Run a simple query.
    auto res = engine.query("SELECT ?s WHERE { ?s <p> <o> }",
                            ad_utility::MediaType::tsv);
    EXPECT_EQ(res, "?s\n<s>\n");

    // Run with a different media type
    res = engine.query("SELECT * WHERE { <s> <p> ?o }",
                       ad_utility::MediaType::csv);
    EXPECT_EQ(res, "o\no\n");

    // Separately run the planning and the query.
    auto plan = engine.parseAndPlanQuery("SELECT * WHERE { <s> <p> ?o }");
    res = engine.query(plan, ad_utility::MediaType::csv);
    EXPECT_EQ(res, "o\no\n");
  }

  c.addWordsFromLiterals_ = true;
  // TODO<joka921, flixtastic> With this change we have a chance to finally test
  // all those configs, for now we just artificially increase the test coverage.
  EXPECT_NO_THROW(Qlever::buildIndex(c));
  EngineConfig ec{c};
  ec.loadTextIndex_ = true;
  Qlever engine{ec};
}

// _____________________________________________________________________________
TEST(IndexBuilderConfig, validate) {
  IndexBuilderConfig c;
  EXPECT_NO_THROW(c.validate());

  c.kScoringParam_ = -3;
  AD_EXPECT_THROW_WITH_MESSAGE(c.validate(), HasSubstr("must be >= 0"));

  c = IndexBuilderConfig{};
  c.bScoringParam_ = -3;
  AD_EXPECT_THROW_WITH_MESSAGE(c.validate(), HasSubstr("must be between"));
  c.bScoringParam_ = 1.1;
  AD_EXPECT_THROW_WITH_MESSAGE(c.validate(), HasSubstr("must be between"));

  c = IndexBuilderConfig{};
  c.wordsfile_ = "blibb";
  AD_EXPECT_THROW_WITH_MESSAGE(c.validate(),
                               HasSubstr("Only specified wordsfile"));
  c.docsfile_ = "blabb";
  EXPECT_NO_THROW(c.validate());
  c.wordsfile_ = "";
  AD_EXPECT_THROW_WITH_MESSAGE(c.validate(),
                               HasSubstr("Only specified docsfile"));
}

// TODO<joka921> Add some initial tests for the building and querying of
// indices.
