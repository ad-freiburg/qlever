// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include <string>

#include "../../util/GTestHelpers.h"
#include "../QleverTestHelpers.h"
#include "libqlever/Qlever.h"
#include "libqlever/blobDiff/TurtleDelta.h"
#include "parser/BlankNodeAdder.h"
#include "util/File.h"
#include "util/FilesystemHelpers.h"

using namespace qlever;
using namespace qlever::blobDiff;
using namespace testing;

namespace {
constexpr std::string_view oldData =
    "<a> <p> \"1\"^^<http://www.w3.org/2001/XMLSchema#int> .\n"
    "<a> <q> \"x\" .\n"
    "<a> <r> <b> .\n";
}  // namespace

// _____________________________________________________________________________
TEST(TurtleDelta, computeAndApplyTripleDelta) {
  auto indexConfig = ad_utility::testing::buildTestIndex(oldData);
  absl::Cleanup cleanupIndex = [baseName = indexConfig.baseName_] {
    for (const auto& file :
         qlever::util::filesWithBaseNameAndSuffix(baseName, "")) {
      ad_utility::deleteFile(file);
    }
  };
  std::string oldFile = absl::StrCat(gtestCurrentTestName(), ".old.ttl");
  std::string newFile = absl::StrCat(gtestCurrentTestName(), ".new.ttl");
  absl::Cleanup cleanupFiles = [&oldFile, &newFile] {
    ad_utility::deleteFile(oldFile);
    ad_utility::deleteFile(newFile);
  };
  {
    auto stream = ad_utility::makeOfstream(oldFile);
    stream << oldData;
  }
  {
    // A change of the lexical form of the integer is no change, a duplicate
    // triple counts once, a literal that is not in the vocabulary of the index
    // and a change of the type (from integer to decimal) are changes.
    auto stream = ad_utility::makeOfstream(newFile);
    stream << "<a> <p> \"01\"^^<http://www.w3.org/2001/XMLSchema#int> .\n"
              "<a> <p> \"01\"^^<http://www.w3.org/2001/XMLSchema#int> .\n"
              "<a> <q> \"new word\" .\n"
              "<a> <r> <b> .\n"
              "<a> <s> 1.0 .\n";
  }

  Qlever qlever{EngineConfig{indexConfig}};
  auto snapshot = qlever.indexAndViewsSnapshot();
  Index& index = snapshot->index_;
  BlankNodeAdder adder{index.getBlankNodeManager()};

  // Without changes, the delta is empty.
  auto unchanged = computeTripleDelta(oldFile, oldFile, index, adder);
  EXPECT_TRUE(unchanged.toInsert_.empty());
  EXPECT_TRUE(unchanged.toDelete_.empty());

  auto delta = computeTripleDelta(oldFile, newFile, index, adder);
  EXPECT_EQ(delta.toInsert_.size(), 2u);
  EXPECT_EQ(delta.toDelete_.size(), 1u);
  EXPECT_TRUE(ql::ranges::is_sorted(delta.toInsert_));

  // The reverse delta swaps the insertions and the deletions.
  auto reverse = computeTripleDelta(newFile, oldFile, index, adder);
  EXPECT_EQ(reverse.toInsert_, delta.toDelete_);
  EXPECT_EQ(reverse.toDelete_, delta.toInsert_);

  applyTripleDelta(index, std::move(delta));
  EXPECT_EQ(qlever.query("SELECT ?p ?o { <a> ?p ?o } ORDER BY ?p",
                         ad_utility::MediaType::tsv),
            "?p\t?o\n<p>\t1\n<q>\t\"new word\"\n<r>\t<b>\n<s>\t1.0\n");
}
