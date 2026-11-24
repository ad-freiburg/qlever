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
#include "../../util/IndexTestHelpers.h"
#include "../../util/TripleComponentTestHelpers.h"
#include "../QleverTestHelpers.h"
#include "libqlever/Qlever.h"
#include "libqlever/blobDiff/TurtleDelta.h"
#include "parser/BlankNodeAdder.h"
#include "parser/RdfParser.h"
#include "parser/Tokenizer.h"
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

// Test `detail::parseTurtleFile`: the triples of the file are returned, and the
// messages of a parse error and of a blank node (labeled or anonymous, in any
// position) name the file.
TEST(TurtleDelta, parseTurtleFile) {
  const Index& index =
      ad_utility::testing::getQec(std::string{oldData})->getIndex();
  std::string filename = absl::StrCat(gtestCurrentTestName(), ".ttl");
  absl::Cleanup cleanup = [&filename] { ad_utility::deleteFile(filename); };
  ad_utility::makeOfstream(filename) << oldData;
  auto triples = blobDiff::detail::parseTurtleFile(filename, index);
  ASSERT_EQ(triples.size(), 3u);
  EXPECT_EQ(triples[2].subject_, ad_utility::testing::iri("<a>"));
  EXPECT_EQ(triples[2].predicate_, ad_utility::testing::iri("<r>"));
  EXPECT_EQ(triples[2].object_, ad_utility::testing::iri("<b>"));

  ad_utility::makeOfstream(filename) << "<a> <p> .\n";
  AD_EXPECT_THROW_WITH_MESSAGE(
      blobDiff::detail::parseTurtleFile(filename, index),
      HasSubstr(absl::StrCat("Parse error in \"", filename, "\"")));

  for (std::string_view withBlankNode :
       {"<a> <p> <b> .\n_:b <p> <c> .\n", "<a> <p> _:b .\n", "<a> <p> [] .\n",
        "<a> <p> [ <q> <c> ] .\n"}) {
    ad_utility::makeOfstream(filename) << withBlankNode;
    AD_EXPECT_THROW_WITH_MESSAGE(
        blobDiff::detail::parseTurtleFile(filename, index),
        AllOf(HasSubstr(absl::StrCat("of \"", filename, "\"")),
              HasSubstr("contains a blank node")));
  }
}

// Test `detail::toIdTriples`: the result is sorted and free of duplicates, all
// triples are in the same (default) graph, equal values with a different
// lexical form get the same `Id`, a word that is not in the vocabulary becomes
// a `LocalVocabIndex`, and blank nodes are rejected.
TEST(TurtleDelta, toIdTriples) {
  const Index& index =
      ad_utility::testing::getQec(std::string{oldData})->getIndex();
  BlankNodeAdder adder{index.getBlankNodeManager()};
  auto triples =
      RdfStringParser<TurtleParser<Tokenizer>>::parseTriplesFromString(
          "<a> <p> \"1\"^^<http://www.w3.org/2001/XMLSchema#int> .\n"
          "<a> <p> \"01\"^^<http://www.w3.org/2001/XMLSchema#int> .\n"
          "<a> <q> \"x\" .\n"
          "<a> <q> \"x\" .\n"
          "<c> <p> \"brand new\" .\n",
          &index.encodedIriManager());
  auto idTriples =
      blobDiff::detail::toIdTriples(std::move(triples), index, adder);
  ASSERT_EQ(idTriples.size(), 3u);
  EXPECT_TRUE(ql::ranges::is_sorted(idTriples));
  for (const auto& triple : idTriples) {
    EXPECT_EQ(triple.ids()[3], idTriples[0].ids()[3]);
  }

  auto getId = ad_utility::testing::makeGetId(index);
  auto fromA = ql::ranges::count_if(idTriples, [&getId](const auto& triple) {
    return triple.ids()[0] == getId("<a>");
  });
  EXPECT_EQ(fromA, 2);
  // `<c>` and `"brand new"` are not in the vocabulary of the index.
  auto fromC = ql::ranges::find_if(idTriples, [](const auto& triple) {
    return triple.ids()[2].getDatatype() == Datatype::LocalVocabIndex;
  });
  ASSERT_NE(fromC, idTriples.end());
  EXPECT_EQ(fromC->ids()[0].getDatatype(), Datatype::LocalVocabIndex);

  auto withBlankNode =
      RdfStringParser<TurtleParser<Tokenizer>>::parseTriplesFromString(
          "<a> <p> _:b .\n", &index.encodedIriManager());
  AD_EXPECT_THROW_WITH_MESSAGE(
      blobDiff::detail::toIdTriples(std::move(withBlankNode), index, adder),
      HasSubstr("Blank nodes are not supported"));
}

// Test `computeTripleDelta` and `applyTripleDelta` end to end on a small index.
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

  std::string blankFile = absl::StrCat(gtestCurrentTestName(), ".blank.ttl");
  absl::Cleanup cleanupBlankFile = [&blankFile] {
    ad_utility::deleteFile(blankFile);
  };
  ad_utility::makeOfstream(blankFile) << "<a> <p> _:b .\n";

  Qlever qlever{EngineConfig{indexConfig}};
  auto snapshot = qlever.indexAndViewsSnapshot();
  Index& index = snapshot->index_;
  BlankNodeAdder adder{index.getBlankNodeManager()};

  // A blank node in either of the two files is rejected.
  AD_EXPECT_THROW_WITH_MESSAGE(
      computeTripleDelta(blankFile, newFile, index, adder),
      HasSubstr("contains a blank node"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      computeTripleDelta(oldFile, blankFile, index, adder),
      HasSubstr("contains a blank node"));

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
