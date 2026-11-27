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
#include "index/LocalVocab.h"
#include "libqlever/Qlever.h"
#include "libqlever/blobDiff/TurtleDelta.h"
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
  auto triples = blobDiff::detail::parseTurtleFile(filename, index, {});
  ASSERT_EQ(triples.size(), 3u);
  EXPECT_EQ(triples[2].subject_, ad_utility::testing::iri("<a>"));
  EXPECT_EQ(triples[2].predicate_, ad_utility::testing::iri("<r>"));
  EXPECT_EQ(triples[2].object_, ad_utility::testing::iri("<b>"));

  ad_utility::makeOfstream(filename) << "<a> <p> .\n";
  AD_EXPECT_THROW_WITH_MESSAGE(
      blobDiff::detail::parseTurtleFile(filename, index, {}),
      HasSubstr(absl::StrCat("Parse error in \"", filename, "\"")));

  for (std::string_view withBlankNode :
       {"<a> <p> <b> .\n_:b <p> <c> .\n", "<a> <p> _:b .\n", "<a> <p> [] .\n",
        "<a> <p> [ <q> <c> ] .\n"}) {
    ad_utility::makeOfstream(filename) << withBlankNode;
    AD_EXPECT_THROW_WITH_MESSAGE(
        blobDiff::detail::parseTurtleFile(filename, index, {}),
        AllOf(HasSubstr(absl::StrCat("of \"", filename, "\"")),
              HasSubstr("contains a blank node")));
  }

  // The `settings` are passed to the parser.
  ad_utility::makeOfstream(filename) << "<a> <p> 42 .\n";
  RdfParserSettings allToDouble;
  allToDouble.integerOverflowBehavior_ =
      TurtleParserIntegerOverflowBehavior::AllToDouble;
  auto withSettings =
      blobDiff::detail::parseTurtleFile(filename, index, allToDouble);
  ASSERT_EQ(withSettings.size(), 1u);
  EXPECT_EQ(withSettings[0].object_, TripleComponent{42.0});
}

// Test `detail::toIdTriples`: the result is sorted and free of duplicates, all
// triples are in the same (default) graph, equal values with a different
// lexical form get the same `Id`, a word that is not in the vocabulary becomes
// a `LocalVocabIndex`, and blank nodes are rejected.
TEST(TurtleDelta, toIdTriples) {
  const Index& index =
      ad_utility::testing::getQec(std::string{oldData})->getIndex();
  LocalVocab localVocab{index.getImpl().allocator()};
  auto triples =
      RdfStringParser<TurtleParser<Tokenizer>>::parseTriplesFromString(
          "<a> <p> \"1\"^^<http://www.w3.org/2001/XMLSchema#int> .\n"
          "<a> <p> \"01\"^^<http://www.w3.org/2001/XMLSchema#int> .\n"
          "<a> <q> \"x\" .\n"
          "<a> <q> \"x\" .\n"
          "<c> <p> \"brand new\" .\n",
          &index.encodedIriManager());
  auto idTriples =
      blobDiff::detail::toIdTriples(std::move(triples), index, localVocab);
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
  AD_EXPECT_THROW_WITH_MESSAGE(blobDiff::detail::toIdTriples(
                                   std::move(withBlankNode), index, localVocab),
                               HasSubstr("Blank nodes are not supported"));
}

// Test `computeTripleDelta` and `Qlever::applyTripleDelta` end to end on a
// small index.
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
    // triple counts once, and a literal that is not in the vocabulary of the
    // index is a change. The decimal `1.0` is a different value than the
    // integer `1`, so both are stored for the same predicate.
    auto stream = ad_utility::makeOfstream(newFile);
    stream << "<a> <p> \"01\"^^<http://www.w3.org/2001/XMLSchema#int> .\n"
              "<a> <p> \"01\"^^<http://www.w3.org/2001/XMLSchema#int> .\n"
              "<a> <p> 1.0 .\n"
              "<a> <q> \"new word\" .\n"
              "<a> <r> <b> .\n";
  }

  std::string blankFile = absl::StrCat(gtestCurrentTestName(), ".blank.ttl");
  absl::Cleanup cleanupBlankFile = [&blankFile] {
    ad_utility::deleteFile(blankFile);
  };
  ad_utility::makeOfstream(blankFile) << "<a> <p> _:b .\n";

  Qlever qlever{EngineConfig{indexConfig}};
  auto index = qlever.indexSnapshot();

  // A blank node in either of the two files is rejected.
  AD_EXPECT_THROW_WITH_MESSAGE(
      computeTripleDelta(blankFile, newFile, index, {}),
      HasSubstr("contains a blank node"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      computeTripleDelta(oldFile, blankFile, index, {}),
      HasSubstr("contains a blank node"));

  // Without changes, the delta is empty.
  auto unchanged = computeTripleDelta(oldFile, oldFile, index, {});
  EXPECT_TRUE(unchanged.toInsert_.empty());
  EXPECT_TRUE(unchanged.toDelete_.empty());

  // A null index is rejected.
  EXPECT_ANY_THROW(computeTripleDelta(oldFile, newFile, nullptr, {}));

  auto delta = computeTripleDelta(oldFile, newFile, index, {});
  EXPECT_EQ(delta.toInsert_.size(), 2u);
  EXPECT_EQ(delta.toDelete_.size(), 1u);
  EXPECT_TRUE(ql::ranges::is_sorted(delta.toInsert_));
  EXPECT_EQ(delta.index_.lock(), index);
  // Only the word `"new word"` is not in the vocabulary of the index.
  EXPECT_EQ(delta.localVocab_.size(), 1u);

  // The reverse delta swaps the insertions and the deletions. (The `Id`s of
  // `"new word"` differ, because each delta has its own local vocabulary.)
  auto reverse = computeTripleDelta(newFile, oldFile, index, {});
  EXPECT_EQ(reverse.toInsert_.size(), delta.toDelete_.size());
  EXPECT_EQ(reverse.toDelete_.size(), delta.toInsert_.size());

  // A delta that was computed for a different index, or for an index that no
  // longer exists, is rejected.
  Qlever otherQlever{EngineConfig{indexConfig}};
  AD_EXPECT_THROW_WITH_MESSAGE(
      qlever.applyTripleDelta(computeTripleDelta(
          oldFile, newFile, otherQlever.indexSnapshot(), {})),
      HasSubstr("not computed for the current index"));
  auto expired = computeTripleDelta(oldFile, newFile, index, {});
  expired.index_.reset();
  AD_EXPECT_THROW_WITH_MESSAGE(qlever.applyTripleDelta(std::move(expired)),
                               HasSubstr("not computed for the current index"));

  // Applying the delta clears the caches and reports the counts.
  std::string query = "SELECT ?p ?o { <a> ?p ?o } ORDER BY ?p";
  EXPECT_EQ(qlever.query(query, ad_utility::MediaType::tsv),
            "?p\t?o\n<p>\t1\n<q>\t\"x\"\n<r>\t<b>\n");

  // Applying an empty delta changes nothing.
  auto emptyMetadata = qlever.applyTripleDelta(std::move(unchanged));
  EXPECT_EQ(emptyMetadata.countBefore_, (DeltaTriplesCount{0, 0}));
  EXPECT_EQ(emptyMetadata.countAfter_, (DeltaTriplesCount{0, 0}));
  EXPECT_EQ(qlever.query(query, ad_utility::MediaType::tsv),
            "?p\t?o\n<p>\t1\n<q>\t\"x\"\n<r>\t<b>\n");

  qlever.queryAndPinResultWithName("pinned", query);
  EXPECT_EQ(qlever.namedResultCache().numEntries(), 1u);
  auto metadata = qlever.applyTripleDelta(std::move(delta));
  EXPECT_EQ(metadata.countBefore_, (DeltaTriplesCount{0, 0}));
  EXPECT_EQ(metadata.countAfter_, (DeltaTriplesCount{2, 1}));
  EXPECT_EQ(qlever.namedResultCache().numEntries(), 0u);
  EXPECT_EQ(qlever.query(query, ad_utility::MediaType::tsv),
            "?p\t?o\n<p>\t1\n<p>\t1.0\n<q>\t\"new word\"\n<r>\t<b>\n");

  // Applying the reverse delta (computed after the `delta` was applied) gives
  // the original data again.
  qlever.applyTripleDelta(
      computeTripleDelta(newFile, oldFile, qlever.indexSnapshot(), {}));
  EXPECT_EQ(qlever.query(query, ad_utility::MediaType::tsv),
            "?p\t?o\n<p>\t1\n<q>\t\"x\"\n<r>\t<b>\n");
}
