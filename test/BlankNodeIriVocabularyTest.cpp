// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "backports/filesystem.h"
#include "index/BlankNodeIriVocabulary.h"
#include "index/Vocabulary.h"
#include "index/vocabulary/VocabularyType.h"
#include "util/FilesystemHelpers.h"
#include "util/GTestHelpers.h"

namespace {
using ad_utility::VocabularyType;

// Delete all files in the current directory whose name starts with `prefix`.
void deleteFilesWithPrefix(const std::string& prefix) {
  namespace fs = ql::filesystem;
  qlever::util::deleteFilesInDirectory(
      fs::current_path(), [&prefix](const auto& path) {
        return ql::starts_with(path.filename().string(), prefix);
      });
}

// Write the given `(iri, blankNodeIndex)` pairs (in the given order) to a fresh
// `BlankNodeIriVocabulary`, then load it again and return it.
BlankNodeIriVocabulary writeAndRead(
    const std::string& base, VocabularyType type,
    const std::vector<std::pair<std::string, uint64_t>>& entries) {
  {
    RdfsVocabulary vocab;
    vocab.resetToType(type);
    vocab.setLocale("en", "US", false);
    BlankNodeIriVocabulary::Writer writer(
        vocab.makeWordWriterPtr(base),
        BlankNodeIriVocabulary::chunksFilename(base));
    for (const auto& [iri, blankNodeIndex] : entries) {
      writer(iri, blankNodeIndex);
    }
    writer.finish();
  }
  BlankNodeIriVocabulary result;
  result.readFromFile(base, type, "en", "US", false);
  return result;
}
}  // namespace

// _____________________________________________________________________________
TEST(BlankNodeIriVocabulary, singleContiguousChunk) {
  for (auto type : {VocabularyType{VocabularyType::Enum::InMemoryUncompressed},
                    VocabularyType{VocabularyType::Enum::OnDiskCompressed}}) {
    std::string base = absl::StrCat(gtestCurrentTestName(), type.toString());
    absl::Cleanup cleanup = [&base] { deleteFilesWithPrefix(base); };

    // The IRIs must be pushed in ascending (sorted) order; the blank node
    // indices are contiguous, so a single chunk describes the whole mapping.
    auto vocab = writeAndRead(
        base, type,
        {{"<http://ex/a>", 3}, {"<http://ex/b>", 4}, {"<http://ex/c>", 5}});
    EXPECT_FALSE(vocab.empty());
    EXPECT_THAT(vocab.getBlankNodeIndex("<http://ex/a>"),
                ::testing::Optional(3u));
    EXPECT_THAT(vocab.getBlankNodeIndex("<http://ex/b>"),
                ::testing::Optional(4u));
    EXPECT_THAT(vocab.getBlankNodeIndex("<http://ex/c>"),
                ::testing::Optional(5u));
    // An IRI that was not stored is not found.
    EXPECT_EQ(vocab.getBlankNodeIndex("<http://ex/d>"), std::nullopt);
  }
}

// _____________________________________________________________________________
TEST(BlankNodeIriVocabulary, multipleChunks) {
  for (auto type : {VocabularyType{VocabularyType::Enum::InMemoryUncompressed},
                    VocabularyType{VocabularyType::Enum::OnDiskCompressed}}) {
    std::string base = absl::StrCat(gtestCurrentTestName(), type.toString());
    absl::Cleanup cleanup = [&base] { deleteFilesWithPrefix(base); };

    // The blank node indices are not contiguous (there are gaps, e.g. because
    // `_:` blank nodes were interleaved at those positions), so the mapping is
    // described by several chunks. The `IRI` order is still ascending.
    auto vocab = writeAndRead(base, type,
                              {{"<http://ex/a>", 0},
                               {"<http://ex/b>", 1},
                               {"<http://ex/m>", 10},
                               {"<http://ex/n>", 11},
                               {"<http://ex/z>", 100}});
    EXPECT_THAT(vocab.getBlankNodeIndex("<http://ex/a>"),
                ::testing::Optional(0u));
    EXPECT_THAT(vocab.getBlankNodeIndex("<http://ex/b>"),
                ::testing::Optional(1u));
    EXPECT_THAT(vocab.getBlankNodeIndex("<http://ex/m>"),
                ::testing::Optional(10u));
    EXPECT_THAT(vocab.getBlankNodeIndex("<http://ex/n>"),
                ::testing::Optional(11u));
    EXPECT_THAT(vocab.getBlankNodeIndex("<http://ex/z>"),
                ::testing::Optional(100u));
    EXPECT_EQ(vocab.getBlankNodeIndex("<http://ex/c>"), std::nullopt);
  }
}

// _____________________________________________________________________________
TEST(BlankNodeIriVocabulary, empty) {
  // A default-constructed vocabulary is empty and never returns a blank node.
  BlankNodeIriVocabulary vocab;
  EXPECT_TRUE(vocab.empty());
  EXPECT_EQ(vocab.getBlankNodeIndex("<http://ex/a>"), std::nullopt);
}
