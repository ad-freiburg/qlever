//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <string>

#include "index/VocabularyMergerImpl.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/SerializeString.h"

namespace {
using ad_utility::vocabulary_merger::writePartialVocabularyToFile;

// Helper to conveniently create an entry for `ItemVec`.
ItemVec::value_type makeEntry(std::string_view word, bool isExternalized,
                              uint64_t id) {
  return {word, ItemVec::value_type::second_type{id, isExternalized}};
}

// Read back a file written by `writePartialVocabularyToFile` and return its
// contents as a vector of (word, isExternalized, id) tuples.
std::vector<std::tuple<std::string, bool, uint64_t>> readBack(
    const std::string& fileName) {
  ad_utility::serialization::FileReadSerializer reader{fileName};
  std::vector<std::tuple<std::string, bool, uint64_t>> result;
  reader >> result;
  return result;
}
}  // namespace

// _____________________________________________________________________________
TEST(IndexVocabularyMergerImpl, writePartialVocabularyToFile) {
  std::string fileName =
      absl::StrCat(::testing::TempDir(), "/writePartialVocabularyToFile.tmp");
  absl::Cleanup cleanup{[&fileName]() { ad_utility::deleteFile(fileName); }};

  // A word large enough to cross the 16 MB flush threshold inside the function,
  // exercising the mid-loop flush in addition to the final flush.
  std::string bigWord(17ULL * 1024 * 1024, 'x');

  ItemVec els{
      makeEntry("\"alpha\"", false, 7),
      makeEntry("\"beta\"", true, 42),
      makeEntry("_:blank", false, 0),
      makeEntry(bigWord, true, 99),
  };

  writePartialVocabularyToFile(els, fileName);

  auto roundtrip = readBack(fileName);
  ASSERT_EQ(roundtrip.size(), els.size());
  EXPECT_EQ(roundtrip.at(0),
            std::make_tuple(std::string{"\"alpha\""}, false, uint64_t{7}));
  EXPECT_EQ(roundtrip.at(1),
            std::make_tuple(std::string{"\"beta\""}, true, uint64_t{42}));
  EXPECT_EQ(roundtrip.at(2),
            std::make_tuple(std::string{"_:blank"}, false, uint64_t{0}));
  EXPECT_EQ(roundtrip.at(3), std::make_tuple(bigWord, true, uint64_t{99}));

  // Empty input: only the size header (zero) is written.
  ItemVec empty;
  writePartialVocabularyToFile(empty, fileName);
  EXPECT_TRUE(readBack(fileName).empty());
}
