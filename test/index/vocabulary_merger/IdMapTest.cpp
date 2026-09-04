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

#include <sstream>
#include <string>

#include "../../util/GTestHelpers.h"
#include "../../util/IdTestHelpers.h"
#include "backports/filesystem.h"
#include "index/vocabulary_merger/IdMap.h"
#include "util/File.h"

using namespace ad_utility::vocabulary_merger;

namespace {
auto V = ad_utility::testing::VocabId;
}  // namespace

// _____________________________________________________________________________
// Two `IdMapEntry`s are equal if and only if both of their members are equal.
TEST(IdMapEntry, comparisonAndOutput) {
  IdMapEntry entry{3, V(4)};
  EXPECT_EQ(entry, (IdMapEntry{3, V(4)}));
  EXPECT_NE(entry, (IdMapEntry{4, V(4)}));
  EXPECT_NE(entry, (IdMapEntry{3, V(5)}));

  // The output consists of the local index and the global ID (which brings its
  // own `operator<<`), in braces.
  std::ostringstream stream;
  stream << entry;
  EXPECT_THAT(stream.str(), ::testing::StartsWith("{3, "));
  EXPECT_THAT(stream.str(), ::testing::EndsWith("}"));
}

// _____________________________________________________________________________
// Test that an `IdMapWriter` writes exactly the format that
// `getIdMapFromFile` expects, also for a number of pairs that by far exceeds
// the internal buffer of the writer.
TEST(IdMapWriter, writeAndReadBack) {
  // Far more entries than fit into the internal buffer of the writer, such
  // that the buffer has to be flushed many times.
  const size_t numPairs = 200'000;
  ASSERT_GT(numPairs * 16, 10 * IdMapWriter::bufferSize.getBytes());
  IdMap expected;
  expected.reserve(numPairs);
  for (size_t i = 0; i < numPairs; ++i) {
    expected.emplace_back(i, V(2 * i + 1));
  }

  std::string filename = gtestCurrentTestName();
  absl::Cleanup cleanup = [&filename] { ad_utility::deleteFile(filename); };
  {
    IdMapWriter writer{filename};
    for (const auto& pair : expected) {
      writer.push_back(pair);
    }
  }
  EXPECT_THAT(getIdMapFromFile(filename),
              ::testing::ElementsAreArray(expected));
  // The file consists of the number of entries (8 bytes), followed by the
  // entries (16 bytes each). This is exactly the format of a serialized
  // `IdMap`.
  EXPECT_EQ(ql::filesystem::file_size(filename), 8 + 16 * numPairs);
}

// _____________________________________________________________________________
// An `IdMapWriter` to which nothing was pushed yields an empty `IdMap`, and an
// explicit call to `finish()` makes the file readable before the writer is
// destroyed (and can be repeated without any effect).
TEST(IdMapWriter, emptyAndExplicitFinish) {
  std::string filename = gtestCurrentTestName();
  absl::Cleanup cleanup = [&filename] { ad_utility::deleteFile(filename); };
  {
    IdMapWriter writer{filename};
    writer.finish();
    EXPECT_THAT(getIdMapFromFile(filename), ::testing::IsEmpty());
    writer.finish();
  }
  EXPECT_THAT(getIdMapFromFile(filename), ::testing::IsEmpty());

  {
    IdMapWriter writer{filename};
    writer.push_back({3, V(4)});
    writer.finish();
    EXPECT_THAT(getIdMapFromFile(filename),
                ::testing::ElementsAre(IdMapEntry{3, V(4)}));
  }
  EXPECT_THAT(getIdMapFromFile(filename),
              ::testing::ElementsAre(IdMapEntry{3, V(4)}));
}
