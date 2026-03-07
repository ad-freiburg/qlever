// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <gtest/gtest.h>

#include "../MaterializedViewsTestHelpers.h"
#include "../util/GTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "index/GraphManager.h"
#include "util/HashSet.h"

// ============================================================================
// GraphNamespaceManager tests
// ============================================================================

TEST(GraphNamespaceManager, allocateNewGraph) {
  {
    GraphNamespaceManager nsm("<http://example.org/g/", 0);

    EXPECT_EQ(nsm.allocateNewGraph().toStringRepresentation(),
              "<http://example.org/g/0>");
    EXPECT_EQ(nsm.allocateNewGraph().toStringRepresentation(),
              "<http://example.org/g/1>");
    EXPECT_EQ(nsm.allocateNewGraph().toStringRepresentation(),
              "<http://example.org/g/2>");
  }
  {
    GraphNamespaceManager nsm("<http://example.org/g/", 12);
    EXPECT_EQ(nsm.allocateNewGraph().toStringRepresentation(),
              "<http://example.org/g/12>");
  }
}
TEST(GraphNamespaceManager, storeAndRestoreData) {
  auto tmpFile = std::filesystem::temp_directory_path() / "testDeltaTriples";
  // Make sure no file like this exists
  std::filesystem::remove(tmpFile);
  absl::Cleanup cleanup{[&tmpFile]() { std::filesystem::remove(tmpFile); }};

  {
    auto nsm = GraphNamespaceManager("<http://example.org/g/", 13);
    nsm.setFilenameForPersistentUpdatesAndReadFromDisk(tmpFile.c_str());
    nsm.writeToDisk();
  }
  {
    auto nsm = GraphNamespaceManager();
    nsm.setFilenameForPersistentUpdatesAndReadFromDisk(tmpFile.c_str());
    EXPECT_THAT(nsm.prefix_, testing::StrEq("<http://example.org/g/"));
    EXPECT_EQ(nsm.allocatedGraphs_.load(), 13);
  }
}

TEST(GraphNamespaceManager, json) {
  GraphNamespaceManager original("<http://example.org/ns/", 42);

  nlohmann::json j;
  to_json(j, original);

  GraphNamespaceManager restored;
  from_json(j, restored);

  auto iri = restored.allocateNewGraph();
  EXPECT_EQ(iri.toStringRepresentation(), "<http://example.org/ns/42>");
}
