// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <gtest/gtest.h>

#include "../util/GTestHelpers.h"
#include "index/GraphNamespaceManager.h"

TEST(GraphNamespaceManager, allocateNewGraph) {
  {
    GraphNamespaceManager nsm("http://example.org/g/", 0);

    EXPECT_EQ(nsm.allocateNewGraph().toStringRepresentation(),
              "<http://example.org/g/0>");
    EXPECT_EQ(nsm.allocateNewGraph().toStringRepresentation(),
              "<http://example.org/g/1>");
    EXPECT_EQ(nsm.allocateNewGraph().toStringRepresentation(),
              "<http://example.org/g/2>");
  }
  {
    GraphNamespaceManager nsm("http://example.org/g/", 12);
    EXPECT_EQ(nsm.allocateNewGraph().toStringRepresentation(),
              "<http://example.org/g/12>");
  }
}
TEST(GraphNamespaceManager, storeAndRestoreData) {
  auto tmpFile =
      std::filesystem::temp_directory_path() / "testGraphNamespaceManager";
  // Make sure no file like this exists
  std::filesystem::remove(tmpFile);
  absl::Cleanup cleanup{[&tmpFile]() { std::filesystem::remove(tmpFile); }};

  {
    auto allocatedGraphs = 13;
    auto nsm = GraphNamespaceManager("http://example.org/g/", allocatedGraphs);
    nsm.setFilenameForPersistentUpdatesAndReadFromDisk(tmpFile.c_str());
    nsm.writeToDisk(allocatedGraphs);
  }
  {
    auto nsm = GraphNamespaceManager();
    nsm.setFilenameForPersistentUpdatesAndReadFromDisk(tmpFile.c_str());
    EXPECT_THAT(nsm.prefixWithoutBraces_,
                testing::StrEq("http://example.org/g/"));
    EXPECT_EQ(*nsm.allocatedGraphs_.rlock(), 13);
  }
}

TEST(GraphNamespaceManager, json) {
  GraphNamespaceManager original("http://example.org/ns/", 42);

  nlohmann::json j;
  to_json(j, original);

  GraphNamespaceManager restored;
  from_json(j, restored);

  auto iri = restored.allocateNewGraph();
  EXPECT_EQ(iri.toStringRepresentation(), "<http://example.org/ns/42>");
}
