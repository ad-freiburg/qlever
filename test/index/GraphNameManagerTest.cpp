// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <gtest/gtest.h>

#include "../util/AllocatorTestHelpers.h"
#include "../util/FileTestHelpers.h"
#include "../util/GTestHelpers.h"
#include "index/GraphNameManager.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/Serializer.h"

// _____________________________________________________________________________
TEST(GraphNameManager, allocateNewGraph) {
  {
    GraphNameManager nsm("http://example.org/g/", 0);

    EXPECT_EQ(nsm.allocateNewGraph().toStringRepresentation(),
              "<http://example.org/g/0>");
    EXPECT_EQ(nsm.allocateNewGraph().toStringRepresentation(),
              "<http://example.org/g/1>");
    EXPECT_EQ(nsm.allocateNewGraph().toStringRepresentation(),
              "<http://example.org/g/2>");
  }
  {
    GraphNameManager nsm("http://example.org/g/", 12);
    EXPECT_EQ(nsm.allocateNewGraph().toStringRepresentation(),
              "<http://example.org/g/12>");
  }
}

// _____________________________________________________________________________
TEST(GraphNameManager, storeAndRestoreData) {
  auto [tmpFile, cleanup] = ad_utility::testing::filenameForTesting();

  {
    auto allocatedGraphs = 13;
    auto nsm = GraphNameManager("http://example.org/g/", allocatedGraphs);
    ad_utility::serialization::FileWriteSerializer serializer{tmpFile.c_str()};
    serializer << nsm;
  }
  {
    auto nsm = GraphNameManager();
    ad_utility::serialization::FileReadSerializer serializer{tmpFile.c_str()};
    serializer >> nsm;
    EXPECT_THAT(nsm.prefixWithoutBraces_,
                testing::StrEq("http://example.org/g/"));
    EXPECT_EQ(nsm.nextUnallocatedGraph_.load(), 13);
  }
}

// _____________________________________________________________________________
TEST(GraphNameManager, json) {
  GraphNameManager original("http://example.org/ns/", 42);

  nlohmann::json j;
  to_json(j, original);

  GraphNameManager restored;
  from_json(j, restored);

  auto iri = restored.allocateNewGraph();
  EXPECT_EQ(iri.toStringRepresentation(), "<http://example.org/ns/42>");
}

// _____________________________________________________________________________
TEST(GraphNameManager, toString) {
  EXPECT_THAT(GraphNameManager("http://example.org/ns/", 42),
              InsertIntoStream(
                  testing::StrEq("GraphNameManager(prefix=\"http://"
                                 "example.org/ns/\", allocatedGraphs=42)")));
}
