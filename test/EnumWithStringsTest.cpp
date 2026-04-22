// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Must be included before `EnumWithStrings.h` (via `VocabularyType.h`) so
// that `boost::any` and `boost::program_options::validate` are visible when
// the `EnumWithStrings::validate` template is instantiated.
#include <gmock/gmock.h>

#include <boost/program_options.hpp>

#include "./util/GTestHelpers.h"
#include "index/vocabulary/VocabularyType.h"
#include "util/HashMap.h"

// _____________________________________________________________________________
TEST(EnumWithStrings, VocabularyTypeEnum) {
  using V = ad_utility::VocabularyType;
  EXPECT_EQ(V::InMemoryUncompressed, V::fromString("in-memory-uncompressed"));
  EXPECT_EQ(V::OnDiskCompressed, V::fromString("on-disk-compressed"));
  EXPECT_EQ("in-memory-uncompressed", V::InMemoryUncompressed.toString());
  EXPECT_EQ("on-disk-compressed", V::OnDiskCompressed.toString());

  std::stringstream stream;
  stream << V::InMemoryUncompressed;
  EXPECT_EQ(stream.str(), "in-memory-uncompressed");

  nlohmann::json j;
  j = V::InMemoryUncompressed;
  EXPECT_EQ(V::InMemoryUncompressed, j.get<V>());
  j = V::OnDiskCompressed;
  EXPECT_EQ(V::OnDiskCompressed, j.get<V>());

  std::vector<V> all(V::all().begin(), V::all().end());
  EXPECT_THAT(all, ::testing::ElementsAre(
                       V::InMemoryUncompressed, V::OnDiskUncompressed,
                       V::InMemoryCompressed, V::OnDiskCompressed,
                       V::OnDiskCompressedGeoSplit));

  ad_utility::HashMap<V, size_t> h;
  for (size_t i = 0; i < 50000; ++i) {
    h[V::random()]++;
  }
  for (auto v : all) {
    EXPECT_GT(h[v], 7000);
    EXPECT_LT(h[v], 13000);
  }

  EXPECT_THROW(V::fromString("not-a-valid-type"), std::runtime_error);
}

// _____________________________________________________________________________
TEST(EnumWithStrings, BoostProgramOptions) {
  namespace po = boost::program_options;
  using V = ad_utility::VocabularyType;

  V vocabType;
  po::options_description desc;
  desc.add_options()("vocab-type", po::value<V>(&vocabType));

  // Valid parse: "on-disk-compressed" should parse to V::OnDiskCompressed.
  {
    po::variables_map vm;
    po::store(po::command_line_parser(std::vector<std::string>{
                                          "--vocab-type", "on-disk-compressed"})
                  .options(desc)
                  .run(),
              vm);
    po::notify(vm);
    EXPECT_EQ(vocabType, V::OnDiskCompressed);
  }

  // Invalid parse: an unrecognised string should throw.
  {
    po::variables_map vm;
    AD_EXPECT_THROW_WITH_MESSAGE(
        po::store(
            po::command_line_parser(
                std::vector<std::string>{"--vocab-type", "not-a-valid-type"})
                .options(desc)
                .run(),
            vm),
        ::testing::HasSubstr("is not a valid vocabulary type"));
  }
}
