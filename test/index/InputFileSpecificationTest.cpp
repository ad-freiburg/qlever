// Copyright 2024 - 2026 The QLever Authors, in particular:
//
// 2024 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <filesystem>
#include <fstream>

#include "index/InputFileSpecification.h"
#include "parser/ParallelBuffer.h"

using namespace qlever;
using namespace testing;

namespace {
// Minimal concrete ParallelBuffer for use in factory-based tests.
struct DummyBuffer : ParallelBuffer {
  explicit DummyBuffer(size_t blocksize) : ParallelBuffer(blocksize) {}
  std::optional<BufferType> getNextBlock() override { return std::nullopt; }
};
}  // namespace

// _____________________________________________________________________________
TEST(InputFileSpecification, FilenameSource) {
  InputFileSpecification spec{"file.ttl", Filetype::Turtle, std::nullopt};
  EXPECT_EQ(spec.filename(), "file.ttl");
  EXPECT_EQ(spec.filetype_, Filetype::Turtle);
  EXPECT_EQ(spec.defaultGraph_, std::nullopt);
  EXPECT_FALSE(spec.parseInParallel_);
  EXPECT_FALSE(spec.parseInParallelSetExplicitly_);
}

// _____________________________________________________________________________
TEST(InputFileSpecification, FactorySource) {
  auto factory = [](size_t blocksize,
                    std::string_view) -> std::unique_ptr<ParallelBuffer> {
    return std::make_unique<DummyBuffer>(blocksize);
  };
  InputFileSpecification spec{
      InputFileSpecification::BufferFactoryAndDescription{factory, "my-stream"},
      Filetype::NQuad, std::string{"myGraph"}};
  EXPECT_EQ(spec.filename(), "my-stream");
  EXPECT_EQ(spec.filetype_, Filetype::NQuad);
  EXPECT_EQ(spec.defaultGraph_, "myGraph");
}

// _____________________________________________________________________________
TEST(InputFileSpecification, GetParallelBufferFileBased) {
  std::filesystem::path tmpFile =
      std::filesystem::temp_directory_path() / "qlever_ifs_test.ttl";
  std::ofstream{tmpFile} << "<s> <p> <o> .\n";

  InputFileSpecification spec{tmpFile.string(), Filetype::Turtle, std::nullopt};
  auto buf = spec.getParallelBuffer(1024);
  EXPECT_NE(buf, nullptr);
  EXPECT_NE(dynamic_cast<ParallelFileBuffer*>(buf.get()), nullptr);

  std::filesystem::remove(tmpFile);
}

// _____________________________________________________________________________
TEST(InputFileSpecification, GetParallelBufferFactoryBased) {
  bool called = false;
  size_t receivedBlocksize = 0;
  std::string receivedDescription;

  auto factory = [&](size_t blocksize,
                     std::string_view desc) -> std::unique_ptr<ParallelBuffer> {
    called = true;
    receivedBlocksize = blocksize;
    receivedDescription = desc;
    return std::make_unique<DummyBuffer>(blocksize);
  };
  InputFileSpecification spec{
      InputFileSpecification::BufferFactoryAndDescription{factory, "my-desc"},
      Filetype::Turtle, std::nullopt};

  auto buf = spec.getParallelBuffer(4096);
  EXPECT_TRUE(called);
  EXPECT_EQ(receivedBlocksize, 4096u);
  EXPECT_EQ(receivedDescription, "my-desc");
  EXPECT_NE(buf, nullptr);
}
