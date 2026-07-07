// Copyright 2024 - 2026 The QLever Authors, in particular:
//
// 2024 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <boost/asio/thread_pool.hpp>
#include <filesystem>
#include <fstream>

#include "index/InputFileSpecification.h"
#include "parser/AsyncBlockSource.h"
#include "util/MemorySize/MemorySize.h"

using namespace qlever;
using namespace testing;
using namespace ad_utility::memory_literals;

namespace {
// Minimal concrete `AsyncBlockSource` for use in factory-based tests.
struct DummyAsyncBlockSource : qlever::parser::AsyncBlockSource {
  explicit DummyAsyncBlockSource(boost::asio::any_io_executor exec,
                                 ad_utility::MemorySize blocksize)
      : AsyncBlockSource{exec, blocksize} {}

 protected:
  std::optional<qlever::parser::ByteBlock> getNextBlockImpl() override {
    return std::nullopt;
  }
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
  auto factory = [](boost::asio::any_io_executor exec,
                    ad_utility::MemorySize blocksize, std::string_view)
      -> std::unique_ptr<qlever::parser::AsyncBlockSource> {
    return std::make_unique<DummyAsyncBlockSource>(exec, blocksize);
  };
  InputFileSpecification spec{
      InputFileSpecification::BufferFactoryAndDescription{factory, "my-stream"},
      Filetype::NQuad, std::string{"myGraph"}};
  EXPECT_EQ(spec.filename(), "my-stream");
  EXPECT_EQ(spec.filetype_, Filetype::NQuad);
  EXPECT_EQ(spec.defaultGraph_, "myGraph");
}

// _____________________________________________________________________________
TEST(InputFileSpecification, FiletypeFromMediaType) {
  EXPECT_EQ(filetypeFromMediaType(ad_utility::MediaType::turtle),
            Filetype::Turtle);
  EXPECT_EQ(filetypeFromMediaType(ad_utility::MediaType::ntriples),
            Filetype::Turtle);
  EXPECT_EQ(filetypeFromMediaType(ad_utility::MediaType::nquads),
            Filetype::NQuad);
  EXPECT_EQ(filetypeFromMediaType(ad_utility::MediaType::json), std::nullopt);
}

// _____________________________________________________________________________
TEST(InputFileSpecification, MakeAsyncBlockSourceFileBased) {
  std::filesystem::path tmpFile =
      std::filesystem::temp_directory_path() / "qlever_ifs_test.ttl";
  std::ofstream{tmpFile} << "<s> <p> <o> .\n";

  boost::asio::thread_pool pool{1};
  InputFileSpecification spec{tmpFile.string(), Filetype::Turtle, std::nullopt};
  auto src = spec.makeAsyncBlockSource(pool.get_executor(), 1024_B);
  EXPECT_NE(src, nullptr);
  EXPECT_NE(dynamic_cast<qlever::parser::AsyncFileBlockSource*>(src.get()),
            nullptr);

  std::filesystem::remove(tmpFile);
}

// _____________________________________________________________________________
TEST(InputFileSpecification, MakeAsyncBlockSourceFactoryBased) {
  bool called = false;
  ad_utility::MemorySize receivedBlocksize{};
  std::string receivedDescription;

  auto factory = [&](boost::asio::any_io_executor exec,
                     ad_utility::MemorySize blocksize, std::string_view desc)
      -> std::unique_ptr<qlever::parser::AsyncBlockSource> {
    called = true;
    receivedBlocksize = blocksize;
    receivedDescription = desc;
    return std::make_unique<DummyAsyncBlockSource>(exec, blocksize);
  };
  InputFileSpecification spec{
      InputFileSpecification::BufferFactoryAndDescription{factory, "my-desc"},
      Filetype::Turtle, std::nullopt};

  boost::asio::thread_pool pool{1};
  auto src = spec.makeAsyncBlockSource(pool.get_executor(), 4096_B);
  EXPECT_TRUE(called);
  EXPECT_EQ(receivedBlocksize, 4096_B);
  EXPECT_EQ(receivedDescription, "my-desc");
  EXPECT_NE(src, nullptr);
}
