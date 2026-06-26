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

using namespace qlever;
using namespace testing;

namespace {
// Minimal concrete `AsyncBlockSource` for use in factory-based tests.
struct DummyBlockSource : qlever::parser::AsyncBlockSource {
  explicit DummyBlockSource(boost::asio::any_io_executor exec, size_t blocksize)
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
  auto factory = [](boost::asio::any_io_executor exec, size_t blocksize,
                    std::string_view)
      -> std::unique_ptr<qlever::parser::AsyncBlockSource> {
    return std::make_unique<DummyBlockSource>(exec, blocksize);
  };
  InputFileSpecification spec{
      InputFileSpecification::BufferFactoryAndDescription{factory, "my-stream"},
      Filetype::NQuad, std::string{"myGraph"}};
  EXPECT_EQ(spec.filename(), "my-stream");
  EXPECT_EQ(spec.filetype_, Filetype::NQuad);
  EXPECT_EQ(spec.defaultGraph_, "myGraph");
}

// _____________________________________________________________________________
TEST(InputFileSpecification, GetAsyncBlockSourceFileBased) {
  std::filesystem::path tmpFile =
      std::filesystem::temp_directory_path() / "qlever_ifs_test.ttl";
  std::ofstream{tmpFile} << "<s> <p> <o> .\n";

  InputFileSpecification spec{tmpFile.string(), Filetype::Turtle, std::nullopt};
  boost::asio::thread_pool pool{1};
  auto buf = spec.getAsyncBlockSource(pool.get_executor(), 1024);
  EXPECT_NE(buf, nullptr);
  EXPECT_NE(dynamic_cast<qlever::parser::AsyncFileBlockSource*>(buf.get()),
            nullptr);

  std::filesystem::remove(tmpFile);
}

// _____________________________________________________________________________
TEST(InputFileSpecification, GetAsyncBlockSourceFactoryBased) {
  bool called = false;
  size_t receivedBlocksize = 0;
  std::string receivedDescription;

  auto factory = [&](boost::asio::any_io_executor exec, size_t blocksize,
                     std::string_view desc)
      -> std::unique_ptr<qlever::parser::AsyncBlockSource> {
    called = true;
    receivedBlocksize = blocksize;
    receivedDescription = desc;
    return std::make_unique<DummyBlockSource>(exec, blocksize);
  };
  InputFileSpecification spec{
      InputFileSpecification::BufferFactoryAndDescription{factory, "my-desc"},
      Filetype::Turtle, std::nullopt};

  boost::asio::thread_pool pool{1};
  auto buf = spec.getAsyncBlockSource(pool.get_executor(), 4096);
  EXPECT_TRUE(called);
  EXPECT_EQ(receivedBlocksize, 4096u);
  EXPECT_EQ(receivedDescription, "my-desc");
  EXPECT_NE(buf, nullptr);
}
