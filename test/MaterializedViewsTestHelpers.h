// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_TEST_MATERIALIZEDVIEWSTESTHELPERS_H_
#define QLEVER_TEST_MATERIALIZEDVIEWSTESTHELPERS_H_

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include <fstream>

#include "./util/GTestHelpers.h"
#include "libqlever/Qlever.h"
#include "util/Exception.h"

namespace materializedViewsTestHelpers {

static constexpr std::string_view dummyTurtle = R"(
  <s1> <p1> "abc" .
  <s1> <p2> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .
  <s2> <p1> "xyz" .
  <s2> <p3> <http://example.com/> .
)";

// _____________________________________________________________________________
inline void makeTestIndex(const std::string& basename, const std::string& kg) {
  // Write dummy turtle file
  auto ttlFilename = absl::StrCat(basename, ".ttl");
  {
    std::ofstream ttl{ttlFilename};
    ttl << kg;
  }

  // Build index on dummy turtle file
  qlever::IndexBuilderConfig config;
  config.inputFiles_.emplace_back(ttlFilename, qlever::Filetype::Turtle);
  config.baseName_ = basename;
  qlever::Qlever::buildIndex(config);
}

// _____________________________________________________________________________
inline void removeTestIndex(const std::string& basename) {
  std::regex pattern(absl::StrCat(basename, "\\..*"));
  std::cout << "Removing test files " << basename << ".*" << std::endl;
  for (const auto& entry :
       std::filesystem::directory_iterator(std::filesystem::current_path())) {
    if (entry.is_regular_file() &&
        std::regex_match(entry.path().filename().string(), pattern)) {
      std::filesystem::remove(entry.path());
    }
  }
}

// _____________________________________________________________________________
class MaterializedViewsTest : public ::testing::Test {
 private:
  std::shared_ptr<qlever::Qlever> qlv_;

 protected:
  const std::string testIndexBase_ = "_materializedViewsTestIndex";
  const std::string simpleWriteQuery_ = "SELECT * { ?s ?p ?o . BIND(1 AS ?g) }";
  std::stringstream log_;

  virtual std::string getDummyTurtle() const {
    return std::string{dummyTurtle};
  }

  void SetUp() override {
    ad_utility::setGlobalLoggingStream(&log_);
    makeTestIndex(testIndexBase_, getDummyTurtle());
    qlever::EngineConfig config;
    config.baseName_ = testIndexBase_;
    qlv_ = std::make_shared<qlever::Qlever>(config);
  }

  void TearDown() override {
    qlv_ = nullptr;
    removeTestIndex(testIndexBase_);
    ad_utility::setGlobalLoggingStream(&std::cout);
  }

  qlever::Qlever& qlv() {
    AD_CORRECTNESS_CHECK(qlv_ != nullptr);
    return *qlv_;
  }

  void clearLog() { log_.str(""); }
};

// _____________________________________________________________________________
class MaterializedViewsTestLarge : public MaterializedViewsTest {
 protected:
  static constexpr size_t numFakeSubjects_ = 10'000;

  std::string getDummyTurtle() const override {
    std::string dummy;
    for (size_t i = 0; i < numFakeSubjects_; ++i) {
      dummy =
          absl::StrCat(dummy, "<s", i,
                       "> <p1> \"abc\" ."
                       "<s",
                       i, "> <p2> \"", 2 * i,
                       "\"^^<http://www.w3.org/2001/XMLSchema#integer> .\n");
    }
    return dummy;
  }
};

}  // namespace materializedViewsTestHelpers

#endif  // QLEVER_TEST_MATERIALIZEDVIEWSTESTHELPERS_H_
