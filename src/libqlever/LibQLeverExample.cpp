//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <absl/strings/str_replace.h>

#include <iostream>

#include "libqlever/Qlever.h"
#include "util/Timer.h"

static const std::string warmup1 = "SELECT * { ?s ?p ?o}";
static const std::string warmup2 = "SELECT * { ?s ?p ?o} ";

static const std::string queryTemplate = R"(
SELECT *  {
  #INPUTS#
  SERVICE ql:named-cached-query-warmup1 {}
  SERVICE ql:named-cached-query-warmup2 {}
}
)";

std::vector<std::string> inputs{""};

int main(int argc, char** argv) {
  qlever::QleverConfig config;
  config.baseName = "exampleIndex";
  AD_CONTRACT_CHECK(argc >= 2);
  config.inputFiles.emplace_back(argv[1], qlever::Filetype::Turtle);
  config.vocabularyType_ =
      ad_utility::VocabularyType{ad_utility::VocabularyType::Enum::InMemory};
  qlever::Qlever::buildIndex(config);
  qlever::Qlever qlever{config};
  qlever.pinNamed(warmup1, "warmup1");
  qlever.pinNamed(warmup2, "warmup2");

  for (std::string_view input : inputs) {
    auto query = absl::StrReplaceAll(queryTemplate,
                                     {{std::string_view{"#INPUTS#"}, input}});
    ad_utility::Timer t{ad_utility::Timer::Started};
    auto result = qlever.query(std::move(query));
    std::cout << "retrieved a query result of size " << result.size() << " in "
              << t.msecs().count() << "ms\n";
  }
}
