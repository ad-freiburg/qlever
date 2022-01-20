//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "./global/Pattern.h"
#include "./index/Vocabulary.h"
#include "./parser/RdfEscaping.h"
#include "./util/BatchedPipeline.h"
#include "./util/Log.h"
#include "./util/ReadableNumberFact.h"
#include "./util/json.h"

int main(int argc, char** argv) {
  // Make sure that large integers are properly formatted
  std::locale locale;
  ad_utility::ReadableNumberFacet facet(1);
  std::locale locWithNumberGrouping(locale, &facet);
  ad_utility::Log::imbue(locWithNumberGrouping);

  if (argc != 2) {
    LOG(ERROR) << "Usage: ./VocabularyConverterMain <indexBasename>"
               << std::endl;
    return EXIT_FAILURE;
  }

  LOG(INFO) << "Converting the line-based vocabulary from previous versions of "
               "QLever (<basename>.vocabulary and possibly "
               "<basename>.text.vocabulary) to the new binary format";
  LOG(INFO) << "The new "
               "files are named <basename>.vocabulary.binary and "
               "<basename>.text.binary-vocabulary and have to be manually "
               "renamed to replace the original files"
            << std::endl;

  std::string basename{argv[1]};
  {
    std::string inFileName = basename + ".vocabulary";
    std::string outFileName = basename + ".vocabulary.binary";

    RdfsVocabulary vocab;
    std::ifstream f(basename + CONFIGURATION_FILE);
    AD_CHECK(f.is_open());
    nlohmann::json j;
    f >> j;

    if (j.find("prefixes") != j.end()) {
      if (j["prefixes"]) {
        vector<string> prefixes;
        std::ifstream prefixFile(basename + PREFIX_FILE);
        AD_CHECK(prefixFile.is_open());
        for (string prefix; std::getline(prefixFile, prefix);) {
          prefixes.emplace_back(std::move(prefix));
        }
        vocab.initializePrefixes(prefixes);
      } else {
        vocab.initializePrefixes(std::vector<std::string>());
      }
    }

    CompactVectorOfStrings<char>::Writer writer{outFileName};
    LOG(INFO) << "Reading vocabulary from file " << inFileName << "\n";
    std::fstream in(inFileName.c_str(), std::ios_base::in);
    string line;
    [[maybe_unused]] bool first = true;
    // return a std::optional<string> with the next word and nullopt in case of
    // exhaustion.
    auto creator = [&]() -> std::optional<std::string> {
      if (std::getline(in, line)) {
        return std::optional(std::move(line));
      } else {
        return std::nullopt;
      }
    };

    auto expand = [&vocab](std::string&& s) { return vocab.expandPrefix(s); };

    auto normalize = [](std::string&& s) {
      return RdfEscaping::unescapeNewlinesAndBackslashes(s);
    };

    auto compress = [&vocab](std::string&& s) {
      return vocab.compressPrefix(s);
    };

    auto push = [&writer, i = 0ul](CompressedString&& s) mutable {
      auto sv = s.toStringView();
      writer.push(sv.data(), sv.size());
      i++;
      if (i % 50'000'000 == 0) {
        LOG(INFO) << "Read " << i << " words." << std::endl;
      }
      return std::move(s);
    };

    auto pipeline = ad_pipeline::setupParallelPipeline<2, 3, 2, 1>(
        100000, creator, expand, normalize, compress, push);
    while ([[maybe_unused]] auto opt = pipeline.getNextValue()) {
      // run to exhaustion
    }
    in.close();
    writer.finish();
    LOG(INFO) << "Done converting vocabulary." << std::endl;
  }
  {
    std::string inFileName = basename + ".text.vocabulary";
    std::string outFileName = basename + ".text.vocabulary.binary";

    std::ifstream in{inFileName};
    if (in.is_open()) {
      LOG(INFO) << "Also converting the text vocabulary" << std::endl;
    } else {
      LOG(INFO) << "No text vocabulary was found, exiting" << std::endl;
      return EXIT_SUCCESS;
    }
    CompactVectorOfStrings<char>::Writer writer{outFileName};
    std::string line;
    while (std::getline(in, line)) {
      writer.push(line.data(), line.size());
    }
    writer.finish();
    LOG(INFO) << "Finished converting the external vocabulary" << std::endl;
  }
}
