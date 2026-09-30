// Copyright 2019 - 2026 The QLever Authors, in particular:
//
// 2019 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Only performs the "mergeVocabulary" step of the index build, on the partial
// vocabularies that an index build has left behind (see the option
// `--keep-temporary-files` of `qlever-index`), and writes the merged
// vocabulary with the real writer of a vocabulary type. This makes the merge
// phase runnable (and hence measurable and profilable) on its own.

#include <boost/program_options.hpp>
#include <iostream>
#include <string>

#include "global/Constants.h"
#include "global/FileSuffixConstants.h"
#include "index/VocabularyMerger.h"
#include "index/vocabulary/PolymorphicVocabulary.h"
#include "index/vocabulary/StringSortComparator.h"
#include "util/GlobalExecutor.h"
#include "util/MemorySize/MemorySize.h"
#include "util/ProgramOptionsHelpers.h"
#include "util/Timer.h"

namespace po = boost::program_options;

// ____________________________________________________________________________
int main(int argc, char** argv) {
  std::string basename;
  std::string outputBasename;
  size_t numPartialVocabularies = 0;
  ad_utility::VocabularyType vocabType{
      ad_utility::VocabularyType::Enum::OnDiskCompressedGeoSplit};
  ad_utility::MemorySize memory = DEFAULT_MEMORY_LIMIT_INDEX_BUILDING;
  size_t numThreads = 0;

  po::options_description options("Options for VocabularyMergerMain");
  auto add = [&options](auto&&... args) {
    options.add_options()(AD_FWD(args)...);
  };
  add("help,h", "Produce this help message.");
  add("index-basename,i", po::value(&basename)->required(),
      "The basename of the index build whose partial vocabularies are merged "
      "(the files `<basename>.partial-vocab.words.tmp.<i>`).");
  add("num-partial-vocabularies,n",
      po::value(&numPartialVocabularies)->required(),
      "The number of partial vocabularies (see the log of the index build).");
  add("output-basename,o", po::value(&outputBasename),
      "The basename of the merged vocabulary, default `<index-basename>."
      "merged`. The vocabulary is written to `<output-basename>.vocabulary*`.");
  auto vocabTypes =
      ad_utility::VocabularyType::getListOfValuesForIndexBuilding();
  add("vocabulary-type", po::value(&vocabType),
      absl::StrCat("The vocabulary type, any of ", vocabTypes,
                   ", default `on-disk-compressed-geo-split`.")
          .c_str());
  add("memory,m", po::value(&memory),
      "The memory that the merge may use, default 5 GB.");
  add("num-threads,j", po::value(&numThreads),
      "The number of threads of the global thread pool, default the number "
      "of cores.");
  po::variables_map optionsMap;
  try {
    po::store(po::parse_command_line(argc, argv, options), optionsMap);
    if (optionsMap.count("help")) {
      std::cout << options << '\n';
      return EXIT_SUCCESS;
    }
    po::notify(optionsMap);
  } catch (const std::exception& e) {
    std::cerr << "Error in command-line argument: " << e.what() << '\n';
    std::cerr << options << '\n';
    return EXIT_FAILURE;
  }
  if (outputBasename.empty()) {
    outputBasename = basename + ".merged";
  }
  if (numThreads > 0) {
    ad_utility::setGlobalExecutorNumThreads(numThreads);
  }

  // The same comparator, writer and settings as
  // `IndexImpl::passFileForVocabulary` (with the default locale).
  TripleComponentComparator comparator;
  auto sortPred = [&comparator](std::string_view a, std::string_view b) {
    return comparator(a, b, TripleComponentComparator::Level::TOTAL);
  };
  PolymorphicVocabulary vocabulary;
  vocabulary.resetToType(vocabType);
  auto writer = vocabulary.makeParallelWriterPtr(outputBasename + VOCAB_SUFFIX);
  writer->readableName() = "merged vocabulary";

  AD_LOG_INFO << "Merging " << numPartialVocabularies
              << " partial vocabularies of " << basename << " into "
              << outputBasename << VOCAB_SUFFIX << " (" << vocabType << ") ..."
              << std::endl;
  ad_utility::Timer timer{ad_utility::Timer::Started};
  auto metaData = ad_utility::vocabulary_merger::mergeVocabulary(
      basename, numPartialVocabularies, sortPred, *writer, memory);
  writer->finish();
  AD_LOG_INFO << "Merged " << metaData.numWordsTotal() << " words and "
              << metaData.getNextBlankNodeIndex() << " blank nodes in "
              << timer.msecs() << std::endl;
  return EXIT_SUCCESS;
}
