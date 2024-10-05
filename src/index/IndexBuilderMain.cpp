// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2014-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2018-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#include <boost/program_options.hpp>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <string>

#include "CompilationInfo.h"
#include "global/Constants.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/Index.h"
#include "parser/RdfParser.h"
#include "parser/Tokenizer.h"
#include "util/File.h"
#include "util/MemorySize/MemorySize.h"
#include "util/ProgramOptionsHelpers.h"
#include "util/ReadableNumberFact.h"

using std::string;

namespace po = boost::program_options;

string getStxxlConfigFileName(const string& location) {
  return absl::StrCat(location, ".stxxl");
}

string getStxxlDiskFileName(const string& location, const string& tail) {
  return absl::StrCat(location, tail, ".stxxl-disk");
}

// Write a .stxxl config-file.
// All we want is sufficient space somewhere with enough space.
// We can use the location of input files and use a constant size for now.
// The required size can only be estimated anyway, since index size
// depends on the structure of words files rather than their size only,
// because of the "multiplications" performed.
void writeStxxlConfigFile(const string& location, const string& tail) {
  string stxxlConfigFileName = getStxxlConfigFileName(location);
  ad_utility::File stxxlConfig(stxxlConfigFileName, "w");
  auto configFile = ad_utility::makeOfstream(stxxlConfigFileName);
  // Inform stxxl about .stxxl location
  setenv("STXXLCFG", stxxlConfigFileName.c_str(), true);
  configFile << "disk=" << getStxxlDiskFileName(location, tail) << ","
             << STXXL_DISK_SIZE_INDEX_BUILDER << ",syscall\n";
}

// Check, that the `vec` has exactly 1 or exactly `numFiles` many entries. If
// `allowZero` is true, then an empty vector will also be accepted. If this
// condition is violated, throw an exception. This is used to validate the
// parameters for file types and default graphs.
static constexpr auto checkNumParams = [](const auto& vec, size_t numFiles,
                                          bool allowZero,
                                          std::string_view parameterName) {
  if (allowZero && vec.empty()) {
    return;
  }
  if (vec.size() == 1 || vec.size() == numFiles) {
    return;
  }
  auto error = absl::StrCat(
      "The parameter \"", parameterName,
      "\" must be specified either exactly once (in which case it is "
      "used for all input files) or exactly `num-inpu-files` times, in "
      "which case each input file has its own value.");
  if (allowZero) {
    absl::StrAppend(&error,
                    " It can additionally be completely omitted, in which "
                    "case a default value is used for all inputs");
  }
  throw std::runtime_error{std::move(error)};
};

// Convert the `filetype` string, which must be "ttl", "nt", or "nq" to the
// corresponding `qlever::Filetype` value.
auto getFiletype = [](std::string_view filetype) {
  if (filetype == "ttl" || filetype == "nt") {
    return qlever::Filetype::Turtle;
  } else if (filetype == "nq") {
    return qlever::Filetype::NQuad;
  } else {
    throw std::runtime_error{
        absl::StrCat("The parameter --file-format, -F must be either one of "
                     "`ttl`, `nt`, or `nq`, but is `",
                     filetype, "`")};
  }
};

// Main function.
int main(int argc, char** argv) {
  // Copy the git hash and datetime of compilation (which require relinking)
  // to make them accessible to other parts of the code
  qlever::version::copyVersionInfo();
  setlocale(LC_CTYPE, "");

  std::locale loc;
  ad_utility::ReadableNumberFacet facet(1);
  std::locale locWithNumberGrouping(loc, &facet);
  ad_utility::Log::imbue(locWithNumberGrouping);

  string baseName;
  string wordsfile;
  string docsfile;
  string textIndexName;
  string kbIndexName;
  string settingsFile;
  std::vector<string> filetype;
  std::vector<string> inputFile;
  std::vector<string> defaultGraphs;
  bool noPatterns = false;
  bool onlyAddTextIndex = false;
  bool keepTemporaryFiles = false;
  bool onlyPsoAndPos = false;
  bool addWordsFromLiterals = false;
  std::optional<ad_utility::MemorySize> stxxlMemory;
  optind = 1;

  Index index{ad_utility::makeUnlimitedAllocator<Id>()};

  boost::program_options::options_description boostOptions(
      "Options for IndexBuilderMain");
  auto add = [&boostOptions]<typename... Args>(Args&&... args) {
    boostOptions.add_options()(std::forward<Args>(args)...);
  };
  add("help,h", "Produce this help message.");
  add("index-basename,i", po::value(&baseName)->required(),
      "The basename of the output files (required).");
  add("kg-input-file,f", po::value(&inputFile),
      "The file with the knowledge graph data to be parsed from. If omitted, "
      "will read from stdin.");
  add("file-format,F", po::value(&filetype),
      "The format of the input file with the knowledge graph data. Must be one "
      "of [nt|ttl|nq]. If not set, QLever will try to deduce it from the "
      "filename suffix.");
  add("default-graph,g", po::value(&defaultGraphs),
      "The default graph Iri for the k-th file");
  add("kg-index-name,K", po::value(&kbIndexName),
      "The name of the knowledge graph index (default: basename of "
      "`kg-input-file`).");

  // Options for the text index.
  add("text-docs-input-file,d", po::value(&docsfile),
      "The full text of the text records from which to build the text index.");
  add("text-words-input-file,w", po::value(&wordsfile),
      "Words of the text records from which to build the text index.");
  add("text-words-from-literals,W", po::bool_switch(&addWordsFromLiterals),
      "Consider all literals from the internal vocabulary as text records. Can "
      "be combined with `text-docs-input-file` and `text-words-input-file`");
  add("text-index-name,T", po::value(&textIndexName),
      "The name of the text index (default: basename of "
      "text-words-input-file).");
  add("add-text-index,A", po::bool_switch(&onlyAddTextIndex),
      "Only build the text index. Assumes that a knowledge graph index with "
      "the same `index-basename` already exists.");

  // Options for the knowledge graph index.
  add("settings-file,s", po::value(&settingsFile),
      "A JSON file, where various settings can be specified (see the QLever "
      "documentation).");
  add("no-patterns", po::bool_switch(&noPatterns),
      "Disable the precomputation for `ql:has-predicate`.");
  add("only-pso-and-pos-permutations,o", po::bool_switch(&onlyPsoAndPos),
      "Only build the PSO and POS permutations. This is faster, but then "
      "queries with predicate variables are not supported");

  // Options for the index building process.
  add("stxxl-memory,m", po::value(&stxxlMemory),
      "The amount of memory in to use for sorting during the index build. "
      "Decrease if the index builder runs out of memory.");
  add("keep-temporary-files,k", po::bool_switch(&keepTemporaryFiles),
      "Do not delete temporary files from index creation for debugging.");

  // Process command line arguments.
  po::variables_map optionsMap;

  try {
    po::store(po::parse_command_line(argc, argv, boostOptions), optionsMap);
    if (optionsMap.count("help")) {
      std::cout << boostOptions << '\n';
      return EXIT_SUCCESS;
    }
    po::notify(optionsMap);
  } catch (const std::exception& e) {
    std::cerr << "Error in command-line argument: " << e.what() << '\n';
    std::cerr << boostOptions << '\n';
    return EXIT_FAILURE;
  }
  if (stxxlMemory.has_value()) {
    index.memoryLimitIndexBuilding() = stxxlMemory.value();
  }

  // If no text index name was specified, take the part of the wordsfile after
  // the last slash.
  if (textIndexName.empty() && !wordsfile.empty()) {
    textIndexName = ad_utility::getLastPartOfString(wordsfile, '/');
  }

  // If no index name was specified, take the part of the input file name after
  // the last slash.
  if (kbIndexName.empty()) {
    kbIndexName = "no index name specified";
  }

  LOG(INFO) << EMPH_ON << "QLever IndexBuilder, compiled on "
            << qlever::version::DatetimeOfCompilation << " using git hash "
            << qlever::version::GitShortHash << EMPH_OFF << std::endl;

  try {
    LOG(TRACE) << "Configuring STXXL..." << std::endl;
    size_t posOfLastSlash = baseName.rfind('/');
    string location = baseName.substr(0, posOfLastSlash + 1);
    string tail = baseName.substr(posOfLastSlash + 1);
    writeStxxlConfigFile(location, tail);
    string stxxlFileName = getStxxlDiskFileName(location, tail);
    LOG(TRACE) << "done." << std::endl;

    index.setKbName(kbIndexName);
    index.setTextName(textIndexName);
    index.usePatterns() = !noPatterns;
    index.setOnDiskBase(baseName);
    index.setKeepTempFiles(keepTemporaryFiles);
    index.setSettingsFile(settingsFile);
    index.loadAllPermutations() = !onlyPsoAndPos;

    // Convert the parameters for the filenames, file types, and default graphs
    // into a `vector<InputFileSpecification>`.
    auto getFileSpecifications = [&]() {
      checkNumParams(filetype, inputFile.size(), false, "--file-format, -F");
      checkNumParams(defaultGraphs, inputFile.size(), true,
                     "--default-graph, -g");
      std::vector<qlever::InputFileSpecification> fileSpecs;
      for (size_t i = 0; i < inputFile.size(); ++i) {
        std::string_view type =
            filetype.size() == 1 ? filetype.at(0) : filetype.at(i);
        auto defaultGraph = [&]() -> std::optional<string> {
          if (defaultGraphs.empty()) {
            return std::nullopt;
          }
          return defaultGraphs.size() == 1 ? defaultGraphs.at(0)
                                           : defaultGraphs.at(i);
        }();
        auto& filename = inputFile.at(i);
        if (filename == "-") {
          filename = "/dev/stdin";
        }
        fileSpecs.emplace_back(filename, getFiletype(type),
                               std::move(defaultGraph));
      }
      return fileSpecs;
    };

    if (!onlyAddTextIndex) {
      auto fileSpecifications = getFileSpecifications();
      AD_CONTRACT_CHECK(!fileSpecifications.empty());
      index.createFromFiles(fileSpecifications);
    }

    if (!wordsfile.empty() || addWordsFromLiterals) {
      index.addTextFromContextFile(wordsfile, addWordsFromLiterals);
    }

    if (!docsfile.empty()) {
      index.buildDocsDB(docsfile);
    }
    ad_utility::deleteFile(stxxlFileName, false);
  } catch (std::exception& e) {
    LOG(ERROR) << e.what() << std::endl;
    exit(2);
  }
  return 0;
}
