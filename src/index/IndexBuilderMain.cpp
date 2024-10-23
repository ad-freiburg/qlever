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

// Check that `values` has exactly one or `numFiles` many entries. If
// `allowEmpty` is true, then an empty vector will also be accepted. If this
// condition is violated, throw an exception. This is used to validate the
// parameters for file types and default graphs.
static void checkNumParameterValues(const auto& values, size_t numFiles,
                                    bool allowEmpty,
                                    std::string_view parameterName) {
  if (allowEmpty && values.empty()) {
    return;
  }
  if (values.size() == 1 || values.size() == numFiles) {
    return;
  }
  auto error = absl::StrCat(
      "The parameter \"", parameterName,
      "\" must be specified either exactly once (in which case it is "
      "used for all input files) or exactly as many times as there are "
      "input files, in which case each input file has its own value.");
  if (allowEmpty) {
    absl::StrAppend(&error,
                    " The parameter can also be omitted entirely, in which "
                    " case a default value is used for all input files.");
  }
  throw std::runtime_error{error};
}

// Convert the `filetype` string, which must be "ttl", "nt", or "nq" to the
// corresponding `qlever::Filetype` value. If no filetyp is given, try to deduce
// the type from the filename.
qlever::Filetype getFiletype(std::optional<std::string_view> filetype,
                             std::string_view filename) {
  auto impl = [](std::string_view s) -> std::optional<qlever::Filetype> {
    if (s == "ttl" || s == "nt") {
      return qlever::Filetype::Turtle;
    } else if (s == "nq") {
      return qlever::Filetype::NQuad;
    } else {
      return std::nullopt;
    }
  };
  if (filetype.has_value()) {
    auto result = impl(filetype.value());
    if (result.has_value()) {
      return result.value();
    } else {
      throw std::runtime_error{
          absl::StrCat("The value of --file-format or -F must be one of "
                       "`ttl`, `nt`, or `nq`, but is `",
                       filetype.value(), "`")};
    }
  }

  auto posOfDot = filename.rfind('.');
  auto throwNotDeducable = [&filename]() {
    throw std::runtime_error{absl::StrCat(
        "Could not deduce the file format from the filename \"", filename,
        "\". Either use files with names that end on `.ttl`, `.nt`, or `.nq`, "
        "or explicitly set the format of the file via --file-format or -F")};
  };
  if (posOfDot == std::string::npos) {
    throwNotDeducable();
  }
  auto deducedType = impl(filename.substr(posOfDot + 1));
  if (deducedType.has_value()) {
    return deducedType.value();
  } else {
    throwNotDeducable();
  }
  // The following line is necessary because Clang and GCC currently can't
  // deduce that the above `else` case always throws and there is currently no
  // way to mark the `throwNotDeducable` lambda as `[[noreturn]]`.
  AD_FAIL();
}

// Get the parameter value at the given index. If the vector is empty, return
// the given `defaultValue`. If the vector has exactly one element, return that
// element, no matter what the index is.
template <typename T>
T getParameterValue(size_t idx, const auto& values, const T& defaultValue) {
  if (values.empty()) {
    return defaultValue;
  }
  if (values.size() == 1) {
    return values.at(0);
  }
  return values.at(idx);
}

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
  std::vector<bool> parseParallel;
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
      "of [nt|ttl|nq]. Can be specified once (then all files use that format), "
      "or once per file, or not at all (in that case, the format is deduced "
      "from the filename suffix if possible).");
  add("default-graph,g", po::value(&defaultGraphs),
      "The graph IRI without angle brackets. Write `-` for the default graph. "
      "Can be omitted (then all files use the default graph), specified once "
      "(then all files use that graph), or once per file.");
  add("parse-parallel,p", po::value(&parseParallel),
      "Enable or disable the parallel parser for all files (if specified once) "
      "or once per input file. Parallel parsing works for all input files "
      "using the N-Triples or N-Quads format, as well as for well-behaved "
      "Turtle files, where all the prefix declarations come in one block at "
      "the beginning and there are no multiline literals");
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
      checkNumParameterValues(filetype, inputFile.size(), true,
                              "--file-format, -F");
      checkNumParameterValues(defaultGraphs, inputFile.size(), true,
                              "--default-graph, -g");
      checkNumParameterValues(parseParallel, parseParallel.size(), true,
                              "--parse-parallel, p");

      std::vector<qlever::InputFileSpecification> fileSpecs;
      for (size_t i = 0; i < inputFile.size(); ++i) {
        auto type = getParameterValue<std::optional<std::string_view>>(
            i, filetype, std::nullopt);

        auto defaultGraph = getParameterValue<std::optional<string>>(
            i, defaultGraphs, std::nullopt);
        if (defaultGraph == "-") {
          defaultGraph = std::nullopt;
        }

        bool parseInParallel = getParameterValue(i, parseParallel, false);
        auto& filename = inputFile.at(i);
        if (filename == "-") {
          filename = "/dev/stdin";
        }
        fileSpecs.emplace_back(filename, getFiletype(type, filename),
                               std::move(defaultGraph), parseInParallel);
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
