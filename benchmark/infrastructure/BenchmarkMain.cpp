// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2022,
// schlegea@informatik.uni-freiburg.de)

#include <algorithm>
#include <boost/program_options.hpp>
#include <boost/program_options/value_semantic.hpp>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <ios>
#include <iostream>
#include <ostream>
#include <sstream>
#include <vector>

#include "../benchmark/infrastructure/Benchmark.h"
#include "../benchmark/infrastructure/BenchmarkToJson.h"
#include "../benchmark/infrastructure/BenchmarkToString.h"
#include "BenchmarkMetadata.h"
#include "util/Algorithm.h"
#include "util/ConfigManager/ConfigManager.h"
#include "util/Exception.h"
#include "util/File.h"
#include "util/json.h"

// We are not in a global header file, so its fine.
using namespace ad_benchmark;

/*
@brief Transform the given benchmark classes and corresponding results to
json, and write them to the specified file.

@param benchmarkClassAndResults The benchmark classes togehter with their
results of running `runAllBenchmarks`.
@param fileName The name of the file, where the json informationen
should be written in.
@param appendToJsonInFile Should the json informationen be appended to the end
of the json structure in the file, or should the previous content be
overwritten? Note: If the json structure in the file isn't an array, an error
will be thrown, except if the file is empty. In that case, `appendToFile` will
be treated as `false`.
*/
static void writeBenchmarkClassAndBenchmarkResultsToJsonFile(
    const std::vector<std::pair<const BenchmarkInterface*, BenchmarkResults>>&
        benchmarkClassAndResults,
    const std::string& fileName, bool appendToJsonInFile = false) {
  // Convert to json.
  nlohmann::json benchmarkClassAndBenchmarkResultsAsJson(
      zipBenchmarkClassAndBenchmarkResultsToJson(benchmarkClassAndResults));
  AD_CORRECTNESS_CHECK(benchmarkClassAndBenchmarkResultsAsJson.is_array());

  /*
  Add the old json arry entries to the new json array entries, if a non empty
  file exists. Otherwise, we create/fill the file.
  */
  if (appendToJsonInFile && std::filesystem::exists(fileName) &&
      !std::filesystem::is_empty(fileName)) {
    /*
    By parsing the file as json and working with `nlohmann::json`, instead of
    the json string representation, we first make sure, that the file only
    contains valid json, and secondly guarantee, that we generate a valid new
    json.
    Also not, that this is not a performance critical place, so we don't have to
    risk errors for a better performance.
    */
    const nlohmann::json fileAsJson(fileToJson(fileName));
    if (!fileAsJson.is_array()) {
      throw std::runtime_error(
          absl::StrCat("The contents of the file ", fileName,
                       " do not describe an array json value. Therefore no "
                       "values can be appended."));
    }

    // Add the old entries to the new entries.
    benchmarkClassAndBenchmarkResultsAsJson.insert(
        benchmarkClassAndBenchmarkResultsAsJson.begin(), fileAsJson.begin(),
        fileAsJson.end());
  }

  ad_utility::makeOfstream(fileName)
      << benchmarkClassAndBenchmarkResultsAsJson << std::endl;
}

/*
 * @brief Goes through all types of registered benchmarks, measures their time
 * and prints their measured time in a fitting format.
 */
int main(int argc, char** argv) {
  // The filename, should the write option be choosen.
  std::string writeFileName = "";
  // The short hand, for the benchmark configuration.
  std::string shortHandConfigurationString = "";
  // Should the user decide, to load a configuration from a json file,
  // this will hold the file name.
  std::string jsonConfigurationFileName = "";

  // For easier usage.
  namespace po = boost::program_options;

  // Declaring the supported options.
  po::options_description options("Options for the benchmark");
  options.add_options()("help,h", "Print the help message.")(
      "print,p", "Roughly prints all benchmarks.")(
      "write,w", po::value<std::string>(&writeFileName),
      "Writes the benchmarks as json to a file, overriding the previous"
      " content of the file.")(
      "append,a",
      "Causes the json option to append to the end of the json arry in the "
      "file, if there is one, instead of overriding the previous content of "
      "the file.")(
      "configuration-json,j",
      po::value<std::string>(&jsonConfigurationFileName),
      "Set the configuration of benchmarks as described in a json file.")(
      "configuration-shorthand,s",
      po::value<std::string>(&shortHandConfigurationString),
      "Allows you to add options to configuration of the benchmarks using the"
      " short hand described in `BenchmarkConfiguration.h:parseShortHand`.")(
      "configuration-options,o",
      "Prints all available benchmark configuration options.");

  // Prints how to use the file correctly and exits.
  auto printUsageAndExit = [&options]() {
    std::cerr << options << "\n";
    exit(1);
  };

  // Calling without using ANY arguments makes no sense.
  if (argc == 1) {
    std::cerr << "You have to specify at least one of the options of `--print`,"
                 "`--configuration-options` or `--write`.\n";
    printUsageAndExit();
  }

  // Parsing the given arguments.
  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, options), vm);
  po::notify(vm);

  // Did they set any option, that would require anything to actually happen?
  // If not, don't do anything. This should also happen, if they explicitly
  // wanted to see the `help` option.
  if (vm.count("help") || !(vm.count("print") || vm.count("write") ||
                            vm.count("configuration-options"))) {
    printUsageAndExit();
  }

  /*
  Set all the configuration options.
  Because the `configManager` also checks, if mandatory options were set, when
  it parses, we always have to call this. Even if it is empty.
  */
  nlohmann::json jsonConfig(nlohmann::json::value_t::object);

  if (vm.count("configuration-json")) {
    jsonConfig.update(fileToJson(jsonConfigurationFileName));
  }
  if (vm.count("configuration-shorthand")) {
    jsonConfig.update(ad_utility::ConfigManager::parseShortHand(
        shortHandConfigurationString));
  }

  BenchmarkRegister::parseConfigWithAllRegisteredBenchmarks(jsonConfig);

  // Print all the available configuration options, if wanted.
  if (vm.count("configuration-options")) {
    std::ranges::for_each(
        BenchmarkRegister::getAllRegisteredBenchmarks(),
        [](const BenchmarkInterface* bench) {
          std::cerr << createCategoryTitle(absl::StrCat("Benchmark class '",
                                                        bench->name(), "'"))
                    << "\n"
                    << bench->getConfigManager().printConfigurationDoc(false)
                    << "\n\n";
        });
    exit(0);
  }

  // Measuring the time for all registered benchmarks.
  // For measuring and saving the times.
  const std::vector<BenchmarkResults>& results{
      BenchmarkRegister::runAllRegisteredBenchmarks()};
  /*
  Pairing the measured times up together with the the benchmark classes,
  that created them. Note: All the classes registered in `BenchmarkRegister`
  are always ran in the same order. So the benchmark class and benchmark
  results are always at the same index position, and are grouped togehter
  correctly.
  */
  const auto& benchmarkClassAndResults{ad_utility::zipVectors(
      BenchmarkRegister::getAllRegisteredBenchmarks(), results)};

  // Actually processing the arguments.
  if (vm.count("print")) {
    // Print the results and metadata.
    std::ranges::for_each(benchmarkClassAndResults,
                          [](const auto& pair) {
                            std::cout << benchmarkResultsToString(pair.first,
                                                                  pair.second)
                                      << "\n\n";
                          },
                          {});
  }

  if (vm.count("write")) {
    writeBenchmarkClassAndBenchmarkResultsToJsonFile(
        benchmarkClassAndResults, writeFileName, vm.count("append"));
  }
}
