// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2022, schlegea@informatik.uni-freiburg.de)
#include <boost/program_options/value_semantic.hpp>
#include <iostream>
#include <ostream>
#include <fstream>
#include <ios>
#include <iomanip>
#include <sstream>
#include <algorithm>
#include <vector>

#include <boost/program_options.hpp>
#include "../benchmark/infrastructure/BenchmarkConfiguration.h"
#include "util/json.h"
#include "util/File.h"
#include "../benchmark/infrastructure/Benchmark.h"
#include "../benchmark/infrastructure/BenchmarkToJson.h"
#include "../benchmark/infrastructure/BenchmarkResultToString.h"


/*
 * @brief Write the json object to the specified file.
 *
 * @param fileName The name of the file, where the json informationen
 *  should be written in.
 * @param appendToFile Should the json informationen be appended to the end
 *  of the file, or should the previous content be overwritten?
 */
static void writeJsonToFile(nlohmann::json j, const std::string& fileName,
    bool appendToFile = false) {
  ad_utility::makeOfstream(fileName, appendToFile ?
      (std::ios::out | std::ios::app) : std::ios::out) << j;
}

static std::string readFileToString(const std::string& fileName){
  // The string gets build using a string stream.
  std::ostringstream transchribedString{};

  // Adding a buffer using `<<` causes the content of the buffer to be
  // individually added to the string stream. In other words: The entire
  // content of the opened file will be added.
  transchribedString << ad_utility::makeOfstream(fileName).rdbuf();

  return transchribedString.str();
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
  options.add_options()
      ("help,h", "Print the help message.")
      ("print,p", "Roughly prints all benchmarks.")
      ("write,w", po::value<std::string>(&writeFileName),
       "Writes the benchmarks as json to a file, overriding the previous"
       " content of the file.")
      ("append,a", "Causes the json option to append to the end of the"
      " file, instead of overriding the previous content of the file.")
      ("configuration-json,j",
      po::value<std::string>(&jsonConfigurationFileName),
      "Set the configuration of benchmarks as described in a json file.")
      ("configuration-shorthand,s",
      po::value<std::string>(&shortHandConfigurationString),
      "Allows you to add options to configuration of the benchmarks using the"
      " short hand described in `BenchmarkConfiguration.h:parseShortHand`.")
  ;

  // Prints how to use the file correctly and exits.
  auto printUsageAndExit = [&options](){
    std::cerr << options << "\n";
    exit(1);
  };

  // Calling without using ANY arguments makes no sense.
  if (argc == 1) {
    std::cerr << "You have to specify at least one of the options of `--print`,"
      " or `--write`.\n";
    printUsageAndExit();
  }

  // Parsing the given arguments.
  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, options), vm);
  po::notify(vm);

  // Did they set any option, that would require anything to actually happen?
  // If not, don't do anything. This should also happen, if they explicitly
  // wanted to see the `help` option.
  if (vm.count("help") || !(vm.count("print") || vm.count("write"))){
    printUsageAndExit();
  }

  // Did we get any configuration?
  ad_benchmark::BenchmarkConfiguration config{};

  if (vm.count("configuration-json")){
    config.setJsonString(readFileToString(jsonConfigurationFileName));
  }
  if (vm.count("configuration-shorthand")){
    config.addShortHand(shortHandConfigurationString);
  }

  // Pass the configuration, even if it is empty.
  ad_benchmark::BenchmarkRegister::passConfigurationToAllRegisteredBenchmarks(
    config);

  // Measuring the time for all registered benchmarks.
  // For measuring and saving the times.
  const std::vector<ad_benchmark::BenchmarkResults>&
    results{ad_benchmark::BenchmarkRegister::runAllRegisteredBenchmarks()};

  // Actually processing the arguments.
  if (vm.count("print")) {
    std::ranges::for_each(results, [](
      const ad_benchmark::BenchmarkResults& result){
      std::cout << benchmarkResultsToString(result) << "\n";
    }, {});
  }

  if (vm.count("write")) {
    writeJsonToFile(zipGeneralMetadataAndBenchmarkResultsToJson(
      ad_benchmark::BenchmarkRegister::getAllGeneralMetadata(), results),
      writeFileName, vm.count("append"));
  }
}
