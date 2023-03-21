// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2022, schlegea@informatik.uni-freiburg.de)
#include <boost/program_options/value_semantic.hpp>
#include <iostream>
#include <fstream>
#include <ios>
#include <iomanip>
#include <sstream>
#include <algorithm>
#include <vector>

#include <boost/program_options.hpp>
#include "BenchmarkConfiguration.h"
#include "util/json.h"
#include "util/File.h"
#include "../benchmark/Benchmark.h"
#include "../benchmark/BenchmarkToJson.h"
#include "../benchmark/BenchmarkRecordToString.h"


/*
 * @brief Write the json object to the specified file.
 *
 * @param fileName The name of the file, where the json informationen
 *  should be written in.
 * @param appendToFile Should the json informationen be appended to the end
 *  of the file, or should the previous content be overwritten?
 */
void writeJsonToFile(nlohmann::json j, std::string fileName,
    bool appendToFile = false) {
  ad_utility::makeOfstream(fileName, (appendToFile) ?
      (std::ios::out | std::ios::app) : (std::ios::out)) << j;
}

/*
 * @brief Goes through all types of registered benchmarks, measures their time
 * and prints their measured time in a fitting format.
 */
int main(int argc, char** argv) {
  // The filename, should the json option be choosen.
  std::string jsonFileName = "";
  // The short hand, or json string, for the benchmark configuration.
  std::string benchmarkConfigurationString = "";

  // Declaring the supported options.
  boost::program_options::options_description options("Options for the"
      " benchmark");
  options.add_options()
      ("help,h", "Print the help message.")
      ("print,p", "Roughly prints all benchmarks.")
      ("write,w", boost::program_options::value<std::string>(&jsonFileName),
       "Writes the benchmarks as json to a file, overriding the previous"
       " content of the file.")
      ("append,a", "Causes the json option to append to the end of the"
      " file, instead of overriding the previous content of the file.")
      ("configuration-shorthand,s",
      boost::program_options::value<std::string>(&benchmarkConfigurationString),
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
    std::cerr << "You have to specify at least one of the options of print,"
      " or json. Running the benchmarks without outputting their resulsts,"
      " is just a waste of time.\n";
    printUsageAndExit();
  }

  // Parsing the given arguments.
  boost::program_options::variables_map vm;
  boost::program_options::store(
      boost::program_options::parse_command_line(argc, argv, options), vm);
  boost::program_options::notify(vm);

  // Did they set any option, that would require anything to actually happen?
  // If not, don't do anything. This should also happen, if they explicitly
  // wanted to see the `help` option.
  if (vm.count("help") || !(vm.count("print") || vm.count("write"))){
    printUsageAndExit();
  }

  // Did we get configuration short hand?
  if (vm.count("configuration-shorthand")){
    BenchmarkConfiguration config{};
    config.parseShortHand(benchmarkConfigurationString);
    BenchmarkRegister::passConfigurationToAllRegisteredBenchmarks(config);
  }

  // Measuring the time for all registered benchmarks.
  // For measuring and saving the times.
  const std::vector<BenchmarkRecords>&
    records{BenchmarkRegister::runAllRegisteredBenchmarks()};

  // Actually processing the arguments.
  if (vm.count("print")) {
    std::ranges::for_each(records, [](const BenchmarkRecords& record){
      std::cout << benchmarkRecordsToString(record) << "\n";
    }, {});
  }

  if (vm.count("write")) {
    writeJsonToFile(zipGeneralMetadataAndBenchmarkRecordsToJson(
      BenchmarkRegister::getAllGeneralMetadata(), records), jsonFileName,
        vm.count("append"));
  }
}
