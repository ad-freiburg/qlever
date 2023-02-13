// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (November of 2022, schlegea@informatik.uni-freiburg.de)

#include <iostream>
#include <fstream>
#include <ios>
#include <iomanip>
#include <sstream>
#include <algorithm>
#include <vector>

#include <boost/program_options.hpp>
#include "util/json.h"
#include "../benchmark/Benchmark.h"
#include "../benchmark/BenchmarkRecordToJson.h"
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
  // Using this constructor, the file is already opened.
  std::ofstream myFile(fileName, (appendToFile) ?
      (std::ios::out | std::ios::app) : (std::ios::out));

  // Error message, if the file couldn't be opened.
  if (!myFile.is_open()){
    std::cerr << "Unable to open file '" << fileName << "'.";
  }

  // nlohmann already has support for things like this.
  myFile << j;

  myFile.close();
}

/*
 * @brief Goes through all types of registered benchmarks, measures their time
 * and prints their measured time in a fitting format.
 */
int main(int argc, char** argv) {
  // Prints how to use the file correctly and exits.
  auto printUsageAndExit = [&argv](){
    std::cerr << "Usage: ./" << argv[0] << " [options]\n"
      << " --help, -h: Print this help message.\n"
      << " --print, -p: Roughly prints all benchmarks.\n"
      << " --json, -j <file>: Writes the benchmarks as json to a file,"
      " overriding the previous content of the file.\n"
      << " --append, -a: Causes the json option to append to the end of the"
      "file, instead of overriding the previous content of the file.\n";
    exit(1);
  };

  // Calling without using ANY arguments makes no sense.
  if (argc == 1) {printUsageAndExit();}

  // The filename, should the json option be choosen.
  std::string jsonFileName = "";

  // Declaring the supported options.
  boost::program_options::options_description options("options");
  options.add_options()
      ("help,h", "Print the help message.")
      ("print,p", "Roughly prints all benchmarks.")
      ("json,j", boost::program_options::value<std::string>(&jsonFileName),
       "Writes the benchmarks as json to a file, overriding the previous"
       "content of the file.")
      ("append,a", "Causes the json option to append to the end of the"
      "file, instead of overriding the previous content of the file.")
  ;

  // Parsing the given arguments.
  boost::program_options::variables_map vm;
  boost::program_options::store(
      boost::program_options::parse_command_line(argc, argv, options), vm);
  boost::program_options::notify(vm);

  // Did they want the help option?
  if (vm.count("help")) {printUsageAndExit();}

  // We got at least one argument at this point and all options need all the
  // benchmarks measured, so time to do that.

  // Measuring the time for all registered benchmarks.
  // For measuring and saving the times.
  const BenchmarkRecords&
    records{BenchmarkRegister::runAllRegisteredBenchmarks()};

  // Actually processing the arguments.
  if (vm.count("print")) {
    std::cout << benchmarkRecordsToString(records) << "\n";
  };

  if (vm.count("json")) {
    writeJsonToFile(benchmarksToJson(records), jsonFileName,
        vm.count("append"));
  };
}
