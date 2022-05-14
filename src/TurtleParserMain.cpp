// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>

#include <getopt.h>

#include <array>
#include <fstream>
#include <iostream>
#include <string>

#include "./parser/TurtleParser.h"
#include "./util/Log.h"

/**
 * @brief Instantiate a Parser that parses filename and writes the resulting
 * triples to argument out.
 *
 * @tparam Parser A Parser that supports a call to getline that yields a triple
 * @param out the parsed triples are written to this file
 * @param filename the filename from which the triples are parsed, can be
 * "/dev/stdin"
 */
template <class Parser>
void writeNTImpl(std::ostream& out, const std::string& filename) {
  Parser p(filename);
  TurtleTriple triple;
  size_t numTriples = 0;
  while (p.getLine(triple)) {
    out << triple._subject << " " << triple._predicate << " "
        << triple._object.toRdfLiteral() << " .\n";
    numTriples++;
    if (numTriples % 10000000 == 0) {
      LOG(INFO) << "Parsed " << numTriples << " triples" << std::endl;
    }
  }
}

template <class Parser>
void writeLabel(const std::string& filename) {
  Parser p(filename);
  std::array<std::string, 3> triple;
  size_t numTriples = 0;
  std::unordered_set<std::string> entities;
  while (p.getLine(triple)) {
    entities.insert(std::move(triple[0]));
    entities.insert(std::move(triple[1]));
    entities.insert(std::move(triple[2]));
    numTriples++;
    if (numTriples % 10000000 == 0) {
      LOG(INFO) << "Parsed " << numTriples << " triples" << std::endl;
    }
  }
  for (const auto& t : entities) {
    if (t.starts_with('<')) {
      std::cout << t << " <qlever_label> \"" << t.substr(1, t.size() - 2)
                << ".\n";
    }
  }
}

/**
 * @brief Decide according to arg fileFormat which parser to use.
 * Then call writeNTImpl with the appropriate parser
 * @param out Parsed triples will be written here.
 * @param fileFormat One of [ttl|mmap]
 * @param filename Will read from this file, might be /dev/stdin
 */
template <class Tokenizer_T>
void writeNT(std::ostream& out, const string& fileFormat,
             const std::string& filename) {
  if (fileFormat == "ttl" || fileFormat == "nt") {
    // writeLabel<TurtleStreamParser<Tokenizer_T>>(out, filename);
    writeNTImpl<TurtleStreamParser<Tokenizer_T>>(out, filename);
  } else if (fileFormat == "mmap") {
    writeNTImpl<TurtleMmapParser<Tokenizer_T>>(out, filename);
  } else {
    LOG(ERROR) << "writeNT was called with unknown file format " << fileFormat
               << ". This should never happen, terminating" << std::endl;
    LOG(ERROR) << "Please specify a valid file format" << std::endl;
    exit(1);
  }
}

void writeNTDispatch(std::ostream& out, const string& fileFormat,
                     const std::string& filename,
                     const std::string& regexEngine) {
  if (regexEngine == "re2") {
    writeNT<Tokenizer>(out, fileFormat, filename);
  } else if (regexEngine == "ctre") {
    LOG(INFO) << WARNING_ASCII_ONLY_PREFIXES << std::endl;
    writeNT<TokenizerCtre>(out, fileFormat, filename);
  } else {
    LOG(ERROR)
        << "Please specify a valid regex engine via the -r flag. "
           "Options are \"re2\" or \"ctre\" (The latter only works correct if "
           "prefix names only use ascii characters but is faster.\n";
    exit(1);
  }
}

// _______________________________________________________________________________________________________________
void printUsage(char* execName) {
  std::ios coutState(nullptr);
  coutState.copyfmt(cout);
  cout << std::setfill(' ') << std::left;

  cout << "Usage: " << execName << " -i <index> [OPTIONS]" << endl << endl;
  cout << "Options" << endl;
  cout << "  " << std::setw(20) << "F, file-format" << std::setw(1) << "    "
       << " Specify format of the input file. Must be one of "
          "[nt|ttl|mmap]."
       << " " << std::setw(36)
       << "If not set, we will try to deduce from the filename" << endl
       << " " << std::setw(36)
       << "(mmap assumes an on-disk turtle file that can be mmapped to memory)"
       << endl;
  cout << "  " << std::setw(20) << "i, input-file" << std::setw(1) << "    "
       << " The file to be parsed from. If omitted, we will read from stdin"
       << endl;
  cout << "  " << std::setw(20) << "o, output-file" << std::setw(1) << "    "
       << " The NTriples file to be Written to. If omitted, we will write to "
          "stdout"
       << endl;
  cout << "  " << std::setw(20) << "r, regex-engine" << std::setw(1) << "    "
       << R"( The regex engine used for lexing. Must be one of "re2" or "ctre")"
       << endl;
  cout.copyfmt(coutState);
}

// ________________________________________________________________________
int main(int argc, char** argv) {
  // we possibly write to stdout to pipe it somewhere else, so redirect all
  // logging output to std::err
  ad_utility::setGlobalLogginStream(&std::cerr);
  struct option options[] = {{"help", no_argument, NULL, 'h'},
                             {"file-format", required_argument, NULL, 'F'},
                             {"input-file", required_argument, NULL, 'i'},
                             {"output-file", required_argument, NULL, 'o'},
                             {"regex-engine", required_argument, NULL, 'r'},
                             {NULL, 0, NULL, 0}};

  string inputFile, outputFile, fileFormat, regexEngine;
  while (true) {
    int c = getopt_long(argc, argv, "F:i:o:r:h", options, nullptr);
    if (c == -1) {
      break;
    }
    switch (c) {
      case 'h':
        printUsage(argv[0]);
        return 0;
        break;
      case 'i':
        inputFile = optarg;
        break;
      case 'o':
        outputFile = optarg;
        break;
      case 'F':
        fileFormat = optarg;
        break;
      case 'r':
        regexEngine = optarg;
        break;
      default:
        cout << endl
             << "! ERROR in processing options (getopt returned '" << c
             << "' = 0x" << std::setbase(16) << c << ")" << endl
             << endl;
        printUsage(argv[0]);
        exit(1);
    }
  }

  if (fileFormat.empty()) {
    bool filetypeDeduced = false;
    if (inputFile.ends_with(".nt")) {
      fileFormat = "nt";
      filetypeDeduced = true;
    } else if (inputFile.ends_with(".ttl")) {
      fileFormat = "ttl";
      filetypeDeduced = true;
    } else {
      LOG(WARN)
          << " Could not deduce the type of the input knowledge-base-file by "
             "its extension. Assuming the input to be turtle. Please specify "
             "--file-format (-F)\n";
      LOG(WARN) << "In case this is not correct \n";
    }
    if (filetypeDeduced) {
      LOG(INFO) << "Assuming input file format to be " << fileFormat
                << " due to the input file's extension.\n";
      LOG(INFO)
          << "If this is wrong, please manually specify the --file-format "
             "(-F) flag.\n";
    }
  }

  if (inputFile.empty()) {
    LOG(INFO) << "No input file was specified, parsing from stdin\n";
    inputFile = "/dev/stdin";
  } else if (inputFile == "-") {
    LOG(INFO) << "Parsing from stdin\n";
    inputFile = "/dev/stdin";
  }

  LOG(INFO) << "Trying to parse from input file " << inputFile << std::endl;

  if (!outputFile.empty()) {
    std::ofstream of(outputFile);
    if (!of) {
      LOG(ERROR) << "Error opening '" << outputFile << "'" << std::endl;
      printUsage(argv[0]);
      exit(1);
    }
    LOG(INFO) << "Writing to file " << outputFile << std::endl;
    writeNTDispatch(of, fileFormat, inputFile, regexEngine);
    of.close();
  } else {
    LOG(INFO) << "Writing to stdout" << std::endl;
    writeNTDispatch(std::cout, fileFormat, inputFile, regexEngine);
  }
}
