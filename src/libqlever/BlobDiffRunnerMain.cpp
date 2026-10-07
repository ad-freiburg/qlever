// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// The command line runner for the weekly blob diff workflow, see
// `BlobDiffProducer.h`.

#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>

#include <boost/program_options.hpp>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <vector>

#include "libqlever/BlobDiffProducer.h"
#include "util/File.h"

namespace po = boost::program_options;
using qlever::BlobDiffConfig;
using qlever::BlobDiffProducer;

namespace {
constexpr std::string_view usage =
    "Usage: qlever-blob-diff <command> [options]\n"
    "Commands:\n"
    "  init     --config C --turtle T --state-dir S [--blob-out FILE]\n"
    "  diff     --config C --state-dir S --turtle T --diff-out FILE\n"
    "           [--blob-out FILE] [--compact]\n"
    "  apply    --base BLOB --diff DIFF --out BLOB\n"
    "  inspect  --blob BLOB [--diff DIFF]\n";

std::vector<char> readFile(const std::string& path) {
  auto stream = ad_utility::makeIfstream(path, std::ios::binary);
  return {std::istreambuf_iterator<char>{stream},
          std::istreambuf_iterator<char>{}};
}

void writeFile(const std::string& path, const std::vector<char>& bytes) {
  auto stream = ad_utility::makeOfstream(path, std::ios::binary);
  stream.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
}

// Parse the options of a command. The first argument (the command itself) is
// skipped.
po::variables_map parse(const po::options_description& options, int argc,
                        char** argv) {
  po::variables_map map;
  po::store(po::parse_command_line(argc - 1, argv + 1, options), map);
  po::notify(map);
  return map;
}

// Print the sizes (in bytes) of a new blob and its diff.
void printSizes(size_t blobSize, size_t diffSize) {
  std::cout << absl::StrFormat(
      "Compressed blob: %d bytes\nDiff file: %d bytes\nDiff/blob ratio: "
      "%.4f\n",
      blobSize, diffSize,
      blobSize == 0 ? 0.0 : static_cast<double>(diffSize) / blobSize);
}

int runInit(int argc, char** argv) {
  std::string configFile, turtle, stateDir, blobOut;
  po::options_description options{"init options"};
  options.add_options()("config", po::value(&configFile)->required(),
                        "The JSON config.")(
      "turtle", po::value(&turtle)->required(), "The Turtle file of week 0.")(
      "state-dir", po::value(&stateDir)->required(),
      "The (empty) state directory.")("blob-out", po::value(&blobOut),
                                      "Also write the blob to this file.");
  parse(options, argc, argv);
  auto config = BlobDiffConfig::fromFile(configFile);
  auto blob = BlobDiffProducer::init(config, turtle, stateDir);
  if (!blobOut.empty()) {
    writeFile(blobOut, blob);
  }
  std::cout << "Initialized the state directory " << stateDir << "\n"
            << "Compressed blob: " << blob.size() << " bytes\n";
  return 0;
}

int runDiff(int argc, char** argv) {
  std::string configFile, turtle, stateDir, diffOut, blobOut;
  po::options_description options{"diff options"};
  options.add_options()("config", po::value(&configFile)->required(),
                        "The JSON config.")(
      "state-dir", po::value(&stateDir)->required(), "The state directory.")(
      "turtle", po::value(&turtle)->required(),
      "The Turtle file of the new week.")(
      "diff-out", po::value(&diffOut)->required(), "Write the diff file here.")(
      "blob-out", po::value(&blobOut), "Also write the new blob here.")(
      "compact", po::bool_switch(),
      "Rebuild the geo indices as a single segment (larger diff).");
  auto map = parse(options, argc, argv);
  auto config = BlobDiffConfig::fromFile(configFile);
  auto result = BlobDiffProducer::step(config, turtle, stateDir,
                                       map["compact"].as<bool>());
  writeFile(diffOut, result.diffFile_);
  if (!blobOut.empty()) {
    writeFile(blobOut, result.blob_);
  }
  std::cout << "Inserted triples: " << result.numInsertedTriples_
            << "\nDeleted triples: " << result.numDeletedTriples_ << "\n"
            << result.statistics_ << "\n";
  printSizes(result.blob_.size(), result.diffFile_.size());
  return 0;
}

int runApply(int argc, char** argv) {
  std::string base, diff, out;
  po::options_description options{"apply options"};
  options.add_options()("base", po::value(&base)->required(),
                        "The previous blob.")(
      "diff", po::value(&diff)->required(), "The diff file.")(
      "out", po::value(&out)->required(), "Write the new blob here.");
  parse(options, argc, argv);
  auto result = BlobDiffProducer::apply(readFile(base), readFile(diff));
  writeFile(out, result);
  std::cout << "Wrote the new blob (" << result.size() << " bytes) to " << out
            << "\n";
  return 0;
}

int runInspect(int argc, char** argv) {
  std::string blob, diff;
  po::options_description options{"inspect options"};
  options.add_options()("blob", po::value(&blob)->required(), "The blob.")(
      "diff", po::value(&diff), "A diff file whose target is the blob.");
  parse(options, argc, argv);
  auto blobBytes = readFile(blob);
  std::optional<std::vector<char>> diffBytes;
  if (!diff.empty()) {
    diffBytes = readFile(diff);
  }
  std::optional<ql::span<const char>> diffSpan;
  if (diffBytes.has_value()) {
    diffSpan = ql::span<const char>{diffBytes->data(), diffBytes->size()};
  }
  std::cout << BlobDiffProducer::inspect(blobBytes, diffSpan) << "\n";
  return 0;
}
}  // namespace

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << usage;
    return 1;
  }
  std::string command = argv[1];
  try {
    if (command == "init") {
      return runInit(argc, argv);
    } else if (command == "diff") {
      return runDiff(argc, argv);
    } else if (command == "apply") {
      return runApply(argc, argv);
    } else if (command == "inspect") {
      return runInspect(argc, argv);
    }
    std::cerr << "Unknown command \"" << command << "\"\n" << usage;
    return 1;
  } catch (const std::exception& e) {
    std::cerr << "Error: " << e.what() << std::endl;
    return 1;
  }
}
