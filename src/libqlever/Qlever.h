//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <util/MemorySize/MemorySize.h>

#include <optional>
#include <string>
#include <vector>

#include "index/Index.h"
#include "index/InputFileSpecification.h"
#include "util/AllocatorWithLimit.h"

namespace qlever {

struct IndexBuilderConfig {
  std::string baseName;
  std::string wordsfile;
  std::string docsfile;
  std::string textIndexName;
  std::string kbIndexName;
  std::string settingsFile;
  std::vector<qlever::InputFileSpecification> inputFiles;
  bool noPatterns = false;
  bool onlyAddTextIndex = false;
  bool keepTemporaryFiles = false;
  bool onlyPsoAndPos = false;
  bool addWordsFromLiterals = false;
  std::optional<ad_utility::MemorySize> stxxlMemory;
};

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

class Qlever {
  void buildIndex(IndexBuilderConfig config) {
    Index index{ad_utility::makeUnlimitedAllocator<Id>()};

    if (config.stxxlMemory.has_value()) {
      index.memoryLimitIndexBuilding() = config.stxxlMemory.value();
    }
    // If no text index name was specified, take the part of the wordsfile after
    // the last slash.
    if (config.textIndexName.empty() && !config.wordsfile.empty()) {
      config.textIndexName =
          ad_utility::getLastPartOfString(config.wordsfile, '/');
    }
    try {
      LOG(TRACE) << "Configuring STXXL..." << std::endl;
      size_t posOfLastSlash = config.baseName.rfind('/');
      string location = config.baseName.substr(0, posOfLastSlash + 1);
      string tail = config.baseName.substr(posOfLastSlash + 1);
      writeStxxlConfigFile(location, tail);
      string stxxlFileName = getStxxlDiskFileName(location, tail);
      LOG(TRACE) << "done." << std::endl;

      index.setKbName(config.kbIndexName);
      index.setTextName(config.textIndexName);
      index.usePatterns() = !config.noPatterns;
      index.setOnDiskBase(config.baseName);
      index.setKeepTempFiles(config.keepTemporaryFiles);
      index.setSettingsFile(config.settingsFile);
      index.loadAllPermutations() = !config.onlyPsoAndPos;

      if (!config.onlyAddTextIndex) {
        AD_CONTRACT_CHECK(!config.inputFiles.empty());
        index.createFromFiles(config.inputFiles);
      }

      if (!config.wordsfile.empty() || config.addWordsFromLiterals) {
        index.addTextFromContextFile(config.wordsfile,
                                     config.addWordsFromLiterals);
      }

      if (!config.docsfile.empty()) {
        index.buildDocsDB(config.docsfile);
      }
      ad_utility::deleteFile(stxxlFileName, false);
    } catch (std::exception& e) {
      LOG(ERROR) << "Creating the index for QLever failed with the following "
                    "exception: "
                 << e.what() << std::endl;
      throw;
    }
  }
};
}  // namespace qlever
