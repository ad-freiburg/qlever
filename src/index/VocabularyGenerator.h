// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>
#pragma once

#include <string>
#include <utility>

#include "../global/Constants.h"
#include "../global/Id.h"
#include "../util/HashMap.h"
#include "../util/MmapVector.h"

using IdPairMMapVec = ad_utility::MmapVector<std::pair<Id, Id>>;
using IdPairMMapVecView = ad_utility::MmapVectorView<std::pair<Id, Id>>;
using std::string;
// _______________________________________________________________
// merge the partial vocabularies at  binary files
// basename + PARTIAL_VOCAB_FILE_NAME + to_string(i)
// where 0 <= i < numFiles
// Directly Writes .vocabulary file at basename (no more need to pass
// through Vocabulary class
// Writes file "externalTextFile" which can be used to directly write external
// Literals
// Returns the number of total Words merged
size_t mergeVocabulary(const std::string& basename, size_t numFiles);

// _________________________________________________________________________________________
void writePartialIdMapToBinaryFileForMerging(
    const ad_utility::HashMap<string, Id>& map, const string& fileName);

// _________________________________________________________________________________________
ad_utility::HashMap<Id, Id> IdMapFromPartialIdMapFile(
    const string& mmapFilename);
