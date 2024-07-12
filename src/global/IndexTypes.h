//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "./TypedIndex.h"
#include "./VocabIndex.h"
#include "engine/LocalVocabEntry.h"

// Typedefs for several kinds of typed indices that are used across QLever.

using LocalVocabIndex = const LocalVocabEntry*;
using TextRecordIndex = ad_utility::TypedIndex<uint64_t, "TextRecordIndex">;
using WordVocabIndex = ad_utility::TypedIndex<uint64_t, "WordVocabIndex">;
using BlankNodeIndex = ad_utility::TypedIndex<uint64_t, "BlankNodeIndex">;
