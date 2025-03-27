//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <cstdint>

#include "./TypedIndex.h"

// TODO<joka921> Rename `VocabIndex` to `RdfVocabIndex` at a point in time where
// this (very intrusive) renaming doesn't interfere with too many open pull
// requests.
using VocabIndex = ad_utility::TypedIndex<uint64_t, "VocabIndex">;
