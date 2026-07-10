// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/UpdateMetadata.h"

// ____________________________________________________________________________
void to_json(nlohmann::json& j, const DeltaTriplesCount& count) {
  j = nlohmann::json{{"inserted", count.triplesInserted_},
                     {"deleted", count.triplesDeleted_},
                     {"total", count.triplesInserted_ + count.triplesDeleted_}};
}
