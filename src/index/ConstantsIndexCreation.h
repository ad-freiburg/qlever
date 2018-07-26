// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>
#pragma once

#include <string>

// Constants which are only used during index creation

// here we store all the literals in a text file, which will be externalized.
// This file is only temporary since the final extneralized literals format is
// binary and will be created from this file
const std::string EXTERNAL_LITS_TEXT_FILE_NAME = ".externalized-text";

// How many lines are parsed at once during index creation.
// Reduce to save RAM
static const int NUM_TRIPLES_PER_PARTIAL_VOCAB = 100000000;

// ________________________________________________________________
static const std::string PARTIAL_VOCAB_FILE_NAME = ".partial-vocabulary";
