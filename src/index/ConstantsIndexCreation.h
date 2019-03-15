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

// Determines the maximum number of bytes of an internal literal (before
// compression). Every literal larger as this size is externalized regardless
// of its language tag
static const size_t MAX_INTERNAL_LITERAL_BYTES = 100;

// How many lines are parsed at once during index creation.
// Reduce to save RAM
static const int NUM_TRIPLES_PER_PARTIAL_VOCAB = 100000000;

// How many Triples is the Buffer supposed to parse ahead.
// If too big, the memory consumption is high, if too low we possibly lose speed
static const size_t PARSER_BATCH_SIZE = 10000000;

// If a single relation has more than this number of triples, it will be
// buffered into an MmapVector during the creation of the relations;
static const size_t THRESHOLD_RELATION_CREATION = 2 << 20;

// ________________________________________________________________
static const std::string PARTIAL_VOCAB_FILE_NAME = ".partial-vocabulary";
static const std::string PARTIAL_MMAP_IDS = ".partial-ids-mmap";
