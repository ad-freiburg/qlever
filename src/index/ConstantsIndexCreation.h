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
static const size_t MAX_INTERNAL_LITERAL_BYTES = 1024;

// How many lines are parsed at once during index creation.
// Reduce to save RAM
static const int NUM_TRIPLES_PER_PARTIAL_VOCAB = 100000000;

// How many Triples is the Buffer supposed to parse ahead.
// If too big, the memory consumption is high, if too low we possibly lose speed
static const size_t PARSER_BATCH_SIZE = 1000000;

// That many triples does the turtle parser have to buffer before the call to
// getline returns (unless our input reaches EOF). This makes parsing from
// streams faster.
static const size_t PARSER_MIN_TRIPLES_AT_ONCE = 1000;

// When reading from a file, Chunks of this size will
// be fed to the parser at once (100 << 20  is exactly 100 MiB
static const size_t FILE_BUFFER_SIZE = 100 << 20;

// When the BZIP2 parser encouters a parsing exception it will increase its
// buffer and try again (we have no other way currently to determine if the
// exception was "real" or only because we cut a statement in the middle. Once
// it holds this many bytes in total, it will assume that there was indeed an
// Exception. (Only works safely if no Turtle statement is longer than this
// size. I think currently 1 GB should be enough for this., this is 10MB per
// triple average over 1000 triples.
static const size_t BZIP2_MAX_TOTAL_BUFFER_SIZE = 1 << 30;

// If a single relation has more than this number of triples, it will be
// buffered into an MmapVector during the creation of the relations;
static const size_t THRESHOLD_RELATION_CREATION = 2 << 20;

// ________________________________________________________________
static const std::string PARTIAL_VOCAB_FILE_NAME = ".partial-vocabulary";
static const std::string PARTIAL_MMAP_IDS = ".partial-ids-mmap";

// ________________________________________________________________
static const std::string TMP_BASENAME_COMPRESSION = ".tmp.compression_index";
