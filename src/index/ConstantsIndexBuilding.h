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
static const size_t MAX_INTERNAL_LITERAL_BYTES = 1'000'000;

// How many lines are parsed at once during index creation.
// Reduce to save RAM
static const int NUM_TRIPLES_PER_PARTIAL_VOCAB = 10'000'000;

// How many Triples is the Buffer supposed to parse ahead.
// If too big, the memory consumption is high, if too low we possibly lose speed
static const size_t PARSER_BATCH_SIZE = 1'000'000;

// That many triples does the turtle parser have to buffer before the call to
// getline returns (unless our input reaches EOF). This makes parsing from
// streams faster.
static const size_t PARSER_MIN_TRIPLES_AT_ONCE = 10'000;

// When reading from a file, Chunks of this size will
// be fed to the parser at once (10 MiB)
inline std::atomic<size_t> FILE_BUFFER_SIZE = 10 * (1ul << 20);

inline std::atomic<size_t> BUFFER_SIZE_JOIN_PATTERNS_WITH_OSP = 50'000;

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
static const std::string PARTIAL_VOCAB_FILE_NAME = ".tmp.partial-vocabulary.";
static const std::string PARTIAL_MMAP_IDS = ".tmp.partial-ids-mmap.";

// ________________________________________________________________
static const std::string TMP_BASENAME_COMPRESSION =
    ".tmp.for-prefix-compression";

// _________________________________________________________________
// The degree of parallelism that is used for the index building step, where the
// unique elements of the vocabulary are identified via hash maps. Typically, 6
// is a good value. On systems with very few CPUs, a lower value might be
// beneficial.
constexpr size_t NUM_PARALLEL_ITEM_MAPS = 10;

// The number of threads that are parsing in parallel, when the parallel Turtle
// parser is used.
constexpr size_t NUM_PARALLEL_PARSER_THREADS = 8;

// Increasing the following two constants increases the RAM usage without much
// benefit to the performance.

// The number of unparsed blocks of triples, that may wait for parsing at the
// same time
constexpr size_t QUEUE_SIZE_BEFORE_PARALLEL_PARSING = 10;
// The number of parsed blocks of triples, that may wait for parsing at the same
// time
constexpr size_t QUEUE_SIZE_AFTER_PARALLEL_PARSING = 10;

// The blocksize parameter of the parallel vocabulary merging. Higher values
// mean higher memory consumption, wherease a too low value will impact the
// performance negatively.
static constexpr size_t BLOCKSIZE_VOCABULARY_MERGING = 100;

// A buffer size used during the second pass of the Index build.
// It is not const, so we can set it to a much lower value for unit tests to
// increase the test coverage.
inline size_t BUFFER_SIZE_PARTIAL_TO_GLOBAL_ID_MAPPINGS = 10'000;

// The uncompressed size in bytes of a block of a single column of the
// permutations. If chosen too large, then we lose performance for very small
// index scans which always have to read a complete block. If chosen too small,
// the overhead of the metadata that has to be stored per block becomes
// infeasible. 250K seems to be a reasonable tradeoff here.
constexpr ad_utility::MemorySize
    UNCOMPRESSED_BLOCKSIZE_COMPRESSED_METADATA_PER_COLUMN = 250_kB;
