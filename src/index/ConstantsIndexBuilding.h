// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#ifndef QLEVER_SRC_INDEX_CONSTANTSINDEXBUILDING_H
#define QLEVER_SRC_INDEX_CONSTANTSINDEXBUILDING_H

#include <atomic>
#include <cstdint>
#include <string>

#include "util/MemorySize/MemorySize.h"

// Constants which are only used during index creation

// Determines the maximum number of bytes of an internal literal (before
// compression). Every literal larger as this size is externalized regardless
// of its language tag
constexpr inline size_t MAX_INTERNAL_LITERAL_BYTES = 1'000'000;

// How many lines are parsed at once during index creation.
// Reduce to save RAM
constexpr inline int NUM_TRIPLES_PER_PARTIAL_VOCAB = 10'000'000;

// How many Triples is the Buffer supposed to parse ahead.
// If too big, the memory consumption is high, if too low we possibly lose speed
constexpr inline size_t PARSER_BATCH_SIZE = 1'000'000;

// That many triples does the turtle parser have to buffer before the call to
// getline returns (unless our input reaches EOF). This makes parsing from
// streams faster.
constexpr inline size_t PARSER_MIN_TRIPLES_AT_ONCE = 10'000;

constinit inline std::atomic<size_t> BUFFER_SIZE_JOIN_PATTERNS_WITH_OSP =
    50'000;

// When merging the vocabulary, this many finished words are buffered
// before they are written to the output.
constinit inline std::atomic<size_t> BATCH_SIZE_VOCABULARY_MERGE = 10'000'000;

// When the BZIP2 parser encounters a parsing exception it will increase its
// buffer and try again (we have no other way currently to determine if the
// exception was "real" or only because we cut a statement in the middle. Once
// it holds this many bytes in total, it will assume that there was indeed an
// Exception. (Only works safely if no Turtle statement is longer than this
// size. I think currently 1 GB should be enough for this., this is 10MB per
// triple average over 1000 triples.
constexpr inline size_t BZIP2_MAX_TOTAL_BUFFER_SIZE = 1 << 30;

// If a single relation has more than this number of triples, it will be
// buffered into an MmapVector during the creation of the relations;
constexpr inline size_t THRESHOLD_RELATION_CREATION = 2 << 20;

// ________________________________________________________________
constexpr inline std::string_view PARTIAL_VOCAB_WORDS_INFIX =
    ".partial-vocab.words.tmp.";
constexpr inline std::string_view PARTIAL_VOCAB_IDMAP_INFIX =
    ".partial-vocab.idmap.tmp.";

// ________________________________________________________________
constexpr inline std::string_view TMP_BASENAME_COMPRESSION =
    ".tmp.for-prefix-compression";

// _________________________________________________________________
constexpr inline std::string_view QLEVER_INTERNAL_INDEX_INFIX = ".internal";

// _________________________________________________________________
// The degree of parallelism that is used for the index building step, where the
// unique elements of the vocabulary are identified via hash maps. Typically, 6
// is a good value. On systems with very few CPUs, a lower value might be
// beneficial.
constexpr inline size_t NUM_PARALLEL_ITEM_MAPS = 10;

// The number of threads that are parsing in parallel, when the parallel Turtle
// parser is used.
constexpr inline size_t NUM_PARALLEL_PARSER_THREADS = 8;

// Increasing the following two constants increases the RAM usage without much
// benefit to the performance.

// The number of unparsed blocks of triples, that may wait for parsing at the
// same time
constexpr inline size_t QUEUE_SIZE_BEFORE_PARALLEL_PARSING = 10;
// The number of parsed blocks of triples, that may wait for parsing at the same
// time
constexpr inline size_t QUEUE_SIZE_AFTER_PARALLEL_PARSING = 10;

// The blocksize parameter of the parallel vocabulary merging. Higher values
// mean higher memory consumption, whereas a too low value will impact the
// performance negatively.
constexpr inline size_t BLOCKSIZE_VOCABULARY_MERGING = 100;

// A buffer size used during the second pass of the Index build.
// It is not const, so we can set it to a much lower value for unit tests to
// increase the test coverage.
constinit inline std::atomic<size_t> BUFFER_SIZE_PARTIAL_TO_GLOBAL_ID_MAPPINGS =
    10'000;

// The uncompressed size in bytes of a block of a single column of the
// permutations. If chosen too large, then we lose performance for very small
// index scans which always have to read a complete block. If chosen too small,
// the overhead of the metadata that has to be stored per block becomes
// infeasible. 250K seems to be a reasonable tradeoff here.
constexpr inline ad_utility::MemorySize
    UNCOMPRESSED_BLOCKSIZE_COMPRESSED_METADATA_PER_COLUMN =
        ad_utility::MemorySize::kilobytes(250);

constexpr inline size_t NumColumnsIndexBuilding = 4;

// The maximal number of distinct graphs in a block such that this information
// is stored in the metadata of the block.
constexpr inline size_t MAX_NUM_GRAPHS_STORED_IN_BLOCK_METADATA = 20;

#endif  // QLEVER_SRC_INDEX_CONSTANTSINDEXBUILDING_H
