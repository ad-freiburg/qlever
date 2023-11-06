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
inline std::atomic<size_t>& FILE_BUFFER_SIZE() {
  static std::atomic<size_t> fileBufferSize = 10 * (1ul << 20);
  return fileBufferSize;
}

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

// The uncompressed size in bytes of a block of the permutations.
//
// NOTE: This used to be `1 << 23` (over 8M), which is fairly large (we always
// need to decompress at least one whole block, even when reading only few
// triples). With 100K, the total space for all the `CompressedBlockMetadata` is
// still small compared to the rest of the index. However, with 100K, and single
// block is just 10K compresse, which might result in sub-optimal IO-efficiency
// when reading many blocks. We take 500K as a compromise.
constexpr size_t BLOCKSIZE_COMPRESSED_METADATA = 500'000;
