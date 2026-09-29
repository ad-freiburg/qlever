// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#ifndef QLEVER_SRC_INDEX_CONSTANTSINDEXBUILDING_H
#define QLEVER_SRC_INDEX_CONSTANTSINDEXBUILDING_H

#include <atomic>
#include <cstdint>
#include <string>
#include <string_view>

#include "util/MemorySize/MemorySize.h"

// Constants which are only used during index creation

// Determines the maximum number of bytes of an internal literal (before
// compression). Every literal larger as this size is externalized regardless
// of its language tag
constexpr inline size_t MAX_INTERNAL_LITERAL_BYTES = 1'000'000;

// How many lines are parsed at once during index creation.
// Reduce to save RAM
constexpr inline int NUM_TRIPLES_PER_PARTIAL_VOCAB = 10'000'000;

// That many triples does the turtle parser have to buffer before the call to
// `getBatch` returns (unless our input reaches EOF). This makes parsing from
// streams faster.
constexpr inline size_t PARSER_MIN_TRIPLES_AT_ONCE = 10'000;

inline std::atomic<size_t>& BUFFER_SIZE_JOIN_PATTERNS_WITH_OSP() {
  static std::atomic<size_t> value = 50'000;
  return value;
}

// When the parser encounters a parsing exception it will increase its
// buffer and try again (we have no other way currently to determine if the
// exception was "real" or only because we cut a statement in the middle. Once
// it holds this many bytes in total, it will assume that there was indeed an
// Exception. (Only works safely if no Turtle statement is longer than this
// size. I think currently 1 GB should be enough for this, this is 10MB per
// triple average over 100 triples.
// Not const so it can be lowered in unit tests to increase coverage.
inline ad_utility::MemorySize& RDF_PARSER_MAX_TOTAL_BUFFER_SIZE() {
  static ad_utility::MemorySize value = ad_utility::MemorySize::gigabytes(1);
  return value;
}

// ________________________________________________________________
constexpr inline std::string_view PARTIAL_VOCAB_WORDS_INFIX =
    ".partial-vocab.words.tmp.";
// The size of the blocks into which the words of a partial vocabulary are
// grouped on disk (see `index/vocabulary_merger/PartialVocabularyFile.h`). Each
// chunk of the parallel vocabulary merge reads about one block per partial
// vocabulary beyond the ones it really needs, and holds one decoded block per
// partial vocabulary in memory, so this should be small; but a block is also
// the unit of a single `pread`, so it should not be tiny.
constexpr inline ad_utility::MemorySize PARTIAL_VOCAB_BLOCK_SIZE =
    ad_utility::MemorySize::kilobytes(32);
constexpr inline std::string_view PARTIAL_VOCAB_IDMAP_INFIX =
    ".partial-vocab.idmap.tmp.";

// The infix of the (compressed) files that hold the parsed triples with their
// partial IDs, before they are sorted into the permutations. There is one such
// file per partial vocabulary, holding exactly the triples that were mapped
// using it (see `unsortedTriplesFilename` in
// `index/PartialVocabularyFilenames.h`).
constexpr inline std::string_view UNSORTED_TRIPLES_INFIX = ".unsorted-triples.";

// _________________________________________________________________
constexpr inline std::string_view QLEVER_INTERNAL_INDEX_INFIX = ".internal";

// The number of threads that are parsing in parallel, when the parallel Turtle
// parser is used.
constexpr inline size_t NUM_PARALLEL_PARSER_THREADS = 8;

// Increasing the following two constants increases the RAM usage without much
// benefit to the performance.

// The number of unparsed blocks of triples, that may wait for parsing at the
// same time
constexpr inline size_t QUEUE_SIZE_BEFORE_PARALLEL_PARSING = 10;

// The number of words (and their maximal total size) in an output block of the
// parallel merge of the partial vocabularies, see `vocabularyMergeOptions` in
// `VocabularyMergerImpl.h`. The second limit keeps a block of very few but
// very long words from becoming too large.
constexpr inline size_t VOCAB_MERGER_WORD_BATCH_SIZE = 100'000;
constexpr inline ad_utility::MemorySize VOCAB_MERGER_WORD_BATCH_MEMORY_SIZE =
    ad_utility::MemorySize::megabytes(10);

// The number of merged words after which the vocabulary merger cuts a new
// segment (see `index/vocabulary_merger/Segment.h`), which is the unit of work
// of the stages behind the merge. Larger segments mean fewer tasks but more
// memory per task and a later start of the writing.
constexpr inline size_t VOCAB_MERGER_SEGMENT_NUM_WORDS = 1u << 20;

// The estimated memory per segment that is being built or waiting to be
// committed (its merged words plus its outputs), which bounds the number of
// segments in flight together with the memory limit of the index build.
constexpr inline ad_utility::MemorySize VOCAB_MERGER_MEMORY_PER_SEGMENT =
    ad_utility::MemorySize::megabytes(256);

// The number of threads that write the partial ID maps, see
// `index/vocabulary_merger/IdMapWriters.h`.
constexpr inline size_t VOCAB_MERGER_NUM_ID_MAP_WRITER_THREADS = 8;

// The amount of input (the serialized words of the partial vocabularies,
// counted with their repetitions in different partial vocabularies) that a
// chunk of the parallel merge of the partial vocabularies (see
// `mergeVocabulary` in `index/VocabularyMerger.h`) merges. The chunks are cut
// by bytes and not by words, because the cost of comparing two words grows
// with their length, so that all chunks cost about the same to merge. A chunk
// keeps its whole output in memory until the pipeline behind the merge takes
// it, see `vocabularyMergeOptions`.
constexpr inline ad_utility::MemorySize VOCAB_MERGER_INPUT_PER_CHUNK =
    ad_utility::MemorySize::megabytes(128);
// The number of output blocks that a chunk of that merge may buffer before it
// suspends (the thread behind the merge takes the chunks in order). A block
// holds at most `VOCAB_MERGER_WORD_BATCH_MEMORY_SIZE` of merged words and
// the output of a chunk is at most a few times its input, so this is enough
// for the whole output of a chunk; only the blocks that exist take memory.
constexpr inline size_t VOCAB_MERGER_NUM_BUFFERED_BLOCKS_PER_CHUNK =
    8 * VOCAB_MERGER_INPUT_PER_CHUNK.getBytes() /
        VOCAB_MERGER_WORD_BATCH_MEMORY_SIZE.getBytes() +
    2;
// The maximal number of segments that may be waiting in each of the queues of
// the ID map writers of the vocabulary merger (see
// `index/vocabulary_merger/IdMapWriters.h`).
constexpr inline size_t VOCAB_MERGER_WORD_BATCH_QUEUE_SIZE = 3;

// The default number of rows of a block of the permutations (and of the other
// sorted lists of an index). If chosen too large, then we lose performance for
// very small index scans which always have to read a complete block. If chosen
// too small, the overhead of the metadata that has to be stored per block
// becomes infeasible. 31250 rows (250 kB per column) seems to be a reasonable
// tradeoff here.
constexpr inline size_t DEFAULT_INDEX_ROWS_PER_BLOCK = 31'250;

// The largest number of rows per block that an index can be built with. The
// index builder holds several blocks in RAM at the same time, so a much larger
// value would only exhaust the memory. The bound also catches a negative value
// on the command line, which the option parser turns into a huge number.
constexpr inline size_t MAX_INDEX_ROWS_PER_BLOCK =
    100 * DEFAULT_INDEX_ROWS_PER_BLOCK;

// The key under which the number of rows per block is stored in the
// configuration of an index (`meta-data.json`). It is stored because an index
// can be built with a non-default block size, and everything that writes
// sorted lists of an existing index afterwards has to use the block size of
// that index. Indexes built before this key existed simply use the default.
constexpr inline std::string_view INDEX_ROWS_PER_BLOCK_KEY =
    "index-rows-per-block";

constexpr inline size_t NumColumnsIndexBuilding = 4;

// The maximal number of distinct graphs in a block such that this information
// is stored in the metadata of the block.
constexpr inline size_t MAX_NUM_GRAPHS_STORED_IN_BLOCK_METADATA = 20;

#endif  // QLEVER_SRC_INDEX_CONSTANTSINDEXBUILDING_H
