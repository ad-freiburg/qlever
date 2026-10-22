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
// grouped on disk (see `index/vocabulary_merger/PartialVocabularyFile.h`).
//
// NOTE: Each chunk of the parallel vocabulary merge (see
// `PartialVocabularyRunsInput` in that header) reads about one block per
// partial vocabulary beyond the ones it really needs, and holds one decoded
// block per partial vocabulary in memory, so this should be small. But a block
// is also the unit of a single `pread`, so it should not be tiny.
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

// The number of index mappings (which is the same as the number of merged
// words) that are collected in a single batch of the vocabulary merging (see
// `index/vocabulary_merger/WordBatch.h`). This is also the number of words in
// an output block of the parallel merge of the partial vocabularies (see
// `vocabularyMergeOptions` in `VocabularyMergerImpl.h`).
constexpr inline size_t VOCAB_MERGER_WORD_BATCH_SIZE = 100'000;

// The maximal total size of the words in a single batch of the vocabulary
// merging. A batch is handed on as soon as one of this limit and
// `VOCAB_MERGER_WORD_BATCH_SIZE` is reached, such that a batch of very few but
// very long words doesn't become too large.
constexpr inline ad_utility::MemorySize VOCAB_MERGER_WORD_BATCH_MEMORY_SIZE =
    ad_utility::MemorySize::megabytes(10);

// The size of the buffers for the bytes of the blocks of the partial
// vocabulary files, the first of the two pools of the vocabulary merging (see
// `BufferPools` in `index/vocabulary_merger/MergeBlock.h`).
//
// NOTE: A block of a file ends after the word that makes it at least
// `PARTIAL_VOCAB_BLOCK_SIZE` large (see `PartialVocabularyWriter`), so it is a
// bit larger than that. A buffer with a quarter of slack holds every block
// whose last word is at most a quarter of the block size. A block with a
// longer last word gets a larger buffer, which the pool replaces when it is
// given back.
constexpr inline ad_utility::MemorySize VOCAB_MERGER_FILE_BLOCK_BUFFER_SIZE =
    ad_utility::MemorySize::bytes(PARTIAL_VOCAB_BLOCK_SIZE.getBytes() * 5 / 4);

// The size of the buffers that a block of merged words copies its words into,
// the second of the two pools: an eighth of the maximal memory size of a block.
//
// NOTE: A block of merged words ends at `VOCAB_MERGER_WORD_BATCH_SIZE` words or
// at `VOCAB_MERGER_WORD_BATCH_MEMORY_SIZE` of merged words, whichever comes
// first, and most blocks end at the number of words and hold much less than
// the memory size. A single buffer of the full memory size per block would
// therefore mostly be empty, and the memory limit of the merge, which sums the
// sizes of the words, doesn't see that unused capacity. Buffers of an eighth
// of the memory size bound the unused capacity of a block to one partly filled
// buffer (a block has at most eight of them, plus a buffer of its own for each
// word that is longer than this).
constexpr inline ad_utility::MemorySize VOCAB_MERGER_MERGED_WORDS_BUFFER_SIZE =
    ad_utility::MemorySize::bytes(
        VOCAB_MERGER_WORD_BATCH_MEMORY_SIZE.getBytes() / 8);

// The amount of input (the serialized words of the partial vocabularies,
// counted with their repetitions in different partial vocabularies) that a
// chunk of the parallel merge of the partial vocabularies (see
// `mergeVocabulary` in `index/VocabularyMerger.h`) merges. A chunk keeps its
// whole output in memory until the pipeline behind the merge takes it, see
// `vocabularyMergeOptions`.
//
// NOTE: The chunks are cut by bytes and not by words, because the cost of
// comparing two words grows with their length, so that all chunks cost about
// the same to merge.
constexpr inline ad_utility::MemorySize VOCAB_MERGER_INPUT_PER_CHUNK =
    ad_utility::MemorySize::megabytes(128);

// The number of chunks of that merge that may be in flight per thread of the
// pool (bounded further by the memory), see `vocabularyMergeOptions` in
// `VocabularyMergerImpl.h` for why this is more than one.
constexpr inline size_t VOCAB_MERGER_CHUNKS_IN_FLIGHT_PER_THREAD = 4;

// The number of output blocks that a chunk of that merge may buffer before it
// suspends (the thread behind the merge takes the chunks in order), chosen
// such that the whole output of a chunk fits (only the blocks that exist take
// memory). A chunk with more blocks than this doesn't fail, it only suspends
// until its first blocks have been taken.
//
// NOTE: A block ends at `VOCAB_MERGER_WORD_BATCH_SIZE` words or at
// `VOCAB_MERGER_WORD_BATCH_MEMORY_SIZE` of merged words, whichever comes first.
// The serialized form of a word (see `PartialVocabularyWriter`) takes at least
// 17 bytes (the length, the external flag and the index), so the number of
// words cuts a chunk of `VOCAB_MERGER_INPUT_PER_CHUNK` of input into at most
// `VOCAB_MERGER_INPUT_PER_CHUNK / (17 * VOCAB_MERGER_WORD_BATCH_SIZE)` blocks
// (about 75 for the current values), and a merged word takes less than three
// times its serialized size (a `MergedWord` of 32 bytes and an `Occurrence` of
// 16 bytes next to the word, see `MergedBlock::memorySizeOfAppended`), so the
// memory cuts it into fewer than `3 * VOCAB_MERGER_INPUT_PER_CHUNK /
// VOCAB_MERGER_WORD_BATCH_MEMORY_SIZE` blocks (about 39). The factor `8` is a
// round number close to the sum of the two (it is unrelated to the eight
// buffers of a block, see `VOCAB_MERGER_MERGED_WORDS_BUFFER_SIZE`); a chunk
// with more blocks only suspends, see above.
constexpr inline size_t VOCAB_MERGER_NUM_BUFFERED_BLOCKS_PER_CHUNK =
    8 * VOCAB_MERGER_INPUT_PER_CHUNK.getBytes() /
        VOCAB_MERGER_WORD_BATCH_MEMORY_SIZE.getBytes() +
    2;

// The maximal number of batches that may be waiting in each of the queues of
// the merging pipeline of the vocabulary merger (see the comment above
// `mergeVocabulary` in `index/VocabularyMerger.h`). NOTE: A batch keeps all
// the merged words alive that it was created from (at most
// `VOCAB_MERGER_WORD_BATCH_MEMORY_SIZE`, see there), and all of the queues can
// be full at the same time, so the additional memory footprint of the merging
// is a multiple of this number of batches.
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

// The default number of rows of a sub-block of the permutations. Each block is
// split into sub-blocks of about this many rows, which can be read from disk in
// isolation. The selection of the blocks of a scan (for example, the
// prefiltering of a join or of a `FILTER`) works at the granularity of the
// sub-blocks, while the expensive metadata (for example, the graph info) is
// only stored per block (see `SubBlockMetadata.h`).
constexpr inline size_t DEFAULT_INDEX_ROWS_PER_SUB_BLOCK = 4096;

// The key under which the number of rows per sub-block is stored in the
// configuration of an index (`meta-data.json`). The value `0` means that the
// blocks are not split into sub-blocks, which is also the case for all indexes
// that were built before this key existed.
constexpr inline std::string_view INDEX_ROWS_PER_SUB_BLOCK_KEY =
    "index-rows-per-sub-block";

constexpr inline size_t NumColumnsIndexBuilding = 4;

// The maximal number of distinct graphs in a block such that this information
// is stored in the metadata of the block.
constexpr inline size_t MAX_NUM_GRAPHS_STORED_IN_BLOCK_METADATA = 20;

#endif  // QLEVER_SRC_INDEX_CONSTANTSINDEXBUILDING_H
