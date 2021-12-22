// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <stdexcept>
#include <string>

static const size_t STXXL_MEMORY_TO_USE = 1024L * 1024L * 1024L * 2L;
static const size_t STXXL_DISK_SIZE_INDEX_BUILDER = 1000 * 1000;
static const size_t STXXL_DISK_SIZE_INDEX_TEST = 10;

static constexpr size_t DEFAULT_MEM_FOR_QUERIES_IN_GB = 4;

static const size_t DEFAULT_CACHE_MAX_NUM_ENTRIES = 1000;
static const size_t DEFAULT_CACHE_MAX_SIZE_GB = 30;
static const size_t DEFAULT_CACHE_MAX_SIZE_GB_SINGLE_ENTRY = 5;
static const size_t MAX_NOF_ROWS_IN_RESULT = 100000;
static const size_t MIN_WORD_PREFIX_SIZE = 4;
static const char PREFIX_CHAR = '*';
static const char EXTERNALIZED_LITERALS_PREFIX_CHAR{127};
static const std::string EXTERNALIZED_LITERALS_PREFIX{
    EXTERNALIZED_LITERALS_PREFIX_CHAR};
static const char EXTERNALIZED_ENTITIES_PREFIX_CHAR{static_cast<char>(128)};
static const std::string EXTERNALIZED_ENTITIES_PREFIX{
    EXTERNALIZED_ENTITIES_PREFIX_CHAR};
static const size_t MAX_NOF_NODES = 64;
static const size_t MAX_NOF_FILTERS = 64;

static const size_t BUFFER_SIZE_RELATION_SIZE = 1000 * 1000 * 1000;
static const size_t BUFFER_SIZE_DOCSFILE_LINE = 1024 * 1024 * 100;
static const size_t DISTINCT_LHS_PER_BLOCK = 10 * 1000;
static const size_t USE_BLOCKS_INDEX_SIZE_TRESHOLD = 20 * 1000;

static const size_t TEXT_PREDICATE_CARDINALITY_ESTIMATE = 1000 * 1000 * 1000;

static const size_t GALLOP_THRESHOLD = 1000;

static const char CONTAINS_ENTITY_PREDICATE[] =
    "<QLever-internal-function/contains-entity>";
static const char CONTAINS_WORD_PREDICATE[] =
    "<QLever-internal-function/contains-word>";
static const char CONTAINS_WORD_PREDICATE_NS[] = "ql:contains-word";
static const char INTERNAL_TEXT_MATCH_PREDICATE[] =
    "<QLever-internal-function/text>";
static const char HAS_PREDICATE_PREDICATE[] =
    "<QLever-internal-function/has-predicate>";

// For anonymous nodes in Turtle.
static const std::string ANON_NODE_PREFIX = "QLever-Anon-Node";

static const std::string URI_PREFIX = "<QLever-internal-function/";

static const std::string LANGUAGE_PREDICATE = URI_PREFIX + "langtag>";

static const char VALUE_PREFIX[] = ":v:";
static const char VALUE_DATE_PREFIX[] = ":v:date:";
static const char VALUE_FLOAT_PREFIX[] = ":v:float:";
static const char XSD_DATETIME_TYPE[] =
    "http://www.w3.org/2001/XMLSchema#dateTime";
static const char XSD_INT_TYPE[] = "http://www.w3.org/2001/XMLSchema#int";
static const char XSD_FLOAT_TYPE[] = "http://www.w3.org/2001/XMLSchema#float";
static const char XSD_DOUBLE_TYPE[] = "http://www.w3.org/2001/XMLSchema#double";
static const char XSD_DECIMAL_TYPE[] =
    "http://www.w3.org/2001/XMLSchema#decimal";
static const char VALUE_DATE_TIME_SEPARATOR[] = "T";
static const int DEFAULT_NOF_VALUE_INTEGER_DIGITS = 50;
static const int DEFAULT_NOF_VALUE_EXPONENT_DIGITS = 20;
static const int DEFAULT_NOF_VALUE_MANTISSA_DIGITS = 30;
static const int DEFAULT_NOF_DATE_YEAR_DIGITS = 19;

static const std::string MMAP_FILE_SUFFIX = ".meta-mmap";
static const std::string CONFIGURATION_FILE = ".meta-data.json";
static const std::string PREFIX_FILE = ".prefixes";

static const std::string ERROR_IGNORE_CASE_UNSUPPORTED =
    "Key \"ignore-case\" is no longer supported. Please remove this key from "
    "your settings.json and rebuild your index. You can optionally specify the "
    "\"locale\" key, otherwise \"en.US\" will be used as default";
static const std::string WARNING_ASCII_ONLY_PREFIXES =
    "You requested the CTRE parser for tokenization (either via "
    "ascii-prefixes-only = true in the settings.json or by requesting it "
    "explicitly in TurtleParserMain). This means that the input Turtle data "
    "may only use characters from the ASCII range and that no escape sequences "
    "may be used in prefixed names (e.g., rdfs:label\\,el is not allowed). "
    " Additionally, multiline literals are not allowed and triples have to end "
    "at line boundaries (The regex \". *\\n\" must safely identify the end of "
    "a triple)."
    "This is stricter than the SPARQL standard but makes parsing faster. It "
    "works for many Turtle dumps, e.g. that from Wikidata.";
static const std::string LOCALE_DEFAULT_LANG = "en";
static const std::string LOCALE_DEFAULT_COUNTRY = "US";
static constexpr bool LOCALE_DEFAULT_IGNORE_PUNCTUATION = false;

// Constants for the range of valid compression prefixes
// all ASCII- printable characters are left out.
// when adding more special characters to the vocabulary make sure to leave out
// \n since the vocabulary is stored in a text file line by line.
// All prefix codes have a most significant bit of 1. This means the prefix
// codes are never valid UTF-8 and thus it is always able to determine, whether
// this vocabulary was compressed or not.
static constexpr uint8_t MIN_COMPRESSION_PREFIX = 129;
static constexpr uint8_t NUM_COMPRESSION_PREFIXES = 126;
// if this is the first character of a compressed string, this means that no
// compression has been applied to  a word
static const uint8_t NO_PREFIX_CHAR =
    MIN_COMPRESSION_PREFIX + NUM_COMPRESSION_PREFIXES;

// After performing this many "basic operations", we check for timeouts
static constexpr size_t NUM_OPERATIONS_BETWEEN_TIMEOUT_CHECKS = 32000;
// How many "basic operations" (see above) do we assume for a hashset or hashmap
// operation
static constexpr size_t NUM_OPERATIONS_HASHSET_LOOKUP = 32;

// If the time estimate for a sort operation is larger by more than this factor
// than the remaining time, then the sort is canceled with a timeout exception
static constexpr double SORT_ESTIMATE_CANCELLATION_FACTOR = 3.0;

// When initializing a sort performance estimator, at most this percentage of
// the number of triples in the index is being sorted at once.
static constexpr size_t PERCENTAGE_OF_TRIPLES_FOR_SORT_ESTIMATE = 5;

// When asked to make room for X ids in the cache, actually make room for X
// times this factor.
static constexpr double MAKE_ROOM_SLACK_FACTOR = 2;

#ifdef _PARALLEL_SORT
static constexpr bool USE_PARALLEL_SORT = true;
#include <parallel/algorithm>
namespace ad_utility {
template <typename... Args>
auto parallel_sort(Args&&... args) {
  return __gnu_parallel::sort(std::forward<Args>(args)...);
}
using parallel_tag = __gnu_parallel::parallel_tag;

}  // namespace ad_utility

#else
static constexpr bool USE_PARALLEL_SORT = false;
namespace ad_utility {
template <typename... Args>
auto parallel_sort([[maybe_unused]] Args&&... args) {
  throw std::runtime_error(
      "Triggered the parallel sort although it was disabled. Please report to "
      "the developers!");
}
using parallel_tag = int;
}  // namespace ad_utility
#endif
static constexpr size_t NUM_SORT_THREADS = 4;
