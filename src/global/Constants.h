// Copyright 2023 - 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Björn Buchhold <buchhold@gmail.com> [2014 - 2017]
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_GLOBAL_CONSTANTS_H
#define QLEVER_SRC_GLOBAL_CONSTANTS_H

#include <chrono>
#include <stdexcept>
#include <string>
#include <string_view>

#include "util/MemorySize/MemorySize.h"
#include "util/StringUtils.h"

// For access to `memorySize` literals.
using namespace ad_utility::memory_literals;

constexpr inline ad_utility::MemorySize DEFAULT_MEMORY_LIMIT_INDEX_BUILDING =
    5_GB;
constexpr inline ad_utility::MemorySize DEFAULT_PARSER_BUFFER_SIZE = 10_MB;

constexpr inline ad_utility::MemorySize DEFAULT_MEM_FOR_QUERIES = 4_GB;

constexpr inline uint64_t MAX_NOF_ROWS_IN_RESULT = 1'000'000;
constexpr inline size_t MIN_WORD_PREFIX_SIZE = 4;
constexpr inline char PREFIX_CHAR = '*';

constexpr inline size_t BUFFER_SIZE_DOCSFILE_LINE = 100'000'000;

constexpr inline size_t TEXT_PREDICATE_CARDINALITY_ESTIMATE = 1'000'000'000;

constexpr inline size_t GALLOP_THRESHOLD = 1000;

constexpr inline char QLEVER_INTERNAL_PREFIX_NAME[] = "ql";
constexpr inline std::string_view QLEVER_INTERNAL_PREFIX_URL =
    "http://qlever.cs.uni-freiburg.de/builtin-functions/";

// Make a QLever-internal IRI from `QL_INTERNAL_PREFIX_URL` by appending the
// concatenation of the given `suffixes` and enclosing the result in angle
// brackets (const and non-const version).
namespace string_constants::detail {
constexpr inline std::string_view openAngle = "<";
constexpr inline std::string_view closeAngle = ">";
}  // namespace string_constants::detail
template <const std::string_view&... suffixes>
constexpr std::string_view makeQleverInternalIriConst() {
  using namespace string_constants::detail;
  return ad_utility::constexprStrCat<openAngle, QLEVER_INTERNAL_PREFIX_URL,
                                     suffixes..., closeAngle>();
}
template <typename... T>
inline std::string makeQleverInternalIri(const T&... suffixes) {
  return absl::StrCat("<", QLEVER_INTERNAL_PREFIX_URL, suffixes..., ">");
}

namespace string_constants::detail {
constexpr inline std::string_view empty = "";
}  // namespace string_constants::detail
constexpr inline std::string_view QLEVER_INTERNAL_PREFIX_IRI =
    makeQleverInternalIriConst<string_constants::detail::empty>();
constexpr inline std::string_view
    QLEVER_INTERNAL_PREFIX_IRI_WITHOUT_CLOSING_BRACKET =
        ad_utility::constexprStrCat<string_constants::detail::openAngle,
                                    QLEVER_INTERNAL_PREFIX_URL>();
namespace string_constants::detail {
constexpr inline std::string_view contains_entity = "contains-entity";
}  // namespace string_constants::detail
constexpr inline std::string_view CONTAINS_ENTITY_PREDICATE =
    makeQleverInternalIriConst<string_constants::detail::contains_entity>();
namespace string_constants::detail {
constexpr inline std::string_view contains_word = "contains-word";
}  // namespace string_constants::detail
constexpr inline std::string_view CONTAINS_WORD_PREDICATE =
    makeQleverInternalIriConst<string_constants::detail::contains_word>();

namespace string_constants::detail {
constexpr inline std::string_view text = "text";
}  // namespace string_constants::detail
constexpr inline std::string_view QLEVER_INTERNAL_TEXT_MATCH_PREDICATE =
    makeQleverInternalIriConst<string_constants::detail::text>();
namespace string_constants::detail {
constexpr inline std::string_view has_predicate = "has-predicate";
}  // namespace string_constants::detail
constexpr inline std::string_view HAS_PREDICATE_PREDICATE =
    makeQleverInternalIriConst<string_constants::detail::has_predicate>();
namespace string_constants::detail {
constexpr inline std::string_view has_pattern = "has-pattern";
}  // namespace string_constants::detail
constexpr inline std::string_view HAS_PATTERN_PREDICATE =
    makeQleverInternalIriConst<string_constants::detail::has_pattern>();
namespace string_constants::detail {
constexpr inline std::string_view default_graph = "default-graph";
}  // namespace string_constants::detail
constexpr inline std::string_view DEFAULT_GRAPH_IRI =
    makeQleverInternalIriConst<string_constants::detail::default_graph>();
namespace string_constants::detail {
constexpr inline std::string_view internal_graph = "internal-graph";
}  // namespace string_constants::detail
constexpr inline std::string_view QLEVER_INTERNAL_GRAPH_IRI =
    makeQleverInternalIriConst<string_constants::detail::internal_graph>();
namespace string_constants::detail {
constexpr inline std::string_view blank_node_prefix = "blank-node/";
}  // namespace string_constants::detail
constexpr inline std::string_view QLEVER_INTERNAL_BLANK_NODE_IRI_PREFIX =
    ad_utility::constexprStrCat<string_constants::detail::openAngle,
                                QLEVER_INTERNAL_PREFIX_URL,
                                string_constants::detail::blank_node_prefix>();

// The prefix of the new graph IRIs that are generated when a Graph Store
// Protocol PUT is made without specifying a graph.
namespace string_constants::detail {
constexpr inline std::string_view new_graph_prefix = "graph/";
}  // namespace string_constants::detail
constexpr inline std::string_view QLEVER_NEW_GRAPH_PREFIX =
    ad_utility::constexprStrCat<QLEVER_INTERNAL_PREFIX_URL,
                                string_constants::detail::new_graph_prefix>();

// The prefix of the SERVICE IRI used for a cached result with a name. Use as
// in `SERVICE <ql:cached-result-with-name-$query-name$> {}`.
namespace string_constants::detail {
constexpr inline std::string_view cachedResultWithNamePrefix =
    "cached-result-with-name-";
}  // namespace string_constants::detail
constexpr inline std::string_view CACHED_RESULT_WITH_NAME_PREFIX =
    ad_utility::constexprStrCat<
        QLEVER_INTERNAL_PREFIX_URL,
        string_constants::detail::cachedResultWithNamePrefix>();

constexpr inline std::pair<std::string_view, std::string_view> GEOF_PREFIX = {
    "geof:", "http://www.opengis.net/def/function/geosparql/"};
// The embedding expression functions (e.g. `embf:distance`), dispatched by
// prefix match just like `geof:`.
constexpr inline std::pair<std::string_view, std::string_view> EMBF_PREFIX = {
    "embf:", "http://qlever.cs.uni-freiburg.de/embeddings/functions/"};
constexpr inline std::pair<std::string_view, std::string_view> MATH_PREFIX = {
    "math:", "http://www.w3.org/2005/xpath-functions/math#"};
constexpr inline std::pair<std::string_view, std::string_view> XSD_PREFIX = {
    "xsd", "http://www.w3.org/2001/XMLSchema#"};
constexpr inline std::pair<std::string_view, std::string_view> QL_PREFIX = {
    QLEVER_INTERNAL_PREFIX_NAME, QLEVER_INTERNAL_PREFIX_URL};

constexpr inline std::string_view QLEVER_INTERNAL_VARIABLE_PREFIX =
    "?_QLever_internal_variable_";

constexpr inline std::string_view QLEVER_INTERNAL_BLANKNODE_VARIABLE_PREFIX =
    "?_QLever_internal_variable_bn_";

constexpr inline std::string_view
    QLEVER_INTERNAL_VARIABLE_QUERY_PLANNER_PREFIX =
        "?_QLever_internal_variable_qp_";

constexpr inline std::string_view SCORE_VARIABLE_PREFIX = "?ql_score_";
constexpr inline std::string_view MATCHINGWORD_VARIABLE_PREFIX =
    "?ql_matchingword_";

namespace constants::details::strings {
constexpr inline std::string_view langtag{"langtag"};
constexpr inline std::string_view hasWord{"has-word"};
}  // namespace constants::details::strings
constexpr inline std::string_view LANGUAGE_PREDICATE =
    makeQleverInternalIriConst<constants::details::strings::langtag>();
constexpr inline std::string_view HAS_WORD_PREDICATE =
    makeQleverInternalIriConst<constants::details::strings::hasWord>();

// TODO<joka921> Move them to their own file, make them strings, remove
// duplications, etc.
constexpr inline char XSD_STRING[] = "http://www.w3.org/2001/XMLSchema#string";
constexpr inline char XSD_DATETIME_TYPE[] =
    "http://www.w3.org/2001/XMLSchema#dateTime";
constexpr inline char XSD_DATE_TYPE[] = "http://www.w3.org/2001/XMLSchema#date";
constexpr inline char XSD_GYEAR_TYPE[] =
    "http://www.w3.org/2001/XMLSchema#gYear";
constexpr inline char XSD_GYEARMONTH_TYPE[] =
    "http://www.w3.org/2001/XMLSchema#gYearMonth";
constexpr inline char XSD_DAYTIME_DURATION_TYPE[] =
    "http://www.w3.org/2001/XMLSchema#dayTimeDuration";

constexpr inline char XSD_INT_TYPE[] = "http://www.w3.org/2001/XMLSchema#int";
constexpr inline char XSD_INTEGER_TYPE[] =
    "http://www.w3.org/2001/XMLSchema#integer";
constexpr inline char XSD_FLOAT_TYPE[] =
    "http://www.w3.org/2001/XMLSchema#float";
constexpr inline char XSD_DOUBLE_TYPE[] =
    "http://www.w3.org/2001/XMLSchema#double";
constexpr inline char XSD_DECIMAL_TYPE[] =
    "http://www.w3.org/2001/XMLSchema#decimal";

constexpr inline char XSD_NON_POSITIVE_INTEGER_TYPE[] =
    "http://www.w3.org/2001/XMLSchema#nonPositiveInteger";
constexpr inline char XSD_NEGATIVE_INTEGER_TYPE[] =
    "http://www.w3.org/2001/XMLSchema#negativeInteger";
constexpr inline char XSD_LONG_TYPE[] = "http://www.w3.org/2001/XMLSchema#long";
constexpr inline char XSD_SHORT_TYPE[] =
    "http://www.w3.org/2001/XMLSchema#short";
constexpr inline char XSD_BYTE_TYPE[] = "http://www.w3.org/2001/XMLSchema#byte";
constexpr inline char XSD_NON_NEGATIVE_INTEGER_TYPE[] =
    "http://www.w3.org/2001/XMLSchema#nonNegativeInteger";
constexpr inline char XSD_UNSIGNED_LONG_TYPE[] =
    "http://www.w3.org/2001/XMLSchema#unsignedLong";
constexpr inline char XSD_UNSIGNED_INT_TYPE[] =
    "http://www.w3.org/2001/XMLSchema#unsignedInt";
constexpr inline char XSD_UNSIGNED_SHORT_TYPE[] =
    "http://www.w3.org/2001/XMLSchema#unsignedShort";
constexpr inline char XSD_POSITIVE_INTEGER_TYPE[] =
    "http://www.w3.org/2001/XMLSchema#positiveInteger";
constexpr inline char XSD_BOOLEAN_TYPE[] =
    "http://www.w3.org/2001/XMLSchema#boolean";
constexpr inline char XSD_ANYURI_TYPE[] =
    "http://www.w3.org/2001/XMLSchema#anyURI";
constexpr inline char RDF_PREFIX[] =
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
constexpr inline char RDF_LANGTAG_STRING[] =
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString";

constexpr inline std::string_view GEO_WKT_LITERAL =
    "http://www.opengis.net/ont/geosparql#wktLiteral";
namespace string_constants::detail {
constexpr inline std::string_view geo_literal_prefix = "\"^^<";
}  // namespace string_constants::detail
static constexpr std::string_view GEO_LITERAL_SUFFIX =
    ad_utility::constexprStrCat<string_constants::detail::geo_literal_prefix,
                                GEO_WKT_LITERAL,
                                string_constants::detail::closeAngle>();

constexpr std::string_view SF_PREFIX = "http://www.opengis.net/ont/sf#";

// QLever-owned namespace for embedding-related RDF terms (the vector datatype
// and the embedding-type metadata vocabulary, all flat under prefix `emb:`).
// Deliberately **not** under `builtin-functions/`: IRIs in that namespace are
// treated as QLever-internal, and any triple touching one is dropped during
// index building (see `isQleverInternalTriple` in `IndexImpl.cpp`). That would
// make the embedding-type declarations invisible to the load-time
// `EmbeddingTypeRegistry` scan. Terms here are ordinary RDF instead: they
// survive index building and are visible to queries, like any other input
// triple.
//
// This mirrors GeoSPARQL, which keeps its datatype (`geo:wktLiteral`) and
// properties (`geo:asWKT`) flat in one namespace and separates only its
// *functions* (`geof:`). Likewise the query-time embedding expression function
// lives in a sibling `functions/` namespace (`embf:distance`, see
// `EMBF_PREFIX`).
constexpr inline std::string_view QLEVER_EMBEDDINGS_PREFIX_URL =
    "http://qlever.cs.uni-freiburg.de/embeddings/";
// Make a full `<...>` IRI in the embeddings namespace from the given suffixes
// (mirrors `makeQleverInternalIriConst`).
template <const std::string_view&... suffixes>
constexpr std::string_view makeEmbeddingIriConst() {
  using namespace string_constants::detail;
  return ad_utility::constexprStrCat<openAngle, QLEVER_EMBEDDINGS_PREFIX_URL,
                                     suffixes..., closeAngle>();
}

// The datatype IRI for native embedding vectors (`emb:fp32Vector`, IEEE-754
// binary32). lowerCamelCase per the XSD/GeoSPARQL datatype convention (cf.
// `geo:wktLiteral`). The MVP supports only `fp32Vector`; further precisions
// (`emb:fp16Vector`, ...) are future additions.
namespace string_constants::detail {
constexpr inline std::string_view embeddings_fp32_vector = "fp32Vector";
}  // namespace string_constants::detail
// The bare datatype IRI (no angle brackets), as returned by
// `Literal::getDatatype()`; used to recognize a vector literal by datatype.
constexpr inline std::string_view EMBEDDING_FP32_DATATYPE =
    ad_utility::constexprStrCat<
        QLEVER_EMBEDDINGS_PREFIX_URL,
        string_constants::detail::embeddings_fp32_vector>();

// The embedding-type metadata vocabulary. An embedding node links to its type
// via `emb:type`; the type IRI carries the metadata properties `emb:hasMetric`,
// `emb:hasDimension` and `emb:hasPrecision`. The `EmbeddingTypeRegistry` scans
// these at index load.
namespace string_constants::detail {
constexpr inline std::string_view embeddings_type = "type";
constexpr inline std::string_view embeddings_has_metric = "hasMetric";
constexpr inline std::string_view embeddings_has_dimension = "hasDimension";
constexpr inline std::string_view embeddings_has_precision = "hasPrecision";
}  // namespace string_constants::detail
// The predicate that links an embedding node to its embedding-type IRI.
constexpr inline std::string_view EMBEDDING_TYPE_IRI =
    makeEmbeddingIriConst<string_constants::detail::embeddings_type>();
// The mandatory per-type metadata predicates.
constexpr inline std::string_view EMBEDDING_HAS_METRIC_IRI =
    makeEmbeddingIriConst<string_constants::detail::embeddings_has_metric>();
constexpr inline std::string_view EMBEDDING_HAS_DIMENSION_IRI =
    makeEmbeddingIriConst<string_constants::detail::embeddings_has_dimension>();
constexpr inline std::string_view EMBEDDING_HAS_PRECISION_IRI =
    makeEmbeddingIriConst<string_constants::detail::embeddings_has_precision>();
// The element precision of an embedding type, as a predefined IRI in the
// embeddings namespace (the object of `emb:hasPrecision`, e.g. `emb:fp32`).
// The MVP supports only `emb:fp32`, which must match the `emb:fp32Vector`
// datatype of the type's vectors.
namespace string_constants::detail {
constexpr inline std::string_view embeddings_fp32 = "fp32";
}  // namespace string_constants::detail
constexpr inline std::string_view EMBEDDING_PRECISION_FP32_IRI =
    makeEmbeddingIriConst<string_constants::detail::embeddings_fp32>();

// The supported `emb:hasMetric` values for the MVP (strict), as predefined IRIs
// in the embeddings namespace (e.g. `emb:cosine`). All four are computed as an
// exact *distance* (smaller = closer).
namespace string_constants::detail {
constexpr inline std::string_view embeddings_cosine = "cosine";
constexpr inline std::string_view embeddings_l2 = "l2";
constexpr inline std::string_view embeddings_squared_l2 = "squaredL2";
constexpr inline std::string_view embeddings_dot_product = "dotProduct";
}  // namespace string_constants::detail
constexpr inline std::string_view EMBEDDING_METRIC_COSINE_IRI =
    makeEmbeddingIriConst<string_constants::detail::embeddings_cosine>();
constexpr inline std::string_view EMBEDDING_METRIC_L2_IRI =
    makeEmbeddingIriConst<string_constants::detail::embeddings_l2>();
constexpr inline std::string_view EMBEDDING_METRIC_SQUARED_L2_IRI =
    makeEmbeddingIriConst<string_constants::detail::embeddings_squared_l2>();
constexpr inline std::string_view EMBEDDING_METRIC_DOT_PRODUCT_IRI =
    makeEmbeddingIriConst<string_constants::detail::embeddings_dot_product>();

constexpr inline std::string_view VOCAB_SUFFIX = ".vocabulary";
constexpr inline std::string_view MMAP_FILE_SUFFIX = ".meta";
constexpr inline std::string_view CONFIGURATION_FILE = ".meta-data.json";

constexpr inline std::string_view ERROR_IGNORE_CASE_UNSUPPORTED =
    "Key \"ignore-case\" is no longer supported. Please remove this key from "
    "your settings.json and rebuild your index. You can optionally specify the "
    "\"locale\" key, otherwise \"en.US\" will be used as default";
constexpr inline std::string_view WARNING_ASCII_ONLY_PREFIXES =
    "You specified \"ascii-prefixes-only = true\", which enables faster "
    "parsing for well-behaved TTL files";
// " but only works correctly if there are no escape sequences in "
// "prefixed names (e.g., rdfs:label\\,el is not allowed), no multiline "
// "literals, and the regex \". *\\n\" only matches at the end of a triple. "
// "Most Turtle files fulfill these properties (e.g. that from Wikidata), "
// "but not all";

constexpr inline std::string_view WARNING_PARALLEL_PARSING =
    "You specified \"parallel-parsing = true\", which enables faster parsing "
    "for TTL files with a well-behaved use of newlines";
constexpr inline std::string_view LOCALE_DEFAULT_LANG = "en";
constexpr inline std::string_view LOCALE_DEFAULT_COUNTRY = "US";
constexpr inline bool LOCALE_DEFAULT_IGNORE_PUNCTUATION = false;

// Constants for the range of valid compression prefixes
// all ASCII- printable characters are left out.
// when adding more special characters to the vocabulary make sure to leave out
// \n since the vocabulary is stored in a text file line by line.
// All prefix codes have a most significant bit of 1. This means the prefix
// codes are never valid UTF-8 and thus it is always able to determine, whether
// this vocabulary was compressed or not.
constexpr inline uint8_t MIN_COMPRESSION_PREFIX = 129;
constexpr inline uint8_t NUM_COMPRESSION_PREFIXES = 126;
// if this is the first character of a compressed string, this means that no
// compression has been applied to  a word
constexpr inline uint8_t NO_PREFIX_CHAR =
    MIN_COMPRESSION_PREFIX + NUM_COMPRESSION_PREFIXES;

// When initializing a sort performance estimator, at most this percentage of
// the number of triples in the index is being sorted at once.
constexpr inline size_t PERCENTAGE_OF_TRIPLES_FOR_SORT_ESTIMATE = 5;

// When asked to make room for X ids in the cache, actually make room for X
// times this factor.
constexpr inline size_t MAKE_ROOM_SLACK_FACTOR = 2;

// The maximal number of columns an `IdTable` (an intermediate result of
// query evaluation) may have to be able to use the more efficient `static`
// implementation (For details see `IdTable.h`, `CallFixedSize.h` and the
// usage of `CALL_FIXED_SIZE` in the various subclasses of `Operation`.
// Increasing this number improves the runtime for operations on tables with
// more columns, but also increases compile times because more templates
// have to be instantiated. It might also be necessary to increase some internal
// compiler limits for the evaluation of constexpr functions and templates.
// Under QLEVER_CHEAPER_COMPILATION the value is lowered to reduce the number
// of template instantiations and thereby speed up debug builds.
#ifdef QLEVER_CHEAPER_COMPILATION
constexpr inline int DEFAULT_MAX_NUM_COLUMNS_STATIC_ID_TABLE = 1;
#else
constexpr inline int DEFAULT_MAX_NUM_COLUMNS_STATIC_ID_TABLE = 5;
#endif

// Interval in which an enabled watchdog would check if
// `CancellationHandle::throwIfCancelled` is called regularly.
constexpr inline std::chrono::milliseconds DESIRED_CANCELLATION_CHECK_INTERVAL{
    50};

// In all permutations, the graph ID of the triple is stored as the fourth
// entry. During the index building it is important that this is the first
// column after the "actual" triple.
constexpr inline size_t ADDITIONAL_COLUMN_GRAPH_ID = 3;

// In the PSO and PSO permutations the patterns of the subject and object are
// stored at the following indices. Note that the col0 (the P) is not part of
// the result, so the column order for PSO is S O PatternS PatternO.
constexpr inline size_t ADDITIONAL_COLUMN_INDEX_SUBJECT_PATTERN = 4;
constexpr inline size_t ADDITIONAL_COLUMN_INDEX_OBJECT_PATTERN = 5;

#ifdef _PARALLEL_SORT
constexpr inline bool USE_PARALLEL_SORT = true;
#include <parallel/algorithm>
namespace ad_utility {
template <typename... Args>
auto parallel_sort(Args&&... args) {
  return __gnu_parallel::sort(std::forward<Args>(args)...);
}
using parallel_tag = __gnu_parallel::parallel_tag;

}  // namespace ad_utility

#else
constexpr inline bool USE_PARALLEL_SORT = false;
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
constexpr inline size_t NUM_SORT_THREADS = 4;
/// ANSI escape sequence for bold text in the console
constexpr inline std::string_view EMPH_ON = "\033[1m";
/// ANSI escape sequence to print "normal" text again in the console.
constexpr inline std::string_view EMPH_OFF = "\033[22m";

// Allowed range for geographical coordinates from WTK Text
constexpr inline double COORDINATE_LAT_MAX = 90.0;
constexpr inline double COORDINATE_LNG_MAX = 180.0;

// When operation results are returned as `application/qlever-results+json` the
// Operation string is echoed. This operation string is truncated to ensure
// performance.
constexpr inline size_t MAX_LENGTH_OPERATION_ECHO = 5000;

constexpr inline std::string_view GSP_DIRECT_GRAPH_IDENTIFICATION_PREFIX =
    "http-graph-store";

#endif  // QLEVER_SRC_GLOBAL_CONSTANTS_H
