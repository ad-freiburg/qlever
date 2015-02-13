// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold <buchholb>

#ifndef  GLOBALS_GLOBALS_H_
#define  GLOBALS_GLOBALS_H_

#include <stdint.h>
#include <string>

using std::string;

namespace ad_semsearch {
// typedef uint64_t Id;
typedef uint32_t Id;
typedef uint8_t Score;
typedef uint32_t AggregatedScore;
typedef uint16_t Position;
typedef size_t ListSize;
typedef uint16_t RelationPattern;
typedef uint16_t ClassPattern;

static const Id ONTOLOGY_CONTEXT_ID = ~Id(0);
static const char WIKIPEDIA_URL[] = "http://en.wikipedia.org/wiki/";
static const char ONTO_URL[] = "http://todo-url.org";
static const char XSD_VALUE_NS[] = "http://www.w3.org/2001/XMLSchema";

// Scores
// static const AggregatedScore ENTITY_EQUALS_SCORE = 1;
static const Score DEFAULT_RELATION_SCORE = 1;
static const int ABSTRACT_ENTITY_THRESHOLD = 10;

static const bool IS_A_EXPANDED = false;
static const char PREFIX_CHAR = '*';
static const char QUERY_VARIABLE_START = '$';
static const char FULLTEXT_QUERY_NO_PREFIX_CHAR = 255;
static const char WORD_SEPARATOR_OR = '|';

static const char ENTITY_PREFIX[] = ":e:";
static const char RELATION_PREFIX[] = ":r:";
// NOTE: Documents have to be lexicographically smaller than
// entities, relations, and types.
static const char DOCUMENT_PREFIX[] = ":d:";
static const char SPECIAL_WORD_FIRSTSENTENCE[] = ":s:firstsentence";
static const char REVERSED_RELATION_SUFFIX[] = "_(reversed)";

static const char TYPE_PREFIX[] = ":t:";
static const char VALUE_PREFIX[] = ":v:";
static const char DOCSFILE_POS_DELIMITER[] = "@@";
static const char HIGHLIGHT_START[] = "<hl>";
static const char HIGHLIGHT_END[] = "</hl>";

static const char IS_A_RELATION_FROM_YAGO[] = "is-a.entity.class";
static const char IS_A_RELATION[] = "is-a";
static const char IN_RANGE_RELATION[] = "in-range";
static const char EQUALS_RELATION[] = "equals";
static const char OCCURS_WITH_RELATION[] = "occurs-with";
static const char HAS_OCCURRENCE_OF_RELATION[] = "has-occurrence-of";
static const char OCCURS_IN_RELATION[] = "occurs-in";
static const char HAS_RELATIONS_RELATION[] = "has-relations";
static const char HAS_RELATION_PATTERN[] = "has-relation-pattern";
static const char HAS_CLASS_PATTERN[] = "has-class-pattern";
static const char RELATION_PATTERNS[] = "relation-patterns";
static const char CLASS_PATTERNS[] = "class-patterns";
static const char EID_TO_CID[] = "eid-to-cid";
static const char CLASS_HAS_RELATIONS_RELATION[] = "class-has-relations";
static const char HAS_INSTANCES_RELATION[] = "has-instances";

static const size_t NOF_OW_HITS_NO_FIXED_ENTITY = 2;
static const size_t NOF_OW_HITS_FIXED_ENTITY = 15;
static const size_t MIN_WORD_PREFIX_SIZE = 4;
static const size_t FULLTEXT_ONLY_HITS_PER_GROUP = 3;

// Default version cache sizes
static const size_t NOF_SUBTREES_TO_CACHE = 50;
static const size_t NOF_FT_QUERIES_TO_CACHE = 20;
static const size_t NOF_PSEUDO_PREFIXES_TO_CACHE = 50;
static const size_t NOF_FILTERED_POSTINGLISTS_TO_CACHE = 50;

// Old cache sizes (somewhat overboard)
// static const size_t NOF_SUBTREES_TO_CACHE = 300;
// static const size_t NOF_FT_QUERIES_TO_CACHE = 200;
// static const size_t NOF_PSEUDO_PREFIXES_TO_CACHE = 200;
// static const size_t NOF_FILTERED_POSTINGLISTS_TO_CACHE = 100;

// Small memory version.
// static const size_t NOF_SUBTREES_TO_CACHE = 20;
// static const size_t NOF_FT_QUERIES_TO_CACHE = 10;
// static const size_t NOF_PSEUDO_PREFIXES_TO_CACHE = 10;
// static const size_t NOF_FILTERED_POSTINGLISTS_TO_CACHE = 20;

static const int BUFFER_SIZE_WORDNET_CATEGORIES = 512;
static const int BUFFER_SIZE_ONTOLOGY_WORD = 1024 * 1024;  // 4092;
static const int BUFFER_SIZE_WORD = 1024 * 1024;  // 4092;
static const int BUFFER_SIZE_WORDSFILE_LINE = 1024 * 1024;  // 4092;
static const int BUFFER_SIZE_ONTOLOGY_LINE = 1024 * 1024;  // 4092;
static const int BUFFER_SIZE_TXT_LINE = 1024 * 1024;
static const int BUFFER_SIZE_OFFSETS_FILE = 128;
static const int BUFFER_SIZE_DOCSFILE_LINE = 1024 * 1024 * 100;

static const int STXXL_MEMORY_TO_USE = 1024 * 1024 * 1024;
static const int STXXL_DISK_SIZE_INDEX_BUILDER = 500000;
static const int STXXL_DISK_SIZE_INDEX_BUILDER_TESTS = 10;

static const char INDEX_FILE_EXTENSION[] = ".index";
static const char VOCABULARY_FILE_EXTENSION[] = ".vocabulary";
static const char ENTITY_SCORES_FILE_EXTENSION[] = ".entity-scores";
static const char PSEUDO_PREFIXES_FILE_EXTENSION[] = ".pseudo-prefixes";
static const char EXCERPTS_FILE_EXTENSION[] = ".docs-by-contexts.txt";
static const char WORDS_FILE_EXTENSION[] = ".words-by-contexts.txt";
static const char REVERSE_RELATIONS_FILE_EXTENSION[] = ".reverse-relations";

static const char VALUE_DATE_PREFIX[] = ":v:date:";
static const char VALUE_INTEGER_PREFIX[] = ":v:integer:";
static const char VALUE_FLOAT_PREFIX[] = ":v:float:";
static const char VALUE_DATE_TIME_SEPARATOR[] = "T";
static const int DEFAULT_NOF_VALUE_INTEGER_DIGITS = 50;
static const int DEFAULT_NOF_VALUE_EXPONENT_DIGITS = 20;
static const int DEFAULT_NOF_VALUE_MANTISSA_DIGITS = 30;
static const int DEFAULT_NOF_DATE_YEAR_DIGITS = 19;
}

namespace ad_utility {
static const char EMPH_ON[]  =  "\033[1m";
static const char EMPH_OFF[] =  "\033[0m";
}

#endif  // GLOBALS_GLOBALS_H_
