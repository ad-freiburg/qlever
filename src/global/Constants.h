// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

static const int STXXL_MEMORY_TO_USE = 1024 * 1024 * 1024;
static const int STXXL_DISK_SIZE_INDEX_BUILDER = 300000;
static const int STXXL_DISK_SIZE_INDEX_TEST = 10;

static const size_t NOF_SUBTREES_TO_CACHE = 50;
static const size_t MAX_NOF_ROWS_IN_RESULT = 1000000;
static const size_t MIN_WORD_PREFIX_SIZE = 4;
static const char PREFIX_CHAR = '*';

static const size_t BUFFER_SIZE_RELATION_SIZE = 1000 * 1000 * 1000;
static const size_t BUFFER_SIZE_DOCSFILE_LINE = 1024 * 1024 * 100;
static const size_t DISTINCT_LHS_PER_BLOCK = 10 * 1000;

static const size_t IN_CONTEXT_CARDINALITY_ESTIMATE = 1000 * 1000 * 1000;

static const char IN_CONTEXT_RELATION[] = "<in-context>";
static const char HAS_CONTEXT_RELATION[] = "<contains-context>";
