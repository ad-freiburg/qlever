//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_GLOBAL_FILESUFFIXCONSTANTS_H
#define QLEVER_SRC_GLOBAL_FILESUFFIXCONSTANTS_H

#include <string_view>

// This file contains constants for prefixes and suffixes of filenames that are
// used by QLever to persistently store data on disk.

constexpr inline std::string_view VOCAB_SUFFIX = ".vocabulary";
constexpr inline std::string_view META_FILE_SUFFIX = ".meta";
constexpr inline std::string_view CONFIGURATION_FILE = ".meta-data.json";

// The file-name infix of the permutation files of an index, e.g. the `.index`
// in `<base>.index.pso`. Combined with the lower-cased permutation name (see
// `Permutation::fileNames`) and, for the metadata, with `META_FILE_SUFFIX`.
constexpr inline std::string_view PERMUTATION_FILE_INFIX = ".index";

// The patterns file of an index (used for `ql:has-predicate`).
constexpr inline std::string_view PATTERNS_FILE_SUFFIX = ".index.patterns";

// The copy of the settings file that is stored next to an index.
constexpr inline std::string_view SETTINGS_FILE_SUFFIX = ".settings.json";

// The files of the text index. They only exist if a text index was built.
constexpr inline std::string_view TEXT_INDEX_FILE_SUFFIX = ".text.index";
constexpr inline std::string_view TEXT_VOCAB_FILE_SUFFIX = ".text.vocabulary";
constexpr inline std::string_view TEXT_DOCS_DB_FILE_SUFFIX = ".text.docsDB";

// The file to which the updates (delta triples) of an index are persisted, and
// the file that stores the state of the graph-name allocation. They only exist
// if update persistence is enabled.
constexpr inline std::string_view UPDATE_TRIPLES_SUFFIX = ".update-triples";
constexpr inline std::string_view ALLOCATED_GRAPHS_SUFFIX =
    ".allocated-graphs-state";

// The files of an auxiliary index (see `index/AuxIndex.h`). They only exist if
// at least one auxiliary index has been built for the index. The base name of
// an auxiliary index is the base name of the main index, followed by
// `AUX_INDEX_INFIX` and the generation number, so the files of the second
// generation of the auxiliary index of `wikidata` are
// `wikidata.aux1.vocabulary`, `wikidata.aux1.index.pso`, etc.
constexpr inline std::string_view AUX_INDEX_INFIX = ".aux";
// The array that stores, for each word of the vocabulary of an auxiliary index,
// the position at which that word would be sorted into the vocabulary of the
// main index.
constexpr inline std::string_view AUX_VOCAB_POSITIONS_SUFFIX =
    ".vocabulary.positions";
// The metadata of an auxiliary index.
constexpr inline std::string_view AUX_INDEX_CONFIGURATION_FILE =
    ".aux-meta-data.json";

// The build log of an index. QLever does not write this directly, but
// `qlever-control` creates this file to persist QLever's `stdout` during the
// index building. If it is present, we still move it alongside the index that
// was built.
constexpr inline std::string_view INDEX_LOG_SUFFIX = ".index-log.txt";

// The build log of an index rebuild, it is created whenever a rebuild is
// triggered, in the same directory as the index.
constexpr inline std::string_view REBUILD_INDEX_LOG_SUFFIX =
    ".rebuild-index-log.txt";

#endif  // QLEVER_SRC_GLOBAL_FILESUFFIXCONSTANTS_H
