// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel February of 2023, schlegea@informatik.uni-freiburg.de)

# pragma once

#include <sstream>
#include <algorithm>
#include <vector>

#include "util/json.h"
#include "../benchmark/Benchmark.h"
#include "../benchmark/BenchmarkRecordToString.h"

/*
 * @brief Add a string of the form
 * "
 *  #################
 *  # categoryTitel #
 *  #################
 *
 *  "
 *  to the stringStream.
 */
void addCategoryTitelToStringstream(std::stringstream* stringStream,
    const std::string categoryTitel);

// Default conversion from BenchmarkRecords::RecordEntry to string.
std::string recordEntryToString(const BenchmarkRecords::RecordEntry& recordEntry);

// Default way of adding a vector of RecordEntrys to a stringstream with
// optional prefix.
void addVectorOfRecordEntryToStringstream(std::stringstream* stringStream,
    const std::vector<BenchmarkRecords::RecordEntry>& entries,
    const std::string& prefix = "");

// Visualization for single measurments.
void addSingleMeasurementsToStringstream(std::stringstream* stringStream,
    const std::vector<BenchmarkRecords::RecordEntry>& recordEntries);

// Visualization for groups.
void addGroupsToStringstream(std::stringstream* stringStream,
    const std::vector<BenchmarkRecords::RecordGroup>& recordGroups);

// Visualization for tables.
void addTablesToStringstream(std::stringstream* stringStream,
    const std::vector<BenchmarkRecords::RecordTable>& recordTables);

/*
 * @brief Returns a formated string containing all benchmark information.
 */
std::string benchmarkRecordsToString(const BenchmarkRecords& records);
