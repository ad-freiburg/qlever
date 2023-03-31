// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel February of 2023, schlegea@informatik.uni-freiburg.de)

# pragma once

#include <sstream>
#include <algorithm>
#include <vector>

#include "util/json.h"
#include "../benchmark/infrastructure/Benchmark.h"
#include "../benchmark/infrastructure/BenchmarkMeasurementContainer.h"
#include "../benchmark/infrastructure/BenchmarkRecordToString.h"

/*
 * @brief Add a string of the form
 * "
 *  #################
 *  # categoryTitel #
 *  #################
 *
 *  "
 *  to the stream.
 */
void addCategoryTitelToOStringstream(std::ostringstream* stream,
    const std::string categoryTitel);

// Default way of adding a vector of RecordEntrys to a `ostringstream` with
// optional prefix.
void addVectorOfRecordEntryToOStringstream(std::ostringstream* stream,
    const std::vector<BenchmarkMeasurementContainer::RecordEntry>& entries,
    const std::string& prefix = "");

// Visualization for single measurments.
void addSingleMeasurementsToOStringstream(std::ostringstream* stream,
    const std::vector<BenchmarkMeasurementContainer::RecordEntry>&
    recordEntries);

// Visualization for groups.
void addGroupsToOStringstream(std::ostringstream* stream,
    const std::vector<BenchmarkMeasurementContainer::RecordGroup>&
    recordGroups);

// Visualization for tables.
void addTablesToOStringstream(std::ostringstream* stream,
    const std::vector<BenchmarkMeasurementContainer::RecordTable>&
    recordTables);

/*
 * @brief Returns a formated string containing all benchmark information.
 */
std::string benchmarkRecordsToString(const BenchmarkRecords& records);
