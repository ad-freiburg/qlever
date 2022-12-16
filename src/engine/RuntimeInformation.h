// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2019      Florian Kramer (florian.kramer@neptun.uni-freiburg.de)
//   2022-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#pragma once

#include <iostream>
#include <string>
#include <vector>

#include "absl/strings/str_join.h"
#include "engine/VariableToColumnMap.h"
#include "parser/data/Variable.h"
#include "util/ConcurrentCache.h"
#include "util/HashMap.h"
#include "util/json.h"

/// A class to store information about the status of an operation (result size,
/// time to compute, status, etc.). Also contains the functionality to print
/// that information nicely formatted and to export it to JSON.
class RuntimeInformation {
 public:
  /// The computation status of an operation.
  enum struct Status {
    notStarted,
    completed,
    completedDuringQueryPlanning,
    optimizedOut,
    failed,
    failedBecauseChildFailed
  };
  using enum Status;

  // Public members

  /// The total time spent computing this operation. This includes the
  /// computation of the children.
  double totalTime_ = 0.0;

  /// In case this operation was read from the cache, we will store the time
  /// information about the original computation in the following two members.
  double originalTotalTime_ = 0.0;
  double originalOperationTime_ = 0.0;

  /// The estimated cost, size, and column multiplicities of the operation.
  size_t costEstimate_ = 0;
  size_t sizeEstimate_ = 0;
  std::vector<float> multiplicityEstimates_;

  /// The actual number of rows and columns in the result.
  size_t numRows_ = 0;
  size_t numCols_ = 0;

  /// The status of the operation.
  Status status_ = Status::notStarted;

  /// Was the result of the operation read from the cache or actually computed.
  ad_utility::CacheStatus cacheStatus_ = ad_utility::CacheStatus::computed;

  /// A short human-readable string that identifies the operation.
  std::string descriptor_;

  /// The names of the variables that are stored in the columsn of the result.
  std::vector<std::string> columnNames_;

  /// The child operations of this operation.
  std::vector<RuntimeInformation> children_;

  /// A key-value map of various other information that might be different for
  /// different types of operations.
  nlohmann::json details_;

  // Default constructor.
  RuntimeInformation() = default;

  /// Formatted output to  std::string and streams.
  [[nodiscard]] std::string toString() const;
  void writeToStream(std::ostream& out, size_t indent = 1) const;

  /// Output as json. The signature of this function is mandated by the json
  /// library to allow for implicit conversion.
  friend void to_json(nlohmann::ordered_json& j, const RuntimeInformation& rti);

  /// Set the names of the columns from the HashMap format that is used in the
  /// rest of the Qlever code.
  void setColumnNames(const VariableToColumnMap& columnMap);

  /// Get the time spent computing the operation. This is the total time minus
  /// the time spent computing the children.
  [[nodiscard]] double getOperationTime() const;

  /// Get the total time including the time that this operation spent during
  /// the query planning phase to compute itself or any of its children.
  /// Note: This function has to traverse the whole tree recursively. Therefore,
  ///       it should only be called once on a complete tree when exporting it.
  [[nodiscard]] double getTotalTimeCorrected() const;

  /// Get the cost estimate for this operation. This is the total cost estimate
  /// minus the sum of the cost estimates of all children.
  [[nodiscard]] size_t getOperationCostEstimate() const;

  /// Add a key-value pair to the `details` section of the output. `value` may
  /// be any type that can be implicitly converted to `nlohmann::json`.
  template <typename T>
  void addDetail(const std::string& key, const T& value) {
    details_[key] = value;
  }

 private:
  static std::string_view toString(Status status);
};

// A class to store information about the execution of a complete query, e.g.
// the time spent during query planning. Note: The information about the
// `QueryExecutionTree` (e.g. how much time was spent in which operation) is
// stored in the `RuntimeInformation` class above.
struct RuntimeInformationWholeQuery {
  // The time spent during query planning (this does not include the time spent
  // on `IndexScan`s that were executed during the query planning).
  size_t timeQueryPlanning = 0;
  size_t timeIndexScansQueryPlanning = 0;
  /// Output as json. The signature of this function is mandated by the json
  /// library to allow for implicit conversion.
  friend void to_json(nlohmann::ordered_json& j,
                      const RuntimeInformationWholeQuery& rti);
};
