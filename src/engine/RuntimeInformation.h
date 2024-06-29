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
#include "parser/data/LimitOffsetClause.h"
#include "parser/data/Variable.h"
#include "util/ConcurrentCache.h"
#include "util/HashMap.h"
#include "util/json.h"

/// A class to store information about the status of an operation (result size,
/// time to compute, status, etc.). Also contains the functionality to print
/// that information nicely formatted and to export it to JSON.
class RuntimeInformation {
  using Milliseconds = std::chrono::milliseconds;

 public:
  // Ideally we'd use `using namespace std::chrono_literals;` here,
  // but C++ forbids using this within a class, and we don't want
  // to clutter the global namespace.
  static constexpr auto ZERO = Milliseconds::zero();

  /// The computation status of an operation.
  enum struct Status {
    notStarted,
    inProgress,
    fullyMaterialized,
    lazilyMaterialized,
    optimizedOut,
    failed,
    failedBecauseChildFailed,
    cancelled
  };
  using enum Status;

  // Public members

  /// The total time spent computing this operation. This includes the
  /// computation of the children.
  Milliseconds totalTime_ = ZERO;

  /// In case this operation was read from the cache, we will store the time
  /// information about the original computation in the following two members.
  Milliseconds originalTotalTime_ = ZERO;
  Milliseconds originalOperationTime_ = ZERO;

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

  /// The names of the variables that are stored in the columns of the result.
  std::vector<std::string> columnNames_;

  /// The child operations of this operation.
  std::vector<std::shared_ptr<RuntimeInformation>> children_;

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

  /// Set `columnNames_` from a `VariableToColumnMap`. The former is a vector
  /// (convenient for this class), the latter is a hash map (appropriate for
  /// the rest of the code).
  void setColumnNames(const VariableToColumnMap& columnMap);

  /// Get the time spent computing the operation. This is the total time minus
  /// the time spent computing the children, but always positive.
  [[nodiscard]] Milliseconds getOperationTime() const;

  /// Get the cost estimate for this operation. This is the total cost estimate
  /// minus the sum of the cost estimates of all children.
  [[nodiscard]] size_t getOperationCostEstimate() const;

  /// Add a key-value pair to the `details` section of the output. `value` may
  /// be any type that can be implicitly converted to `nlohmann::json`.
  template <typename T>
  void addDetail(const std::string& key, const T& value) {
    details_[key] = value;
  }

  /// Add a key-value pair to the `details` section of the output.
  /// Overload that allows to pass common type `std::chrono::milliseconds` for
  /// `value` without converting it first.
  void addDetail(const std::string& key, std::chrono::milliseconds value) {
    details_[key] = value.count();
  }

  // Erase the detail with the `key`, do nothing if no such detail exists.
  void eraseDetail(const std::string& key) {
    if (details_.contains(key)) {
      details_.erase(key);
    }
  }

  // Set the runtime information for a LIMIT or OFFSET operation as the new root
  // of the tree and make the old root the only child of the LIMIT operation.
  // The details of the LIMIT/OFFSET, the time (in ms) that was spent computing
  // it, and the information whether the `actual` operation (the old root of the
  // runtime information) is written to the cache, are passed in as arguments.
  void addLimitOffsetRow(const LimitOffsetClause& l, Milliseconds timeForLimit,
                         bool fullResultIsNotCached);

  static std::string_view toString(Status status);

  // A helper function for printing the details as a string.
  static void formatDetailValue(std::ostream& out, std::string_view key,
                                const nlohmann::json& value);
};

// A class to store information about the execution of a complete query, e.g.
// the time spent during query planning. Note: The information about the
// `QueryExecutionTree` (e.g. how much time was spent in which operation) is
// stored in the `RuntimeInformation` class above.
struct RuntimeInformationWholeQuery {
  // The time spent during query planning (this does not include the time spent
  // on `IndexScan`s that were executed during the query planning).
  std::chrono::milliseconds timeQueryPlanning = RuntimeInformation::ZERO;
  /// Output as json. The signature of this function is mandated by the json
  /// library to allow for implicit conversion.
  friend void to_json(nlohmann::ordered_json& j,
                      const RuntimeInformationWholeQuery& rti);
};
