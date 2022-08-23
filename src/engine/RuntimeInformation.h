// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2019      Florian Kramer (florian.kramer@neptun.uni-freiburg.de)
//   2022-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)
#pragma once

#include <absl/strings/str_join.h>
#include <util/HashMap.h>
#include <util/json.h>

#include <iostream>
#include <string>
#include <vector>

/// A class to store information about the status of an operation (result size, time to compute, status, etc.). Also contains the functionality to print that information nicely formatted and to export it to JSON.
class RuntimeInformation {
 public:
  enum struct Status {
    notStarted,
    completed,
    failed,
    failedBecauseChildFailed
  };
  using enum Status;

  // Public members
  double time_ = 0.0;

  // In case this operation was read from the cache, we will store the time
  // information about the original computation in the following two members
  double originalTime_ = 0.0;
  double originalOperationTime_ = 0.0;

  size_t costEstimate_ = 0;
  size_t rows_ = 0;
  size_t sizeEstimate_ = 0;
  size_t cols_ = 0;

  std::vector<float> multiplictyEstimates_;
  bool wasCached_ = false;
  std::string descriptor_;
  std::vector<std::string> columnNames_;
  std::vector<RuntimeInformation> children_;
  Status status_ = Status::notStarted;
  nlohmann::json details_;

  RuntimeInformation() = default;

  [[nodiscard]] std::string toString() const;

  void writeToStream(std::ostream& out, size_t indent = 1) const;

  friend void to_json (nlohmann::ordered_json& j, const RuntimeInformation& rti);

  void setColumnNames(
      const ad_utility::HashMap<std::string, size_t>& columnMap);

  [[nodiscard]] double getOperationTime() const;

  [[nodiscard]] size_t getOperationCostEstimate() const;

  // The time spend in children
  [[nodiscard]] double getChildrenTime() const;


  template <typename T>
  void addDetail(const std::string& key, const T& value) {
    details_[key] = value;
  }

 private:
  static std::string_view toString(Status status);
};
