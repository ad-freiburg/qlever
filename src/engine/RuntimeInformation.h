// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2019      Florian Kramer (florian.kramer@neptun.uni-freiburg.de)
//   2022-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)
#pragma once

#include <absl/strings/str_join.h>
#include <util/HashMap.h>
#include <util/StringUtils.h>
#include <util/Timer.h>
#include <util/json.h>

#include <iostream>
#include <string>
#include <vector>

class RuntimeInformation {
 public:
  // Ordered JSON that preserves the insertion order.
  // This helps to make the JSON more readable as it forces children to be
  // printed after their parents. See also
  // https://github.com/nlohmann/json/issues/485#issuecomment-333652309
  using ordered_json = nlohmann::ordered_json;

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

  friend inline void to_json(RuntimeInformation::ordered_json& j,
                             const RuntimeInformation& rti);

  RuntimeInformation() = default;

  std::string toString() const {
    std::ostringstream buffer;
    // imbue with the same locale as std::cout which uses for exanoke
    // thousands separators
    buffer.imbue(ad_utility::commaLocale);
    // So floats use fixed precision
    buffer << std::fixed << std::setprecision(2);
    writeToStream(buffer);
    return std::move(buffer).str();
  }

  void writeToStream(std::ostream& out, size_t indent = 1) const {
    out << indentStr(indent) << '\n';
    out << indentStr(indent - 1) << "├─ " << descriptor_ << '\n';
    out << indentStr(indent) << "result_size: " << rows_ << " x " << cols_
        << '\n';
    out << indentStr(indent) << "columns: " << absl::StrJoin(columnNames_, ", ")
        << '\n';
    out << indentStr(indent) << "total_time: " << time_ << " ms" << '\n';
    out << indentStr(indent) << "operation_time: " << getOperationTime()
        << " ms" << '\n';
    out << indentStr(indent) << "cached: " << ((wasCached_) ? "true" : "false")
        << '\n';
    if (wasCached_) {
      out << indentStr(indent) << "original_total_time: " << originalTime__ << " ms" << '\n';
      out << indentStr(indent) << "original_operation_time: " << originalOperationTime_
          << " ms" << '\n';
    }
    for (const auto& el : details_.items()) {
      out << indentStr(indent) << "  " << el.key() << ": ";
      // We want to print doubles with fixed precision and stream ints as their
      // native type so they get thousands separators. For everything else we
      // let nlohmann::json handle it
      if (el.value().type() == json::value_t::number_float) {
        out << ad_utility::to_string(el.value().get<double>(), 2);
      } else if (el.value().type() == json::value_t::number_unsigned) {
        out << el.value().get<uint64_t>();
      } else if (el.value().type() == json::value_t::number_integer) {
        out << el.value().get<int64_t>();
      } else {
        out << el.value();
      }
      if (el.key().ends_with("Time")) {
        out << " ms";
      }
      out << '\n';
    }
    if (!children_.empty()) {
      out << indentStr(indent) << "┬\n";
      for (const RuntimeInformation& child : children_) {
        child.writeToStream(out, indent + 1);
      }
    }
  }

  void setColumnNames(
      const ad_utility::HashMap<std::string, size_t>& columnMap) {
    columnNames_.resize(columnMap.size());
    for (const auto& [variable, columnIndex] : columnMap) {
      AD_CHECK(columnIndex < columnNames_.size());
      columnNames_[columnIndex] = variable;
    }
  }

  double getOperationTime() const {
    if (wasCached_) {
      return time_;
    } else {
      return time_ - getChildrenTime();
    }
  }

  size_t getOperationCostEstimate() const {
    size_t result = costEstimate_;
    for (const auto& child : children_) {
      result -= child.costEstimate_;
    }
    return result;
  }

  // The time spend in children
  double getChildrenTime() const {
    double sum = 0;
    for (const RuntimeInformation& child : children_) {
      sum += child.time_;
    }
    return sum;
  }

  template <typename T>
  void addDetail(const std::string& key, const T& value) {
    details_[key] = value;
  }

 private:
  static std::string indentStr(size_t indent) {
    std::string ind;
    for (size_t i = 0; i < indent; i++) {
      ind += "│  ";
    }
    return ind;
  }

  static std::string_view toString(Status status) {
    switch (status) {
      case completed:
        return "completed";
      case notStarted:
        return "not started";
      case failed:
        return "failed";
      case failedBecauseChildFailed:
        return "failed because child failed";
      default:
        AD_FAIL();
    }
  }
};

inline void to_json(RuntimeInformation::ordered_json& j,
                    const RuntimeInformation& rti) {
  j = RuntimeInformation::ordered_json{
      {"description", rti.descriptor_},
      {"result_rows", rti.rows_},
      {"result_cols", rti.cols_},
      {"column_names", rti.columnNames_},
      {"total_time", rti.time_},
      {"operation_time", rti.getOperationTime()},
      {"original_total_time", rti.originalTime_},
      {"original_operation_time", rti.originalOperationTime_},
      {"was_cached", rti.wasCached_},
      {"details", rti.details_},
      {"estimated_total_cost", rti.costEstimate_},
      {"estimated_operation_cost", rti.getOperationCostEstimate()},
      {"estimated_column_multiplicities", rti.multiplictyEstimates_},
      {"estimated_size", rti.sizeEstimate_},
      {"status", RuntimeInformation::toString(rti.status_)},
      {"children", rti.children_}};
}
