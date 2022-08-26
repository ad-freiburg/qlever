// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2022-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#include <engine/RuntimeInformation.h>
#include <util/Exception.h>
#include <util/Log.h>
#include <util/TransparentFunctors.h>

#include <ranges>

// ________________________________________________________________________________________________________________
std::string RuntimeInformation::toString() const {
  std::ostringstream buffer;
  // Imbue with the same locale as std::cout which uses for example
  // thousands separators.
  buffer.imbue(ad_utility::commaLocale);
  // So floats use fixed precision
  buffer << std::fixed << std::setprecision(2);
  writeToStream(buffer);
  return std::move(buffer).str();
}

namespace {
// A small formatting helper used inside RuntimeInformation::writeToStream.
std::string indentStr(size_t indent) {
  std::string ind;
  for (size_t i = 0; i < indent; i++) {
    ind += "│  ";
  }
  return ind;
}
}  // namespace

// ________________________________________________________________________________________________________________
void RuntimeInformation::writeToStream(std::ostream& out, size_t indent) const {
  using json = nlohmann::json;
  out << indentStr(indent) << '\n';
  out << indentStr(indent - 1) << "├─ " << descriptor_ << '\n';
  out << indentStr(indent) << "result_size: " << numRows_ << " x " << numCols_
      << '\n';
  out << indentStr(indent) << "columns: " << absl::StrJoin(columnNames_, ", ")
      << '\n';
  out << indentStr(indent) << "total_time: " << totalTime_ << " ms" << '\n';
  out << indentStr(indent) << "operation_time: " << getOperationTime() << " ms"
      << '\n';
  out << indentStr(indent) << "cached: " << ((wasCached_) ? "true" : "false")
      << '\n';
  if (wasCached_) {
    out << indentStr(indent) << "original_total_time: " << originalTotalTime_
        << " ms" << '\n';
    out << indentStr(indent)
        << "original_operation_time: " << originalOperationTime_ << " ms"
        << '\n';
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

// ________________________________________________________________________________________________________________
void RuntimeInformation::setColumnNames(
    const ad_utility::HashMap<std::string, size_t>& columnMap) {
  if (columnMap.empty()) {
    return;
  }
  // Resize the `columnNames_` vector such that we can use the keys from
  // columnMap (which are not necessarily consecutive) as indexes.
  auto maxColumnIndex =
      std::ranges::max_element(columnMap, {}, ad_utility::secondOfPair)->second;
  columnNames_.resize(maxColumnIndex + 1);
  // Now copy the (variable, index) pairs to the vector.
  for (const auto& [variable, columnIndex] : columnMap) {
    AD_CHECK(columnIndex < columnNames_.size());
    columnNames_[columnIndex] = variable;
  }
}

// ________________________________________________________________________________________________________________
double RuntimeInformation::getOperationTime() const {
  if (wasCached_) {
    return totalTime_;
  } else {
    auto result = totalTime_;
    for (const RuntimeInformation& child : children_) {
      result -= child.totalTime_;
    }
    return result;
  }
}

// ________________________________________________________________________________________________________________
size_t RuntimeInformation::getOperationCostEstimate() const {
  size_t result = costEstimate_;
  for (const auto& child : children_) {
    result -= child.costEstimate_;
  }
  return result;
}

// ________________________________________________________________________________________________________________
std::string_view RuntimeInformation::toString(Status status) {
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

// ________________________________________________________________________________________________________________
void to_json(nlohmann::ordered_json& j, const RuntimeInformation& rti) {
  j = nlohmann::ordered_json{
      {"description", rti.descriptor_},
      {"result_rows", rti.numRows_},
      {"result_cols", rti.numCols_},
      {"column_names", rti.columnNames_},
      {"total_time", rti.totalTime_},
      {"operation_time", rti.getOperationTime()},
      {"original_total_time", rti.originalTotalTime_},
      {"original_operation_time", rti.originalOperationTime_},
      {"was_cached", rti.wasCached_},
      {"details", rti.details_},
      {"estimated_total_cost", rti.costEstimate_},
      {"estimated_operation_cost", rti.getOperationCostEstimate()},
      {"estimated_column_multiplicities", rti.multiplicityEstimates_},
      {"estimated_size", rti.sizeEstimate_},
      {"status", RuntimeInformation::toString(rti.status_)},
      {"children", rti.children_}};
}
