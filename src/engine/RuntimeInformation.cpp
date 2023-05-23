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
  out << indentStr(indent) << "total_time: " << getTotalTimeCorrected() << " ms"
      << '\n';
  out << indentStr(indent) << "operation_time: " << getOperationTime() << " ms"
      << '\n';
  out << indentStr(indent)
      << "cache_status: " << ad_utility::toString(cacheStatus_) << '\n';
  if (cacheStatus_ != ad_utility::CacheStatus::computed) {
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
void RuntimeInformation::setColumnNames(const VariableToColumnMap& columnMap) {
  if (columnMap.empty()) {
    return;
  }

  ColumnIndex maxColumnIndex = std::ranges::max(
      columnMap | std::views::values |
      std::views::transform(&ColumnIndexAndTypeInfo::columnIndex_));
  // Resize the `columnNames_` vector such that we can use the keys from
  // columnMap (which are not necessarily consecutive) as indexes.
  columnNames_.resize(maxColumnIndex + 1);
  // Now copy the (variable, index) pairs to the vector.
  for (const auto& [variable, columnIndexAndType] : columnMap) {
    ColumnIndex columnIndex = columnIndexAndType.columnIndex_;
    columnNames_.at(columnIndex) = variable.name();
  }
}

// ________________________________________________________________________________________________________________
double RuntimeInformation::getOperationTime() const {
  if (cacheStatus_ != ad_utility::CacheStatus::computed) {
    return totalTime_;
  } else {
    // If a child was computed during the query planning, the time spent
    // computing that child is *not* included in this operation's
    // `totalTime_`. That's why we skip such children in the following loop.
    auto result = totalTime_;
    for (const RuntimeInformation& child : children_) {
      if (child.status_ != completedDuringQueryPlanning) {
        result -= child.totalTime_;
      }
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
    case completedDuringQueryPlanning:
      return "completed during query planning";
    case notStarted:
      return "not started";
    case optimizedOut:
      return "optimized out";
    case failed:
      return "failed";
    case failedBecauseChildFailed:
      return "failed because child failed";
    default:
      AD_FAIL();
  }
}

// _____________________________________________________________________________
double RuntimeInformation::getTotalTimeCorrected() const {
  double timeOfChildrenComputedDuringQueryPlanning = 0;

  // Recursively get the `totalTime_` of all descendants that were computed
  // during the query planning and add it to
  // `timeOfChildrenComputedDuringQueryPlanning`. The pattern of a lambda that
  // is called with itself as an argument is required for requires lambdas up
  // until C++20 (for a detailed read, see
  // http://pedromelendez.com/blog/2015/07/16/recursive-lambdas-in-c14/
  // TODO<joka921, C++23> Use `explicit object parameters` aka `deducing this`
  // to simplify the recursive lambda.
  auto recursiveImpl = [&](const auto& recursiveCall,
                           const RuntimeInformation& child) -> void {
    if (child.status_ ==
        RuntimeInformation::Status::completedDuringQueryPlanning) {
      timeOfChildrenComputedDuringQueryPlanning += child.totalTime_;
    }
    for (const auto& descendant : child.children_) {
      recursiveCall(recursiveCall, descendant);
    }
  };
  recursiveImpl(recursiveImpl, *this);
  return totalTime_ + timeOfChildrenComputedDuringQueryPlanning;
}

// ________________________________________________________________________________________________________________
void to_json(nlohmann::ordered_json& j, const RuntimeInformation& rti) {
  j = nlohmann::ordered_json{
      {"description", rti.descriptor_},
      {"result_rows", rti.numRows_},
      {"result_cols", rti.numCols_},
      {"column_names", rti.columnNames_},
      {"total_time", rti.getTotalTimeCorrected()},
      {"operation_time", rti.getOperationTime()},
      {"original_total_time", rti.originalTotalTime_},
      {"original_operation_time", rti.originalOperationTime_},
      {"cache_status", ad_utility::toString(rti.cacheStatus_)},
      {"details", rti.details_},
      {"estimated_total_cost", rti.costEstimate_},
      {"estimated_operation_cost", rti.getOperationCostEstimate()},
      {"estimated_column_multiplicities", rti.multiplicityEstimates_},
      {"estimated_size", rti.sizeEstimate_},
      {"status", RuntimeInformation::toString(rti.status_)},
      {"children", rti.children_}};
}

// ________________________________________________________________________________________________________________
void to_json(nlohmann::ordered_json& j,
             const RuntimeInformationWholeQuery& rti) {
  j = nlohmann::ordered_json{
      {"time_query_planning", rti.timeQueryPlanning},
      {"time_index_scans_query_planning", rti.timeIndexScansQueryPlanning}};
}

// ___________________________________________________________________________________
void RuntimeInformation::addLimitOffsetRow(const LimitOffsetClause& l,
                                           size_t timeForLimit,
                                           bool fullResultIsNotCached) {
  bool hasLimit = l._limit.has_value();
  bool hasOffset = l._offset != 0;
  if (!(hasLimit || hasOffset)) {
    return;
  }
  children_ = std::vector{*this};
  auto& actualOperation = children_.at(0);
  numRows_ = l.actualSize(actualOperation.numRows_);
  details_.clear();
  cacheStatus_ = ad_utility::CacheStatus::computed;
  totalTime_ += static_cast<double>(timeForLimit);
  actualOperation.addDetail("not-written-to-cache-because-child-of-limit",
                            fullResultIsNotCached);
  sizeEstimate_ = l.actualSize(sizeEstimate_);

  // Update the descriptor.
  descriptor_.clear();
  if (hasLimit) {
    descriptor_ = absl::StrCat("LIMIT ", l._limit.value());
  }

  if (hasOffset) {
    absl::StrAppend(&descriptor_, hasLimit ? " " : "", "OFFSET ", l._offset);
  }
}
