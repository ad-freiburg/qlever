// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2022-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)
#include <engine/RuntimeInformation.h>
#include <util/Exception.h>
#include <util/Log.h>
#include <util/TransparentFunctors.h>

#include <ranges>

#include "util/ConcurrentCache.h"
#include "util/HashMap.h"

using namespace std::chrono_literals;

// __________________________________________________________________________
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
std::string indentStr(size_t indent, bool stripped = false) {
  std::string ind;
  for (size_t i = 0; i < indent; i++) {
    if (stripped && i == indent - 1) {
      ind += "│";
    } else {
      ind += "│  ";
    }
  }
  return ind;
}

auto toMs(std::chrono::microseconds us) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(us).count();
}
}  // namespace

// __________________________________________________________________________
void RuntimeInformation::formatDetailValue(std::ostream& out,
                                           std::string_view key,
                                           const nlohmann::json& value) {
  using enum nlohmann::json::value_t;
  // We want to print doubles and ints as their native type so they get
  // thousands separators. For everything else we let nlohmann::json handle it.
  if (value.type() == number_float) {
    out << value.get<double>();
  } else if (value.type() == number_unsigned) {
    out << value.get<uint64_t>();
  } else if (value.type() == number_integer) {
    out << value.get<int64_t>();
  } else {
    out << value;
  }
  if (key.ends_with("Time")) {
    out << " ms";
  }
}

// __________________________________________________________________________
void RuntimeInformation::writeToStream(std::ostream& out, size_t indent) const {
  out << indentStr(indent, true) << '\n';
  out << indentStr(indent - 1) << "├─ " << descriptor_ << '\n';
  out << indentStr(indent) << "result_size: " << numRows_ << " x " << numCols_
      << '\n';
  out << indentStr(indent) << "columns: " << absl::StrJoin(columnNames_, ", ")
      << '\n';
  out << indentStr(indent) << "total_time: " << toMs(totalTime_) << " ms"
      << '\n';
  out << indentStr(indent) << "operation_time: " << toMs(getOperationTime())
      << " ms" << '\n';
  out << indentStr(indent) << "status: " << toString(status_) << '\n';
  out << indentStr(indent)
      << "cache_status: " << ad_utility::toString(cacheStatus_) << '\n';
  if (cacheStatus_ != ad_utility::CacheStatus::computed) {
    out << indentStr(indent)
        // TODO<g++12, Clang 17> use `<< originalTotalTime_` directly
        << "original_total_time: " << toMs(originalTotalTime_) << " ms" << '\n';
    out << indentStr(indent)
        << "original_operation_time: " << toMs(originalOperationTime_) << " ms"
        << '\n';
  }
  for (const auto& el : details_.items()) {
    out << indentStr(indent) << "  " << el.key() << ": ";
    formatDetailValue(out, el.key(), el.value());
    out << '\n';
  }
  if (!children_.empty()) {
    out << indentStr(indent) << "┬\n";
    for (const auto& child : children_) {
      child->writeToStream(out, indent + 1);
    }
  }
}

// __________________________________________________________________________
void RuntimeInformation::setColumnNames(const VariableToColumnMap& columnMap) {
  // Empty hash map -> empty vector.
  if (columnMap.empty()) {
    columnNames_.clear();
    return;
  }

  // Resize the `columnNames_` vector such that we can use the keys from
  // columnMap (which are not necessarily consecutive) as indexes.
  ColumnIndex maxColumnIndex = ql::ranges::max(
      columnMap | ql::views::values |
      ql::views::transform(&ColumnIndexAndTypeInfo::columnIndex_));
  columnNames_.resize(maxColumnIndex + 1);

  // Now copy the `variable, index` pairs from the map to the vector. If the
  // column might contain UNDEF values, append ` (U)` to the variable name.
  //
  for (const auto& [variable, columnIndexAndType] : columnMap) {
    const auto& [columnIndex, undefStatus] = columnIndexAndType;
    std::string_view undefStatusSuffix =
        undefStatus == ColumnIndexAndTypeInfo::UndefStatus::AlwaysDefined
            ? ""
            : " (U)";
    columnNames_.at(columnIndex) =
        absl::StrCat(variable.name(), undefStatusSuffix);
  }
  // Replace the empty column names (columns that are present in the result, but
  // are not visible using a variable) by the placeholder "_" to make the
  // runtime information more readable.
  for (auto& name : columnNames_) {
    if (name.empty()) {
      name = "_";
    }
  }
}

// __________________________________________________________________________
std::chrono::microseconds RuntimeInformation::getOperationTime() const {
  if (cacheStatus_ != ad_utility::CacheStatus::computed) {
    return totalTime_;
  } else {
    // If a child was computed during the query planning, the time spent
    // computing that child is *not* included in this operation's
    // `totalTime_`. That's why we skip such children in the following loop.
    auto timesOfChildren =
        children_ | ql::views::transform(&RuntimeInformation::totalTime_);
    // Prevent "negative" computation times in case totalTime_ was not
    // computed for this yet.
    return std::max(0us, totalTime_ - std::reduce(timesOfChildren.begin(),
                                                  timesOfChildren.end(), 0us));
  }
}

// __________________________________________________________________________
size_t RuntimeInformation::getOperationCostEstimate() const {
  size_t result = costEstimate_;
  for (const auto& child : children_) {
    result -= child->costEstimate_;
  }
  return result;
}

// __________________________________________________________________________
std::string_view RuntimeInformation::toString(Status status) {
  switch (status) {
    case fullyMaterialized:
      return "fully materialized";
    case lazilyMaterialized:
      return "lazily materialized";
    case inProgress:
      return "in progress";
    case notStarted:
      return "not started";
    case optimizedOut:
      return "optimized out";
    case failed:
      return "failed";
    case failedBecauseChildFailed:
      return "failed because child failed";
    case cancelled:
      return "cancelled";
  }
  AD_FAIL();
}

// __________________________________________________________________________
RuntimeInformation::Status RuntimeInformation::fromString(
    std::string_view str) {
  static const ad_utility::HashMap<std::string, RuntimeInformation::Status>
      statusMap = {
          {"not started", RuntimeInformation::notStarted},
          {"in progress", RuntimeInformation::inProgress},
          {"fully materialized", RuntimeInformation::fullyMaterialized},
          {"lazily materialized", RuntimeInformation::lazilyMaterialized},
          {"optimized out", RuntimeInformation::optimizedOut},
          {"failed", RuntimeInformation::failed},
          {"failed because child failed",
           RuntimeInformation::failedBecauseChildFailed},
          {"cancelled", RuntimeInformation::cancelled}};

  auto it = statusMap.find(str);
  if (it == statusMap.end()) {
    AD_FAIL();
  }
  return it->second;
}

// ________________________________________________________________________________________________________________
void to_json(nlohmann::ordered_json& j,
             const std::shared_ptr<RuntimeInformation>& rti) {
  AD_CONTRACT_CHECK(rti);
  to_json(j, *rti);
}

// ________________________________________________________________________________________________________________
void to_json(nlohmann::ordered_json& j, const RuntimeInformation& rti) {
  j = nlohmann::ordered_json{
      {"description", rti.descriptor_},
      {"result_rows", rti.numRows_},
      {"result_cols", rti.numCols_},
      {"column_names", rti.columnNames_},
      {"total_time", toMs(rti.totalTime_)},
      {"operation_time", toMs(rti.getOperationTime())},
      {"original_total_time", toMs(rti.originalTotalTime_)},
      {"original_operation_time", toMs(rti.originalOperationTime_)},
      {"cache_status", ad_utility::toString(rti.cacheStatus_)},
      {"details", rti.details_},
      {"estimated_total_cost", rti.costEstimate_},
      {"estimated_operation_cost", rti.getOperationCostEstimate()},
      {"estimated_column_multiplicities", rti.multiplicityEstimates_},
      {"estimated_size", rti.sizeEstimate_},
      {"status", RuntimeInformation::toString(rti.status_)},
      {"children", rti.children_}};
}

// __________________________________________________________________________
void to_json(nlohmann::ordered_json& j,
             const RuntimeInformationWholeQuery& rti) {
  j = nlohmann::ordered_json{
      {"time_query_planning", rti.timeQueryPlanning.count()}};
}

// __________________________________________________________________________
void from_json(const nlohmann::json& j, RuntimeInformation& rti) {
  // Helper lambdas to ignore missing key or invalid value.
  auto tryGet = [&j]<typename T>(T& dst, std::string_view key) {
    try {
      j.at(key).get_to(dst);
    } catch (const nlohmann::json::exception& e) {
    }
  };
  using namespace std::chrono;
  auto tryGetTime = [&j](microseconds& dst, std::string_view key) {
    try {
      dst =
          duration_cast<microseconds>(milliseconds(j.at(key).get<uint64_t>()));
    } catch (const nlohmann::json::exception& e) {
    }
  };

  tryGet(rti.descriptor_, "description");
  tryGet(rti.numRows_, "result_rows");
  tryGet(rti.numCols_, "result_cols");
  tryGet(rti.columnNames_, "column_names");
  tryGetTime(rti.totalTime_, "total_time");
  tryGetTime(rti.originalTotalTime_, "original_total_time");
  tryGetTime(rti.originalOperationTime_, "original_operation_time");
  if (auto it = j.find("cache_status"); it != j.end()) {
    rti.cacheStatus_ = ad_utility::fromString(it->get<std::string_view>());
  }
  tryGet(rti.details_, "details");
  tryGet(rti.costEstimate_, "estimated_total_cost");
  tryGet(rti.multiplicityEstimates_, "estimated_column_multiplicities");
  tryGet(rti.sizeEstimate_, "estimated_size");
  if (auto it = j.find("status"); it != j.end()) {
    rti.status_ = RuntimeInformation::fromString(it->get<std::string>());
  }
  if (auto it = j.find("children"); it != j.end()) {
    for (const auto& child : *it) {
      rti.children_.push_back(std::make_shared<RuntimeInformation>(child));
    }
  }
}

// __________________________________________________________________________
void RuntimeInformation::addLimitOffsetRow(const LimitOffsetClause& l,
                                           bool fullResultIsNotCached) {
  bool hasLimit = l._limit.has_value();
  bool hasOffset = l._offset != 0;
  if (!(hasLimit || hasOffset)) {
    return;
  }
  // Create a shallow copy, so we can modify the original values without
  // losing anything
  children_ = std::vector{std::make_shared<RuntimeInformation>(*this)};
  const auto& actualOperation = children_.at(0);
  numRows_ = l.actualSize(actualOperation->numRows_);
  details_.clear();
  cacheStatus_ = ad_utility::CacheStatus::computed;
  actualOperation->addDetail("not-written-to-cache-because-child-of-limit",
                             fullResultIsNotCached);
  actualOperation->eraseDetail("limit");
  actualOperation->eraseDetail("offset");
  addDetail("executed-implicitly-during-query-export", !fullResultIsNotCached);
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
