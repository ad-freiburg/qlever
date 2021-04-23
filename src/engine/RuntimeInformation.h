// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)
//
#pragma once

#include <iostream>
#include <nlohmann/fifo_map.hpp>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

#include "../util/HashMap.h"
#include "../util/StringUtils.h"
#include "../util/Timer.h"

class RuntimeInformation {
 public:
  // The following declarations introduce an ordered_json type
  // that functions exactly like nlohmann::json but keeps
  // object keys in insertion order.
  // This helps to make the JSON more readable as it forces children to be
  // printed after their parents. See also
  // https://github.com/nlohmann/json/issues/485#issuecomment-333652309
  template <class K, class V, class dummy_compare, class A>
  using fifo_map = nlohmann::fifo_map<K, V, nlohmann::fifo_map_compare<K>, A>;
  using ordered_json = nlohmann::basic_json<fifo_map>;

  friend inline void to_json(RuntimeInformation::ordered_json& j,
                             const RuntimeInformation& rti);

  RuntimeInformation()
      : _time(0),
        _rows(0),
        _cols(0),
        _wasCached(false),
        _descriptor(),
        _columnNames(),
        _details(),
        _children() {}

  std::string toString() const {
    std::ostringstream buffer;
    // imbue with the same locale as std::cout which uses for exanoke
    // thousands separators
    buffer.imbue(ad_utility::commaLocale);
    // So floats use fixed precision
    buffer << std::fixed << std::setprecision(2);
    writeToStream(buffer);
    return buffer.str();
  }

  void writeToStream(std::ostream& out, size_t indent = 1) const {
    using json = nlohmann::json;
    out << indentStr(indent) << '\n';
    out << indentStr(indent - 1) << u8"├─ " << _descriptor << '\n';
    out << indentStr(indent) << "result_size: " << _rows << " x " << _cols
        << '\n';
    out << indentStr(indent)
        << "columns: " << ad_utility::join(_columnNames, ", ") << '\n';
    out << indentStr(indent) << "total_time: " << _time << " ms" << '\n';
    out << indentStr(indent) << "operation_time: " << getOperationTime()
        << " ms" << '\n';
    out << indentStr(indent) << "cached: " << ((_wasCached) ? "true" : "false")
        << '\n';
    for (const auto& el : _details.items()) {
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
      if (ad_utility::endsWith(el.key(), "Time")) {
        out << " ms";
      }
      out << '\n';
    }
    if (_children.size()) {
      out << indentStr(indent) << u8"┬\n";
      for (const RuntimeInformation& child : _children) {
        child.writeToStream(out, indent + 1);
      }
    }
  }

  void setDescriptor(const std::string& descriptor) {
    _descriptor = descriptor;
  }

  void setColumnNames(
      const ad_utility::HashMap<std::string, size_t>& columnMap) {
    _columnNames.resize(columnMap.size());
    for (const auto& column : columnMap) {
      _columnNames[column.second] = column.first;
    }
  }

  // Set the overall time in milliseconds
  void setTime(double time) { _time = time; }

  // Get the overall time in milliseconds
  double getTime() const { return _time; }

  // Set the number of rows
  void setRows(size_t rows) { _rows = rows; }

  // Get the number of rows
  size_t getRows() { return _rows; }

  // Set the number of columns
  void setCols(size_t cols) { _cols = cols; }

  // Get the number of columns
  size_t getCols() { return _cols; }

  double getOperationTime() const {
    if (_wasCached) {
      return getTime();
    } else {
      return getTime() - getChildrenTime();
    }
  }

  // The time spend in children
  double getChildrenTime() const {
    double sum = 0;
    for (const RuntimeInformation& child : _children) {
      sum += child.getTime();
    }
    return sum;
  }

  void setWasCached(bool wasCached) { _wasCached = wasCached; }

  void addChild(const RuntimeInformation& r) { _children.push_back(r); }

  // direct access to the children
  std::vector<RuntimeInformation>& children() { return _children; }
  [[nodiscard]] const std::vector<RuntimeInformation>& children() const {
    return _children;
  }

  template <typename T>
  void addDetail(const std::string& key, const T& value) {
    _details[key] = value;
  }

 private:
  static std::string indentStr(size_t indent) {
    std::string ind;
    for (size_t i = 0; i < indent; i++) {
      ind += u8"│  ";
    }
    return ind;
  }

  double _time;
  size_t _rows;
  size_t _cols;
  bool _wasCached;
  std::string _descriptor;
  std::vector<std::string> _columnNames;
  nlohmann::json _details;
  std::vector<RuntimeInformation> _children;
};

inline void to_json(RuntimeInformation::ordered_json& j,
                    const RuntimeInformation& rti) {
  j = RuntimeInformation::ordered_json{
      {"description", rti._descriptor},
      {"result_rows", rti._rows},
      {"result_cols", rti._cols},
      {"column_names", rti._columnNames},
      {"total_time", rti._time},
      {"operation_time", rti.getOperationTime()},
      {"was_cached", rti._wasCached},
      {"details", rti._details},
      {"children", rti._children}};
}
