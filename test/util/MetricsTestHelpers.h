// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_METRICSTESTHELPERS_H
#define QLEVER_METRICSTESTHELPERS_H

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <optional>
#include <string>
#include <string_view>
#include <utility>

using Label = std::pair<std::string_view, std::string_view>;
inline auto MetricIs = [](std::string_view metric, std::string_view value,
                          std::optional<Label> label = std::nullopt) {
  std::string labelText =
      label.has_value()
          ? absl::StrCat("{", label->first, "=\"", label->second, "\"}")
          : "";
  return testing::HasSubstr(absl::StrCat(metric, labelText, " ", value));
};
inline auto IsZero = [](std::string_view metric,
                        std::optional<Label> label = std::nullopt) {
  return MetricIs(metric, "0", label);
};

#endif  // QLEVER_METRICSTESTHELPERS_H
