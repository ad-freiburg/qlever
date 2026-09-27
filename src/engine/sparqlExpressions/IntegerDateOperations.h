// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of this project.

#pragma once

#include <cstdint>
#include <optional>

#include "backports/algorithm.h"
#include "backports/span.h"
#include "global/Id.h"
#include "util/Exception.h"

namespace ql::engine::scalar {

// Integer-space date helpers: build a date `Id` from its components
// and read the year, month or day of a date `Id` as an integer, without going
// through the string or `DateYearOrDuration` expression machinery. All
// functions delegate to the `Id`/`Date` representation.
class IntegerDateOperations {
 public:
  // ___________________________________________________________________________
  // Build a date `Id`. Throws `DateOutOfRangeException` if a component is out
  // of range (for example month 13). Not constexpr, because
  // `DateYearOrDuration` has no constexpr constructor.
  [[nodiscard]] static Id makePackedDate(int year, uint8_t month, uint8_t day,
                                         uint8_t hour = 0, uint8_t minute = 0,
                                         uint8_t second = 0) {
    return Id::makeFromDate(DateYearOrDuration{
        Date{year, month, day, hour, minute, static_cast<double>(second)}});
  }

  // ___________________________________________________________________________
  // The year of `dateId`, `std::nullopt` if `dateId` is not a date.
  [[nodiscard]] static std::optional<int64_t> extractYear(Id dateId) noexcept {
    if (dateId.getDatatype() != Datatype::Date) {
      return std::nullopt;
    }
    return dateId.getDate().getYear();
  }

  // ___________________________________________________________________________
  // The month of `dateId`, `std::nullopt` if `dateId` is not a date or has no
  // month (for example an `xsd:gYear`).
  [[nodiscard]] static std::optional<int64_t> extractMonth(Id dateId) noexcept {
    if (dateId.getDatatype() != Datatype::Date) {
      return std::nullopt;
    }
    return dateId.getDate().getMonth();
  }

  // ___________________________________________________________________________
  // The day of `dateId`, `std::nullopt` if `dateId` is not a date or has no
  // day.
  [[nodiscard]] static std::optional<int64_t> extractDay(Id dateId) noexcept {
    if (dateId.getDatatype() != Datatype::Date) {
      return std::nullopt;
    }
    return dateId.getDate().getDay();
  }

  // ___________________________________________________________________________
  // Write the year of each of the `inputDates` to the element of `outputYears`
  // at the same position (`std::nullopt` for non-date `Id`s). `outputYears`
  // must be at least as large as `inputDates`.
  static void extractYearsBatch(ql::span<const Id> inputDates,
                                ql::span<std::optional<int64_t>> outputYears) {
    AD_CONTRACT_CHECK(outputYears.size() >= inputDates.size());
    ql::ranges::transform(inputDates, outputYears.begin(), &extractYear);
  }
};

}  // namespace ql::engine::scalar
