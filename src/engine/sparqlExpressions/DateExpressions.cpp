//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "engine/sparqlExpressions/NaryExpressionImpl.h"

namespace sparqlExpression {
namespace detail {
// Date functions.
// The input is `std::nullopt` if the argument to the expression is not a date.
inline auto extractYear = [](std::optional<DateOrLargeYear> d) {
  if (!d.has_value()) {
    return Id::makeUndefined();
  } else {
    return Id::makeFromInt(d->getYear());
  }
};

inline auto extractMonth = [](std::optional<DateOrLargeYear> d) {
  // TODO<C++23> Use the monadic operations for std::optional
  if (!d.has_value()) {
    return Id::makeUndefined();
  }
  auto optionalMonth = d.value().getMonth();
  if (!optionalMonth.has_value()) {
    return Id::makeUndefined();
  }
  return Id::makeFromInt(optionalMonth.value());
};

inline auto extractDay = [](std::optional<DateOrLargeYear> d) {
  // TODO<C++23> Use the monadic operations for `std::optional`.
  if (!d.has_value()) {
    return Id::makeUndefined();
  }
  auto optionalDay = d.value().getDay();
  if (!optionalDay.has_value()) {
    return Id::makeUndefined();
  }
  return Id::makeFromInt(optionalDay.value());
};

inline auto extractHours = [](std::optional<DateOrLargeYear> d) {
  if (!d.has_value() || d->getType() != DateOrLargeYear::Type::DateTime) {
    return Id::makeUndefined();
  }
  auto hours = d.value().getDate().getHour();
  if (hours == -1 ) {
    return Id::makeUndefined();
  }
  return Id::makeFromInt(hours);
};

inline auto extractMinutes = [](std::optional<DateOrLargeYear> d) {
  if (!d.has_value() || d->getType() != DateOrLargeYear::Type::DateTime) {
    return Id::makeUndefined();
  }
  auto minutes = d.value().getDate().getMinute();
  return Id::makeFromInt(minutes);
};

inline auto extractSeconds = [](std::optional<DateOrLargeYear> d) {
  if (!d.has_value() || d->getType() != DateOrLargeYear::Type::DateTime) {
    return Id::makeUndefined();
  }
  auto seconds = d.value().getDate().getSecond();
  return Id::makeFromDouble(seconds);
};

NARY_EXPRESSION(YearExpression, 1, FV<decltype(extractYear), DateValueGetter>);
NARY_EXPRESSION(MonthExpression, 1,
                FV<decltype(extractMonth), DateValueGetter>);
NARY_EXPRESSION(DayExpression, 1, FV<decltype(extractDay), DateValueGetter>);
NARY_EXPRESSION(HoursExpression, 1, FV<decltype(extractHours), DateValueGetter>);
NARY_EXPRESSION(MinutesExpression, 1, FV<decltype(extractMinutes), DateValueGetter>);
NARY_EXPRESSION(SecondsExpression, 1, FV<decltype(extractSeconds), DateValueGetter>);

}  // namespace detail
using namespace detail;
SparqlExpression::Ptr makeYearExpression(SparqlExpression::Ptr child) {
  return std::make_unique<YearExpression>(std::move(child));
}

SparqlExpression::Ptr makeDayExpression(SparqlExpression::Ptr child) {
  return std::make_unique<DayExpression>(std::move(child));
}

SparqlExpression::Ptr makeMonthExpression(SparqlExpression::Ptr child) {
  return std::make_unique<MonthExpression>(std::move(child));
}

SparqlExpression::Ptr makeHoursExpression(SparqlExpression::Ptr child) {
  return std::make_unique<HoursExpression>(std::move(child));
}

SparqlExpression::Ptr makeMinutesExpression(SparqlExpression::Ptr child) {
  return std::make_unique<MinutesExpression>(std::move(child));
}

SparqlExpression::Ptr makeSecondsExpression(SparqlExpression::Ptr child) {
  return std::make_unique<SecondsExpression>(std::move(child));
}
}  // namespace sparqlExpression
