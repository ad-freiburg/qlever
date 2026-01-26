//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "engine/sparqlExpressions/NaryExpressionImpl.h"

namespace sparqlExpression {
namespace detail {

using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;
using Literal = ad_utility::triple_component::Literal;

// Date functions.
// The input is `std::nullopt` if the argument to the expression is not a date.

//______________________________________________________________________________
struct ExtractYear {
  Id operator()(std::optional<DateYearOrDuration> d) const {
    if (!d.has_value()) {
      return Id::makeUndefined();
    } else {
      return Id::makeFromInt(d->getYear());
    }
  }
};

//______________________________________________________________________________
struct ExtractMonth {
  Id operator()(std::optional<DateYearOrDuration> d) const {
    // TODO<C++23> Use the monadic operations for std::optional
    if (!d.has_value()) {
      return Id::makeUndefined();
    }
    auto optionalMonth = d.value().getMonth();
    if (!optionalMonth.has_value()) {
      return Id::makeUndefined();
    }
    return Id::makeFromInt(optionalMonth.value());
  }
};

//______________________________________________________________________________
struct ExtractDay {
  Id operator()(std::optional<DateYearOrDuration> d) const {
    // TODO<C++23> Use the monadic operations for `std::optional`.
    if (!d.has_value()) {
      return Id::makeUndefined();
    }
    auto optionalDay = d.value().getDay();
    if (!optionalDay.has_value()) {
      return Id::makeUndefined();
    }
    return Id::makeFromInt(optionalDay.value());
  }
};

//______________________________________________________________________________
struct ExtractStrTimezone {
  IdOrLiteralOrIri operator()(std::optional<DateYearOrDuration> d) const {
    // TODO<C++23> Use the monadic operations for std::optional
    if (!d.has_value()) {
      return Id::makeUndefined();
    }
    auto timezoneStr = d.value().getStrTimezone();
    return LiteralOrIri{Literal::literalWithNormalizedContent(
        asNormalizedStringViewUnsafe(timezoneStr))};
  }
};

//______________________________________________________________________________
struct ExtractTimezoneDurationFormat {
  Id operator()(std::optional<DateYearOrDuration> d) const {
    // TODO<C++23> Use the monadic operations for std::optional
    if (!d.has_value()) {
      return Id::makeUndefined();
    }
    const auto& optDayTimeDuration =
        DateYearOrDuration::xsdDayTimeDurationFromDate(d.value());
    return optDayTimeDuration.has_value()
               ? Id::makeFromDate(optDayTimeDuration.value())
               : Id::makeUndefined();
  }
};

//______________________________________________________________________________
template <auto dateMember, auto makeId>
struct ExtractTimeComponentImpl {
  Id operator()(std::optional<DateYearOrDuration> d) const {
    if (!d.has_value() || !d->isDate()) {
      return Id::makeUndefined();
    }
    Date date = d.value().getDate();
    if (!date.hasTime()) {
      return Id::makeUndefined();
    }
    return std::invoke(makeId, std::invoke(dateMember, date));
  }
};

//______________________________________________________________________________
using ExtractHours = ExtractTimeComponentImpl<&Date::getHour, &Id::makeFromInt>;
using ExtractMinutes =
    ExtractTimeComponentImpl<&Date::getMinute, &Id::makeFromInt>;
using ExtractSeconds =
    ExtractTimeComponentImpl<&Date::getSecond, &Id::makeFromDouble>;

//______________________________________________________________________________
NARY_EXPRESSION(MonthExpression, 1, FV<ExtractMonth, DateValueGetter>);
NARY_EXPRESSION(DayExpression, 1, FV<ExtractDay, DateValueGetter>);
NARY_EXPRESSION(TimezoneStrExpression, 1,
                FV<ExtractStrTimezone, DateValueGetter>);
NARY_EXPRESSION(TimezoneDurationExpression, 1,
                FV<ExtractTimezoneDurationFormat, DateValueGetter>);
NARY_EXPRESSION(HoursExpression, 1, FV<ExtractHours, DateValueGetter>);
NARY_EXPRESSION(MinutesExpression, 1, FV<ExtractMinutes, DateValueGetter>);
NARY_EXPRESSION(SecondsExpression, 1, FV<ExtractSeconds, DateValueGetter>);

//______________________________________________________________________________
// `YearExpression` requires `YearExpressionImpl` to be easily identifiable if
// provided as a `SparqlExpression*` object.
CPP_class_template(typename NaryOperation)(
    requires(isOperation<NaryOperation>)) class YearExpressionImpl
    : public NaryExpression<NaryOperation> {
 public:
  using NaryExpression<NaryOperation>::NaryExpression;
  bool isYearExpression() const override { return true; }
};

using YearExpression =
    YearExpressionImpl<Operation<1, FV<ExtractYear, DateValueGetter>>>;

}  // namespace detail
using namespace detail;

//______________________________________________________________________________
SparqlExpression::Ptr makeYearExpression(SparqlExpression::Ptr child) {
  return std::make_unique<YearExpression>(std::move(child));
}

SparqlExpression::Ptr makeDayExpression(SparqlExpression::Ptr child) {
  return std::make_unique<DayExpression>(std::move(child));
}

SparqlExpression::Ptr makeTimezoneStrExpression(SparqlExpression::Ptr child) {
  return std::make_unique<TimezoneStrExpression>(std::move(child));
}

SparqlExpression::Ptr makeTimezoneExpression(SparqlExpression::Ptr child) {
  return std::make_unique<TimezoneDurationExpression>(std::move(child));
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
