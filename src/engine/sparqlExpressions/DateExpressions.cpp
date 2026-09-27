//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "engine/sparqlExpressions/IntegerDateOperations.h"
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "global/RuntimeParameters.h"

namespace sparqlExpression {
namespace detail {

using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;
using Literal = ad_utility::triple_component::Literal;
using ql::engine::scalar::IntegerDateOperations;

// Date functions.
// The input is `std::nullopt` if the argument to the expression is not a date.
// `ExtractYear`, `ExtractMonth` and `ExtractDay` additionally accept the date
// `Id` from `DateIdValueGetter` (`UNDEF` if the argument is not a date) and
// then read the component from the packed `Id` without decoding it into a
// `DateYearOrDuration` first. Which of the two overloads is used is decided by
// the runtime parameter `integer-date-extraction` (see `makeYearExpression`
// below).

// Return the `value` as an integer `Id`, `UNDEF` if it is `std::nullopt`.
inline Id makeIntOrUndefined(std::optional<int64_t> value) {
  return value.has_value() ? Id::makeFromInt(value.value())
                           : Id::makeUndefined();
}

//______________________________________________________________________________
struct ExtractYear {
  Id operator()(std::optional<DateYearOrDuration> d) const {
    if (!d.has_value()) {
      return Id::makeUndefined();
    } else {
      return Id::makeFromInt(d->getYear());
    }
  }

  Id operator()(Id id) const {
    return makeIntOrUndefined(IntegerDateOperations::extractYear(id));
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

  Id operator()(Id id) const {
    return makeIntOrUndefined(IntegerDateOperations::extractMonth(id));
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

  Id operator()(Id id) const {
    return makeIntOrUndefined(IntegerDateOperations::extractDay(id));
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
struct ExtractEpoch {
  Id operator()(std::optional<DateYearOrDuration> d) const {
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
    if (!d.has_value() || !d->isDate()) {
      return Id::makeUndefined();
    }
    Date date = d.value().getDate();

    std::optional<int64_t> epoch = date.toEpochInt();
    if (!epoch.has_value()) {
      return Id::makeUndefined();
    }
    return Id::makeFromInt(epoch.value());
#else
    throw std::runtime_error(
        "This QLever server does not support ql:toEpoch because it was "
        "compiled with the restricted feature set on C++ 17");
#endif
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
NARY_EXPRESSION(MonthFromIdExpression, 1, FV<ExtractMonth, DateIdValueGetter>);
NARY_EXPRESSION(DayFromIdExpression, 1, FV<ExtractDay, DateIdValueGetter>);
NARY_EXPRESSION(TimezoneStrExpression, 1,
                FV<ExtractStrTimezone, DateValueGetter>);
NARY_EXPRESSION(TimezoneDurationExpression, 1,
                FV<ExtractTimezoneDurationFormat, DateValueGetter>);
NARY_EXPRESSION(ToEpochExpression, 1, FV<ExtractEpoch, DateValueGetter>);
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
using YearFromIdExpression =
    YearExpressionImpl<Operation<1, FV<ExtractYear, DateIdValueGetter>>>;

// Return an expression of type `FromId` if the runtime parameter
// `integer-date-extraction` is set, and of type `FromDate` otherwise.
template <typename FromDate, typename FromId>
SparqlExpression::Ptr makeDateComponentExpression(SparqlExpression::Ptr child) {
  if (getRuntimeParameter<&RuntimeParameters::integerDateExtraction_>()) {
    return std::make_unique<FromId>(std::move(child));
  }
  return std::make_unique<FromDate>(std::move(child));
}

}  // namespace detail
using namespace detail;

//______________________________________________________________________________
SparqlExpression::Ptr makeYearExpression(SparqlExpression::Ptr child) {
  return makeDateComponentExpression<YearExpression, YearFromIdExpression>(
      std::move(child));
}

SparqlExpression::Ptr makeDayExpression(SparqlExpression::Ptr child) {
  return makeDateComponentExpression<DayExpression, DayFromIdExpression>(
      std::move(child));
}

SparqlExpression::Ptr makeTimezoneStrExpression(SparqlExpression::Ptr child) {
  return std::make_unique<TimezoneStrExpression>(std::move(child));
}

SparqlExpression::Ptr makeTimezoneExpression(SparqlExpression::Ptr child) {
  return std::make_unique<TimezoneDurationExpression>(std::move(child));
}

SparqlExpression::Ptr makeToEpochExpression(SparqlExpression::Ptr child) {
  return std::make_unique<ToEpochExpression>(std::move(child));
}

SparqlExpression::Ptr makeMonthExpression(SparqlExpression::Ptr child) {
  return makeDateComponentExpression<MonthExpression, MonthFromIdExpression>(
      std::move(child));
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
