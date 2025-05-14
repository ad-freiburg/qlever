// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "util/Timer.h"
#include "util/json.h"

#ifndef QLEVER_TIMETRACER_H
#define QLEVER_TIMETRACER_H

namespace ad_utility::timer {

struct Trace {
  std::string name_;
  std::chrono::milliseconds begin_;
  std::optional<std::chrono::milliseconds> end_ = std::nullopt;
  std::vector<Trace> children_ = {};

  std::chrono::milliseconds duration() const {
    if (!end_) {
      throw std::runtime_error("Trace has not yet ended.");
    }
    return end_.value() - begin_;
  }

  /// Output as json. The signature of this function is mandated by the json
  /// library to allow for implicit conversion.
  friend void to_json(nlohmann::ordered_json& j, const Trace& trace) {
    // TODO: deal with end not being present

    j = nlohmann::ordered_json{{"name", trace.name_},
                               {"begin", trace.begin_.count()},
                               {"end", trace.end_.value().count()},
                               {"duration", trace.duration().count()}};
    if (!trace.children_.empty()) {
      nlohmann::ordered_json children = nlohmann::ordered_json::array();
      for (const Trace& childTrace : trace.children_) {
        children.push_back(childTrace);
      }
      j["children"] = children;
    }
  }

  friend void to_json_short(nlohmann::ordered_json& j, const Trace& trace) {
    if (trace.children_.empty()) {
      j[trace.name_] = trace.duration().count();
    } else {
      nlohmann::ordered_json childJ = {{"total", trace.duration().count()}};
      for (const Trace& childTrace : trace.children_) {
        to_json_short(childJ, childTrace);
      }
      j[trace.name_] = childJ;
    }
  }
};

class TimeTracer {
  Timer timer_;
  Trace rootTrace_;
  std::vector<std::reference_wrapper<Trace>> activeTraces_;

 public:
  explicit TimeTracer(std::string name)
      : timer_(Timer::Started),
        rootTrace_{std::move(name), std::chrono::milliseconds::zero()},
        activeTraces_({rootTrace_}){};
  TimeTracer(const TimeTracer&) = delete;
  TimeTracer& operator=(const TimeTracer&) = delete;

  void beginTrace(std::string name) {
    if (activeTraces_.empty()) {
      throw std::runtime_error("The trace has ended.");
    }
    activeTraces_.back().get().children_.emplace_back(std::move(name),
                                                      timer_.msecs());
    activeTraces_.emplace_back(activeTraces_.back().get().children_.back());
  }

  void endTrace(std::string_view name) {
    if (activeTraces_.empty()) {
      throw std::runtime_error("The trace has ended.");
    }

    Trace& activeTrace = activeTraces_.back().get();
    if (activeTrace.name_ != name) {
      throw std::runtime_error(
          absl::StrCat("Tried to end trace \"", name, "\", but trace \"",
                       activeTrace.name_, "\" was running."));
    }
    activeTrace.end_ = timer_.msecs();
    activeTraces_.pop_back();
  }

  nlohmann::ordered_json getJSON() const { return {rootTrace_}; }
  nlohmann::ordered_json getJSONShort() const {
    nlohmann::ordered_json j;
    to_json_short(j, rootTrace_);
    return j;
  }
};

}  // namespace ad_utility::timer

#endif  // QLEVER_TIMETRACER_H
