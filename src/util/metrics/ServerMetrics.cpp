// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/metrics/ServerMetrics.h"

#include <opentelemetry/metrics/provider.h>

#include <utility>

#include "CompilationInfo.h"
#include "util/metrics/Metrics.h"

// _____________________________________________________________________________
ServerMetrics::ServerMetrics(
    absl::AnyInvocable<DeltaTriplesCount() const> getDeltaTriples,
    absl::AnyInvocable<int64_t() const> getMemoryLeft,
    absl::AnyInvocable<int64_t() const> getCacheUsed,
    absl::AnyInvocable<int64_t() const> getCacheLimit,
    absl::AnyInvocable<int64_t() const> getRebuildInProgress,
    std::optional<ad_utility::MemorySize> maxMem,
    absl::AnyInvocable<int64_t() const> getNumTriplesIndex,
    absl::AnyInvocable<ThreadPoolSnapshot() const> getThreadPoolStatistics)
    : getDeltaTriples_(std::move(getDeltaTriples)),
      getMemoryLeft_(std::move(getMemoryLeft)),
      getCacheUsed_(std::move(getCacheUsed)),
      getCacheLimit_(std::move(getCacheLimit)),
      getRebuildInProgress_(std::move(getRebuildInProgress)),
      getNumTriplesIndex_(std::move(getNumTriplesIndex)),
      getThreadPoolStatistics_(std::move(getThreadPoolStatistics)) {
  auto meter = opentelemetry::metrics::Provider::GetMeterProvider()->GetMeter(
      "qlever", "0.0.1");
  buildInfoMetric_ = meter->CreateInt64Gauge(
      "qlever.build_info",
      "Build information of the running QLever binary; the value is always 1, "
      "all information is contained in the labels");
  startTimeMetric_ = meter->CreateInt64Gauge(
      "qlever.server.start_time",
      "Unix timestamp when the QLever server was started", "s");
  indexLoadMetric_ = meter->CreateInt64Gauge(
      "qlever.index.load_time",
      "Unix timestamp when the current index was loaded", "s");
  startedSparqlOperations_ = meter->CreateUInt64Counter(
      "qlever.sparql_operation.started",
      "Number of SPARQL operations started since server start");
  runningSparqlOperations_ = meter->CreateInt64UpDownCounter(
      "qlever.sparql_operation.running",
      "Number of SPARQL operations currently being processed");
  finishedSparqlOperations_ = meter->CreateUInt64Counter(
      "qlever.sparql_operation.finished",
      "Number of SPARQL operations successfully finished since server start");
  sparqlOperationDuration_ = meter->CreateDoubleHistogram(
      "qlever.sparql_operation.duration",
      "Execution time of successful SPARQL operations", "ms");
  deltaTriplesMetric_ = meter->CreateInt64ObservableGauge(
      "qlever.delta_triples",
      "Number of triples that are updated relative to the index");
  sparqlErrors_ = meter->CreateUInt64Counter(
      "qlever.sparql_operation.errors",
      "Errors during the execution of SPARQL operations");
  httpErrors_ = meter->CreateUInt64Counter(
      "qlever.http.errors",
      "Errors during the execution of non SPARQL operations");
  memoryQueryFree_ = meter->CreateInt64ObservableGauge(
      "qlever.memory_query_available", "Available memory for query processing",
      "By");
  memoryQueryTotal_ =
      meter->CreateInt64Gauge("qlever.memory_query_limit",
                              "Memory allocated for query processing", "By");
  memoryCacheUsed_ = meter->CreateInt64ObservableGauge(
      "qlever.memory_cache_used", "Memory used for caching", "By");
  memoryCacheLimit_ = meter->CreateInt64ObservableGauge(
      "qlever.memory_cache_limit", "Memory allocated for caching", "By");
  rebuildInProgressMetric_ = meter->CreateInt64ObservableGauge(
      "qlever.index.rebuild_in_progress",
      "Whether an index rebuild is currently in progress (1) or not (0)");
  numTriplesIndex_ = meter->CreateInt64ObservableGauge(
      "qlever.index.num_triples",
      "Total number of triples in the index (excluding delta triples)");
  createThreadPoolInstruments(*meter);

  auto now = std::chrono::duration_cast<std::chrono::seconds>(
                 std::chrono::system_clock::now().time_since_epoch())
                 .count();
  startTimeMetric_->Record(now);
  indexLoadMetric_->Record(now);

  buildInfoMetric_->Record(
      1,
      {{"compiler", *qlever::version::compilerWithoutLinking.rlock()},
       {"compiler_version",
        *qlever::version::compilerVersionWithoutLinking.rlock()},
       {"version", *qlever::version::projectVersionWithoutLinking.rlock()},
       {"git_hash", *qlever::version::gitShortHashWithoutLinking.rlock()},
       {"compile_time",
        *qlever::version::timeOfCompilationUnixWithoutLinking.rlock()},
       {"cxx_standard", *qlever::version::cxxStandardWithoutLinking.rlock()}});
  if (maxMem.has_value()) {
    memoryQueryTotal_->Record(maxMem.value().getBytes());
  }

  // Record 0 once per label so every combination appears from the start.
  ad_utility::metrics::initializeCounter(startedSparqlOperations_.get(),
                                         "operation", {"query", "update"});
  ad_utility::metrics::initializeCounter(runningSparqlOperations_.get(),
                                         "operation", {"query", "update"});
  ad_utility::metrics::initializeCounter(finishedSparqlOperations_.get(),
                                         "operation", {"query", "update"});
  ad_utility::metrics::initializeCounter(
      sparqlErrors_.get(), "type",
      {"system_error", "internal", "syntax", "timeout",
       "send_streamable_response", "protocol", "in_use"});
  ad_utility::metrics::initializeCounter(httpErrors_.get(), "type",
                                         {"internal", "http"});
}

// _____________________________________________________________________________
ServerMetrics::~ServerMetrics() {
  deltaTriplesMetric_->RemoveCallback(&deltaTriplesCallback, this);
  memoryQueryFree_->RemoveCallback(
      &observeCallback<&ServerMetrics::getMemoryLeft_>, this);
  memoryCacheUsed_->RemoveCallback(
      &observeCallback<&ServerMetrics::getCacheUsed_>, this);
  memoryCacheLimit_->RemoveCallback(
      &observeCallback<&ServerMetrics::getCacheLimit_>, this);
  rebuildInProgressMetric_->RemoveCallback(
      &observeCallback<&ServerMetrics::getRebuildInProgress_>, this);
  numTriplesIndex_->RemoveCallback(
      &observeCallback<&ServerMetrics::getNumTriplesIndex_>, this);
  for (const auto& [instrument, callback] : threadPoolInstruments_) {
    instrument->RemoveCallback(callback, this);
  }
}

namespace {
constexpr std::array allTaskClasses{ad_utility::TaskClass::Query,
                                    ad_utility::TaskClass::IndexRebuild,
                                    ad_utility::TaskClass::Other};
}

// _____________________________________________________________________________
template <uint64_t ad_utility::TaskStatistics::*Time>
void ServerMetrics::taskTimeCallback(
    opentelemetry::metrics::ObserverResult result, void* state) {
  auto pool =
      static_cast<const ServerMetrics*>(state)->getThreadPoolStatistics_();
  for (ad_utility::TaskClass taskClass : allTaskClasses) {
    opentelemetry::nostd::get<opentelemetry::nostd::shared_ptr<
        opentelemetry::metrics::ObserverResultT<double>>>(result)
        ->Observe(
            ad_utility::nanosecondsToSeconds(pool.ofClass(taskClass).*Time),
            {{"class", std::string{toString(taskClass)}}});
  }
}

// _____________________________________________________________________________
void ServerMetrics::createThreadPoolInstruments(
    opentelemetry::metrics::Meter& meter) {
  using ad_utility::TaskClass;
  using ad_utility::TaskStatistics;
  using opentelemetry::metrics::ObserverResult;
  using opentelemetry::metrics::ObserverResultT;
  namespace nostd = opentelemetry::nostd;
  static constexpr auto snapshot = [](void* state) {
    return static_cast<const ServerMetrics*>(state)->getThreadPoolStatistics_();
  };
  static constexpr auto observeSeconds =
      [](const ObserverResult& result, uint64_t nanoseconds,
         std::string_view label, std::string_view value) {
        nostd::get<nostd::shared_ptr<ObserverResultT<double>>>(result)->Observe(
            ad_utility::nanosecondsToSeconds(nanoseconds),
            {{std::string{label}, std::string{value}}});
      };
  static constexpr auto observeCount = [](const ObserverResult& result,
                                          uint64_t count) {
    nostd::get<nostd::shared_ptr<ObserverResultT<int64_t>>>(result)->Observe(
        static_cast<int64_t>(count));
  };

  // The time of the threads of the pool, by what they did.
  threadPoolInstruments_.push_back(
      {meter.CreateDoubleObservableCounter(
           "qlever.thread_pool.thread_time",
           "Time of the threads of the thread pool, during which they ran a "
           "task and used a CPU (computing), ran a task and did not use a CPU "
           "(blocked), or ran no task although a task was waiting "
           "(idle_while_waiting)",
           "s"),
       [](ObserverResult result, void* state) {
         auto pool = snapshot(state);
         observeSeconds(result, pool.computingTime(), "state", "computing");
         observeSeconds(result, pool.blockedTime(), "state", "blocked");
         observeSeconds(result, pool.idleWhileWaitingTime_, "state",
                        "idle_while_waiting");
       }});

  // The number of tasks and their times, per class of tasks.
  threadPoolInstruments_.push_back(
      {meter.CreateInt64ObservableCounter(
           "qlever.thread_pool.tasks",
           "Number of tasks that were run on the thread pool"),
       [](ObserverResult result, void* state) {
         auto pool = snapshot(state);
         for (TaskClass taskClass : allTaskClasses) {
           nostd::get<nostd::shared_ptr<ObserverResultT<int64_t>>>(result)
               ->Observe(
                   static_cast<int64_t>(pool.ofClass(taskClass).numTasks_),
                   {{"class", std::string{toString(taskClass)}}});
         }
       }});
  auto addTaskTime = [this, &meter](const char* name, const char* description,
                                    Callback callback) {
    threadPoolInstruments_.push_back(
        {meter.CreateDoubleObservableCounter(name, description, "s"),
         callback});
  };
  addTaskTime("qlever.thread_pool.task_wait_time",
              "Time that tasks waited between being posted and being started",
              &taskTimeCallback<&TaskStatistics::waitTime_>);
  addTaskTime("qlever.thread_pool.task_wall_time",
              "Time that tasks ran, by the wall clock",
              &taskTimeCallback<&TaskStatistics::wallTime_>);
  addTaskTime("qlever.thread_pool.task_cpu_time",
              "Time that tasks ran, by the CPU clock of their thread",
              &taskTimeCallback<&TaskStatistics::cpuTime_>);
  addTaskTime(
      "qlever.thread_pool.result_wait_time",
      "Time that consumers were blocked waiting for the result of a task",
      &taskTimeCallback<&TaskStatistics::resultWaitTime_>);

  // The blocks of index scans that were read by tasks of the pool.
  threadPoolInstruments_.push_back(
      {meter.CreateInt64ObservableCounter(
           "qlever.thread_pool.scan_blocks",
           "Number of blocks of index scans that were read on the thread pool"),
       [](ObserverResult result, void* state) {
         observeCount(result, snapshot(state).total().numScanBlocks_);
       }});
  threadPoolInstruments_.push_back(
      {meter.CreateInt64ObservableCounter(
           "qlever.thread_pool.scan_size",
           "Size of the blocks of index scans that were read on the thread "
           "pool, after decompression",
           "By"),
       [](ObserverResult result, void* state) {
         observeCount(result, snapshot(state).total().numScanBytes_);
       }});
  threadPoolInstruments_.push_back(
      {meter.CreateInt64ObservableCounter(
           "qlever.thread_pool.scan_thread_changes",
           "Number of blocks of index scans that were read by another thread "
           "than the previous block of the same scan"),
       [](ObserverResult result, void* state) {
         observeCount(result, snapshot(state).total().numScanThreadChanges_);
       }});
  threadPoolInstruments_.push_back(
      {meter.CreateDoubleObservableCounter(
           "qlever.thread_pool.scan_cpu_time",
           "CPU time of the tasks that read blocks of index scans", "s"),
       [](ObserverResult result, void* state) {
         nostd::get<nostd::shared_ptr<ObserverResultT<double>>>(result)
             ->Observe(ad_utility::nanosecondsToSeconds(
                 snapshot(state).total().scanCpuTime_));
       }});
}

// _____________________________________________________________________________
void ServerMetrics::registerCallbacks() {
  for (const auto& [instrument, callback] : threadPoolInstruments_) {
    instrument->AddCallback(callback, this);
  }
  deltaTriplesMetric_->AddCallback(&deltaTriplesCallback, this);
  memoryQueryFree_->AddCallback(
      &observeCallback<&ServerMetrics::getMemoryLeft_>, this);
  memoryCacheUsed_->AddCallback(&observeCallback<&ServerMetrics::getCacheUsed_>,
                                this);
  memoryCacheLimit_->AddCallback(
      &observeCallback<&ServerMetrics::getCacheLimit_>, this);
  rebuildInProgressMetric_->AddCallback(
      &observeCallback<&ServerMetrics::getRebuildInProgress_>, this);
  numTriplesIndex_->AddCallback(
      &observeCallback<&ServerMetrics::getNumTriplesIndex_>, this);
}

// _____________________________________________________________________________
void ServerMetrics::observe(opentelemetry::metrics::ObserverResult result,
                            int64_t value) {
  opentelemetry::nostd::get<opentelemetry::nostd::shared_ptr<
      opentelemetry::metrics::ObserverResultT<int64_t>>>(result)
      ->Observe(value);
}

// _____________________________________________________________________________
void ServerMetrics::observe(opentelemetry::metrics::ObserverResult result,
                            int64_t value, const std::string& label,
                            const std::string& labelValue) {
  opentelemetry::nostd::get<opentelemetry::nostd::shared_ptr<
      opentelemetry::metrics::ObserverResultT<int64_t>>>(result)
      ->Observe(value, {{label, labelValue}});
}

// _____________________________________________________________________________
template <absl::AnyInvocable<int64_t() const> ServerMetrics::*Getter>
void ServerMetrics::observeCallback(
    opentelemetry::metrics::ObserverResult result, void* state) {
  const auto& self = *static_cast<const ServerMetrics*>(state);
  observe(std::move(result), (self.*Getter)());
}

// _____________________________________________________________________________
void ServerMetrics::deltaTriplesCallback(
    opentelemetry::metrics::ObserverResult result, void* state) {
  const auto& self = *static_cast<const ServerMetrics*>(state);
  auto deltaTriples = self.getDeltaTriples_();
  observe(result, deltaTriples.triplesInserted_, "type", "inserted");
  observe(result, deltaTriples.triplesDeleted_, "type", "deleted");
  observe(result, deltaTriples.triplesInserted_ + deltaTriples.triplesDeleted_);
}
