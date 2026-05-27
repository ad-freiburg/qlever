// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Tanmay Garg (gargt@cs.uni-freiburg.de)

#ifndef QLEVER_SRC_UTIL_QUERYEVENTLOG_H
#define QLEVER_SRC_UTIL_QUERYEVENTLOG_H

#include <atomic>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>

#include "util/ThreadSafeQueue.h"
#include "util/jthread.h"

namespace ad_utility {

// Process-wide append-only sink for per-query start/end events.
// Producers enqueue fully-formatted JSONL lines via `push`; one
// background thread drains the queue and flushes after each line.
// `push` is a silent no-op until `setOutputFile` has been called.
class QueryEventLog {
 public:
  // Bound on the internal queue. `push` blocks when the queue is full.
  static constexpr size_t maxQueueSize_ = 100'000;

  // Singleton accessor for production callers. Tests construct their own
  // local instance via the public default constructor.
  static QueryEventLog& instance();

  QueryEventLog() = default;
  ~QueryEventLog();

  QueryEventLog(const QueryEventLog&) = delete;
  QueryEventLog& operator=(const QueryEventLog&) = delete;

  // Open `path` in append mode and spawn the writer thread. Must be
  // called at most once per instance; a second call throws.
  void setOutputFile(const std::filesystem::path& path);

  // Enqueue one already-formatted line. The caller must include the
  // trailing newline. No-op if the sink has not been configured.
  void push(std::string line);

 private:
  // Declaration order is load-bearing: on teardown the writer must
  // join (after `~QueryEventLog` calls `queue_->finish()`) before the
  // queue and stream are destroyed.
  std::ofstream stream_;
  std::atomic<bool> configured_{false};
  std::unique_ptr<data_structures::ThreadSafeQueue<std::string>> queue_;
  JThread writer_;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_QUERYEVENTLOG_H
