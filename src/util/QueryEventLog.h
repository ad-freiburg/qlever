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

// Process-wide append-only sink for per-query start/end events. Producers
// enqueue a fully-formatted JSONL line (including the trailing newline) via
// `push`; one background thread drains the bounded queue and writes to the
// configured file, flushing after each line so live consumers see new
// events promptly.
//
// Until `setOutputFile` has been called the sink is inactive and `push` is
// a silent no-op, so test binaries and code paths that never configure a
// file keep working unchanged.
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
  // Declaration order matters: on destruction members are torn down in
  // reverse, so `writer_` joins first, then the queue is destroyed,
  // then the file stream closes. The explicit destructor first calls
  // `queue_->finish()` so the writer wakes on `nullopt` and exits
  // cleanly before its `JThread` is joined.
  std::ofstream stream_;
  std::atomic<bool> configured_{false};
  std::unique_ptr<data_structures::ThreadSafeQueue<std::string>> queue_;
  JThread writer_;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_QUERYEVENTLOG_H
