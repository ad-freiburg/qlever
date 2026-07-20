// Copyright 2026 The QLever Authors, in particular:
// 2026 Tanmay Garg <gargt@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_QUERYEVENTLOG_H
#define QLEVER_SRC_UTIL_QUERYEVENTLOG_H

#include <atomic>
#include <fstream>
#include <memory>
#include <string>

#include "backports/filesystem.h"
#include "util/ThreadSafeQueue.h"
#include "util/jthread.h"

namespace ad_utility {

// Append-only sink for per-query start/end events.
// Producers enqueue fully-formatted JSONL lines via `push`; one
// background thread drains the queue and flushes after each line.
// `push` is a silent no-op until `setOutputFile` has been called.
class QueryEventLog {
 public:
  // Bound on the internal queue. `push` blocks when the queue is full.
  // Lines are flushed immediately, so this only absorbs transient bursts.
  static constexpr size_t maxQueueSize_ = 1'000;

  QueryEventLog() = default;
  ~QueryEventLog();

  QueryEventLog(const QueryEventLog&) = delete;
  QueryEventLog& operator=(const QueryEventLog&) = delete;

  // Open `path` in append mode and spawn the writer thread. Must be
  // called at most once per instance; a second call throws.
  void setOutputFile(const ql::filesystem::path& path);

  // Enqueue one already-formatted line; the writer appends the trailing
  // newline. No-op if the sink has not been configured.
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
