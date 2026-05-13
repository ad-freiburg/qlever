// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Tanmay Garg (gargt@cs.uni-freiburg.de)

#include "util/QueryEventLog.h"

#include "util/Exception.h"

namespace ad_utility {

// _____________________________________________________________________________
QueryEventLog& QueryEventLog::instance() {
  static QueryEventLog instance;
  return instance;
}

// _____________________________________________________________________________
QueryEventLog::~QueryEventLog() {
  // Wake the writer with `nullopt` so it exits its `pop()` loop. This
  // must happen before the `JThread` member is destroyed, otherwise the
  // implicit `join()` would block forever on a writer that is itself
  // blocked inside `queue_->pop()`. After this body returns, members
  // destruct in reverse declaration order: `writer_` joins, then
  // `queue_` is freed, then `stream_` closes.
  if (queue_) {
    queue_->finish();
  }
}

// _____________________________________________________________________________
void QueryEventLog::setOutputFile(const std::filesystem::path& path) {
  AD_CONTRACT_CHECK(!configured_.load(),
                    "QueryEventLog::setOutputFile may only be called once.");
  // Open the stream before constructing the queue and spawning the
  // writer: the writer thread captures `this` and immediately starts
  // popping, so every member it touches must be live at spawn time.
  stream_.open(path, std::ios::app | std::ios::out);
  AD_CONTRACT_CHECK(stream_.is_open(),
                    "QueryEventLog: failed to open output file.");
  queue_ = std::make_unique<data_structures::ThreadSafeQueue<std::string>>(
      maxQueueSize_);
  writer_ = JThread{[this] {
    // Flush after every line so live consumers see new events at low
    // event rates. `flush()` only hands the buffer to the kernel (not
    // an `fsync`), so the throughput cost is negligible. `pop()`
    // returns `nullopt` after `finish()`, which is our shutdown signal.
    while (auto line = queue_->pop()) {
      stream_ << *line;
      stream_.flush();
    }
  }};
  // Publish `configured_` last so `push` only observes a usable state
  // after the queue and writer are fully set up.
  configured_.store(true);
}

// _____________________________________________________________________________
void QueryEventLog::push(std::string line) {
  // No-op when the sink is unconfigured. This keeps test binaries and
  // code paths that never call `setOutputFile` working unchanged.
  if (!configured_.load()) {
    return;
  }
  // `ThreadSafeQueue::push` blocks on a full queue. Producers contend
  // only on the queue's internal mutex; they never touch the file.
  queue_->push(std::move(line));
}

}  // namespace ad_utility
