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
  // Wake the writer (blocked in `pop()`) before its `JThread` is joined
  // by the implicit member destruction below.
  if (queue_) {
    queue_->finish();
  }
}

// _____________________________________________________________________________
void QueryEventLog::setOutputFile(const std::filesystem::path& path) {
  AD_CONTRACT_CHECK(!configured_.load(),
                    "QueryEventLog::setOutputFile may only be called once.");
  // Open stream and construct queue before spawning the writer; it
  // captures `this` and starts popping immediately.
  stream_.open(path, std::ios::app | std::ios::out);
  AD_CONTRACT_CHECK(stream_.is_open(),
                    "QueryEventLog: failed to open output file.");
  queue_ = std::make_unique<data_structures::ThreadSafeQueue<std::string>>(
      maxQueueSize_);
  writer_ = JThread{[this] {
    // Flush per line so live readers see events promptly; `flush()` is
    // not `fsync`, so the cost is negligible.
    while (auto line = queue_->pop()) {
      stream_ << *line;
      stream_.flush();
    }
  }};
  // Publish last so `push` only sees a fully set-up sink.
  configured_.store(true);
}

// _____________________________________________________________________________
void QueryEventLog::push(std::string line) {
  if (!configured_.load()) {
    return;
  }
  // Blocks if the queue is full.
  queue_->push(std::move(line));
}

}  // namespace ad_utility
