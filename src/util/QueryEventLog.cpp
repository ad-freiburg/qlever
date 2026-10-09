// Copyright 2026 The QLever Authors, in particular:
// 2026 Tanmay Garg <gargt@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/QueryEventLog.h"

#include "backports/filesystem.h"
#include "util/Exception.h"

namespace ad_utility {

// _____________________________________________________________________________
QueryEventLog::~QueryEventLog() {
  // Wake the writer (blocked in `pop()`) before its `JThread` is joined
  // by the implicit member destruction below.
  if (queue_) {
    queue_->finish();
  }
}

// _____________________________________________________________________________
void QueryEventLog::setOutputFile(const ql::filesystem::path& path) {
  AD_CONTRACT_CHECK(!configured_.exchange(true),
                    "QueryEventLog::setOutputFile may only be called once.");
  // Open stream and construct queue before spawning the writer; it
  // captures `this` and starts popping immediately.
  stream_.open(path.string(), std::ios::app | std::ios::out);
  AD_CONTRACT_CHECK(stream_.is_open(),
                    "QueryEventLog: failed to open output file.");
  queue_ = std::make_unique<data_structures::ThreadSafeQueue<std::string>>(
      maxQueueSize_);
  writer_ = JThread{[this] {
    // Terminate and flush each line so a live reader sees events promptly.
    while (auto line = queue_->pop()) {
      stream_ << *line << std::endl;
    }
  }};
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
