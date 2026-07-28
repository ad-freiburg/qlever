//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_EXTERNALOVERFLOWSTORAGE_H
#define QLEVER_SRC_UTIL_EXTERNALOVERFLOWSTORAGE_H

#include <optional>
#include <string>
#include <vector>

#include "util/File.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/Serializer.h"
#include "util/TypeTraits.h"

namespace ad_utility {

// A minimal, append-only storage for elements of type `T`. The first
// `threshold` elements are kept in memory, all further elements are written to
// a temporary file on disk via a `FileWriteSerializer`. `T` must be
// serializable using the `ad_utility::serialization` framework. The temporary
// file is deleted when the storage is destroyed.
template <typename T>
class ExternalOverflowStorage {
 private:
  // The maximal number of elements that are kept in memory.
  size_t threshold_;
  // The name of the temporary file to which the overflow is written.
  std::string filename_;
  // The elements that are kept in memory (at most `threshold_` many).
  std::vector<T> buffer_;
  // The serializer for the overflow elements. It is created lazily as soon as
  // the first overflow element is pushed.
  std::optional<serialization::FileWriteSerializer> writer_;
  // The number of elements that have been written to the overflow file.
  size_t numOverflowElements_ = 0;
  // Whether the temporary file has been created (and thus has to be deleted on
  // destruction).
  bool fileWasCreated_ = false;

 public:
  // Create the storage. At most `threshold` elements are kept in memory, all
  // further elements are written to a temporary file called `filename`.
  ExternalOverflowStorage(size_t threshold, std::string filename)
      : threshold_{threshold}, filename_{std::move(filename)} {}

  // The storage owns a temporary file, so it can neither be copied nor moved.
  ExternalOverflowStorage(const ExternalOverflowStorage&) = delete;
  ExternalOverflowStorage& operator=(const ExternalOverflowStorage&) = delete;
  ExternalOverflowStorage(ExternalOverflowStorage&&) = delete;
  ExternalOverflowStorage& operator=(ExternalOverflowStorage&&) = delete;

  // Delete the temporary file (if it was ever created).
  ~ExternalOverflowStorage() {
    if (fileWasCreated_) {
      writer_.reset();
      deleteFile(filename_);
    }
  }

  // Append an element. As long as fewer than `threshold` elements are stored
  // the element is kept in memory, afterwards it is written to disk.
  void push_back(const T& element) {
    if (buffer_.size() < threshold_) {
      buffer_.push_back(element);
      return;
    }
    if (!writer_.has_value()) {
      // Open (and truncate) the file. The writer stays open until `clear` is
      // called, in particular it survives a `forEach` (which only flushes and
      // reads via a separate handle), so the file is opened exactly once per
      // `clear` cycle and there is never any stale overflow on disk.
      writer_.emplace(filename_);
      fileWasCreated_ = true;
    }
    writer_.value() << element;
    ++numOverflowElements_;
  }

  // Call `function` for each stored element, in insertion order.
  CPP_template(typename F)(requires ad_utility::InvocableWithExactReturnType<
                           F, void, const T&>) void forEach(F function) {
    ql::ranges::for_each(std::as_const(buffer_), function);
    if (numOverflowElements_ == 0) {
      return;
    }
    AD_CORRECTNESS_CHECK(writer_.has_value());
    // Make sure that everything that has been written is visible to the
    // read handle that we open below.
    writer_.value().flush();
    serialization::FileReadSerializer reader{filename_};
    for (size_t i = 0; i < numOverflowElements_; ++i) {
      T element;
      reader >> element;
      std::invoke(function, std::as_const(element));
    }
  }

  // Remove all elements. The on-disk overflow is discarded as well (the file
  // will be truncated the next time an element overflows to disk).
  void clear() {
    buffer_.clear();
    writer_.reset();
    numOverflowElements_ = 0;
  }
};
}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_EXTERNALOVERFLOWSTORAGE_H
