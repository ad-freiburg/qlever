//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_ONDISKVECTOR_H
#define QLEVER_ONDISKVECTOR_H

#include <stxxl/vector>
#include <foxxll/io/syscall_file.hpp>
#include <foxxll/io/file.hpp>
#include "./Log.h"


namespace ad_utility {

// 2 tags to differentiate between different versions of the
// setup / construction variants of Mmap Vector which only take a filename
class CreateTag {};
class ReuseTag {};

// Enum that specifies access patterns to this array
enum class AccessPattern { None, Random, Sequential };
template <typename T>
class OnDiskVector : public stxxl::vector<T> {
 private:
  using Base = stxxl::vector<T>;
  std::string _filename;
 private:
  explicit OnDiskVector(const std::string& filename, int mode)
      : Base{foxxll::file_ptr{new foxxll::syscall_file{filename,
                                   mode}}}, _filename(filename)
        {
    LOG(INFO) << "Constructing OnDiskVector from " << filename << std::endl;
  }

 public:
  // TODO: this should be deleted, but requires some refactorings.
  OnDiskVector() {
      LOG(INFO) << "Default constructor of OnDiskVector" << std::endl;
  };
  OnDiskVector(const std::string& filename, ReuseTag, AccessPattern = AccessPattern::None) :OnDiskVector{filename, foxxll::file::RDWR} {}
  OnDiskVector(const std::string& filename, CreateTag, AccessPattern = AccessPattern::None) :OnDiskVector{filename, foxxll::file::TRUNC | foxxll::file::RDWR | foxxll::file::CREAT} {}

  // create Array of given size  fill with default value
  // file contents will be overriden if existing!
  // allows read and write access
  OnDiskVector(size_t size, const T& defaultValue, string filename,
            AccessPattern = AccessPattern::None) : OnDiskVector(filename, CreateTag{}) {
    this->reserve(size);
    // TODO<joka921> use Buffered....
    for (size_t i = 0; i < size; ++i) {
      this->push_back(defaultValue);
    }
  }

  static OnDiskVector createEmptyNonPersistent() {
    return OnDiskVector();
  }
  static OnDiskVector create (const std::string& filename) {
    return OnDiskVector{filename, foxxll::file::TRUNC | foxxll::file::RDWR | foxxll::file::CREAT};
  }
  static OnDiskVector reuse(const std::string& filename) {
    return OnDiskVector{filename, foxxll::file::RDWR};
  }

  // TODO:: enforce the "readOnlyness"
  static OnDiskVector reuseReadOnly(const std::string& filename) {
    return OnDiskVector{filename, foxxll::file::RDWR};
  }
  ~OnDiskVector() {
    LOG(INFO) << "Destroying an OnDiskVector of size: " << this->size() << " and file " << _filename << std::endl;
  }
};
}

#endif  // QLEVER_ONDISKVECTOR_H
