//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_CACHINGMEMORYRESOURCE_H
#define QLEVER_SRC_UTIL_CACHINGMEMORYRESOURCE_H

#include "backports/memory_resource.h"
#include "util/HashMap.h"

namespace ad_utility {

// A memory resource that inherits from `ql::pmr::memory_resource` with the
// following properties:
// 1. It is threadsafe
// 2. It internally holds a cache of previously allocated blocks. Allocation
//    requests are served from the cache if possible, else from the default
//    resource. Deallocated blocks are inserted into the cache.
// 3. Only when the memory resource is destroyed the cache is cleared and the
// contained blocks are actually deallocated.
// 4. The cache doesn't perform any kind of pooling for blocks with similar
//    sizes or alignments, but is only able to reuse memory if the size and
//    alignment match exactly.
// This class can be used when we know that blocks of the same size will be
// reused over and over again. For example in `IndexBuilderTypes.h` we use it
// for multiple hash maps that all reserve the same amount of space. A simpler
// approach would be to reuse the hash maps directly, but once this is not
// feasible, this allocator can come in handy.
class CachingMemoryResource : public ql::pmr::memory_resource {
 private:
  ql::pmr::memory_resource* allocator_ = ql::pmr::get_default_resource();
  ad_utility::HashMap<std::pair<size_t, size_t>, std::vector<void*>> cache_;
  std::mutex mutex_;

  // Allocation: Find suitable block in the cache, or allocate a new block.
  void* do_allocate(std::size_t bytes, std::size_t alignment) override {
    std::lock_guard l{mutex_};
    auto it = cache_.find(std::pair{bytes, alignment});
    if (it == cache_.end() || it->second.empty()) {
      auto res = allocator_->allocate(bytes, alignment);
      return res;
    }
    auto& c = it->second;
    auto res = c.back();
    c.pop_back();
    return res;
  }

  // Deallocation: add the block to the cache.
  void do_deallocate(void* p, std::size_t bytes,
                     std::size_t alignment) override {
    std::lock_guard l{mutex_};
    cache_[std::pair{bytes, alignment}].push_back(p);
  }

  // Equality implementation.
  bool do_is_equal(
      const ql::pmr::memory_resource& other) const noexcept override {
    return this == &other;
  }

 public:
  // Destructor: actually deallocate the blocks in the cache.
  ~CachingMemoryResource() override {
    for (const auto& [key, pointers] : cache_) {
      for (auto ptr : pointers) {
        allocator_->deallocate(ptr, key.first, key.second);
      }
    }
  }
};
}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_CACHINGMEMORYRESOURCE_H
