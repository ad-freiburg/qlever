#pragma once

#include "../global/Id.h"
#include "../util/File.h"
#include "CompressedRelation.h"

/**
 * This allows iterating over one of the permutations of the index once.
 **/
template <typename Permutation>
class MetaDataIterator {
 public:
  MetaDataIterator(const Permutation& permutation)
      : permutation_{permutation},
        _iterator(permutation._meta.data().ordered_begin()),
        _endIterator(permutation._meta.data().ordered_end()),
        _buffer_offset(0) {
    scanCurrentPos();
  }

  // prefix increment
  MetaDataIterator& operator++() {
    if (empty()) {
      // don't do anything if we have already reached the end
      return *this;
    }
    ++_buffer_offset;
    if (_buffer_offset >= _buffer.size()) {
      ++_iterator;
      if (empty()) {
        // don't do anything if we have already reached the end
        return *this;
      }
      scanCurrentPos();
      _buffer_offset = 0;
    }
    return *this;
  }

  std::array<Id, 3> operator*() {
    return {_iterator->first, _buffer[_buffer_offset][0],
            _buffer[_buffer_offset][1]};
  }

  bool empty() const { return _iterator == _endIterator; }

 private:
  void scanCurrentPos() {
    auto id = _iterator->first;
    CompressedRelationMetaData::scan(id, &_buffer, permutation_);
  }

  const Permutation& permutation_;
  typename Permutation::MetaData::MapType::ConstOrderedIterator _iterator;
  typename Permutation::MetaData::MapType::ConstOrderedIterator _endIterator;

  // This buffers the results of the scans we need to use to read the relations
  std::vector<std::array<Id, 2>> _buffer;
  size_t _buffer_offset;
};
