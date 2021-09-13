#pragma once

#include "../global/Id.h"
#include "../util/File.h"

/**
 * This allows iterating over one of the permutations of the index once.
 **/
template <typename MetaDataType>
class MetaDataIterator {
 public:
  MetaDataIterator(const MetaDataType& meta, ad_utility::File file)
      : meta_(meta),
        _iterator(meta.data().ordered_begin()),
        _endIterator(meta.data().ordered_end()),
        _buffer_offset(0),
        _file(file) {
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
    FullRelationMetaData rmd;
    if constexpr (requires { _iterator->second.get(); }) {
      rmd = _iterator->second.get();
    } else {
      rmd = _iterator->second;
    }
    _buffer.resize(rmd.getNofElements());
    _file.read(_buffer.data(), rmd.getNofElements() * 2 * sizeof(Id),
               rmd._startFullIndex);
  }

  const MetaDataType& meta_;
  typename MetaDataType::MapType::ConstOrderedIterator _iterator;
  const typename MetaDataType::MapType::ConstOrderedIterator _endIterator;

  // This buffers the results of the scans we need to use to read the relations
  std::vector<std::array<Id, 2>> _buffer;
  size_t _buffer_offset;

  ad_utility::File _file;
};
