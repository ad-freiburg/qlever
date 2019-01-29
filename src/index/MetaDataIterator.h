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
        _iterator(meta.data().begin()),
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
      scanCurrentPos();
    }
    return *this;
  }

  std::array<Id, 3> operator*() {
    return {_iterator->first, _buffer[_buffer_offset][0],
            _buffer[_buffer_offset][1]};
  }

  bool empty() { return _iterator == meta_.data().end(); }

 private:
  void scanCurrentPos() {
    const FullRelationMetaData& rmd = _iterator->second.get();
    _buffer.reserve(rmd.getNofElements() + 2);
    _buffer.resize(rmd.getNofElements());
    _file.read(_buffer.data(), rmd.getNofElements() * 2 * sizeof(Id),
               rmd._startFullIndex);
  }

  const MetaDataType& meta_;
  typename MetaDataType::MapType::Iterator _iterator;

  // This buffers the results of the scans we need to use to read the relations
  std::vector<std::array<Id, 2>> _buffer;
  size_t _buffer_offset;

  ad_utility::File _file;
};
