// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#pragma once
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>
#include "../util/File.h"
#include "Id.h"

typedef uint32_t PatternID;

static const PatternID NO_PATTERN = std::numeric_limits<PatternID>::max();

/**
 * @brief This represents a set of relations of a single entity.
 *        (e.g. a set of books that all have an author and a title).
 *        This information can then be used to efficiently count the relations
 *        that a set of entities has (e.g. for autocompletion of relations
 *        while writing a query).
 */
struct Pattern {
  using value_type = Id;
  using ref = value_type&;
  using const_ref = const value_type&;

  ref operator[](const size_t pos) { return _data[pos]; }
  const_ref operator[](const size_t pos) const { return _data[pos]; }

  bool operator==(const Pattern& other) const {
    if (size() != other.size()) {
      return false;
    }
    for (size_t i = 0; i < size(); i++) {
      if (other._data[i] != _data[i]) {
        return false;
      }
    }
    return true;
  }

  bool operator!=(const Pattern& other) const {
    if (size() != other.size()) {
      return true;
    }
    for (size_t i = 0; i < size(); i++) {
      if (other._data[i] != _data[i]) {
        return true;
      }
    }
    return false;
  }

  bool operator<(const Pattern& other) const {
    if (size() == 0) {
      return true;
    }
    if (other.size() == 0) {
      return false;
    }
    return _data[0] < other._data[0];
  }

  bool operator>(const Pattern& other) const {
    if (other.size() == 0) {
      return true;
    }
    if (size() == 0) {
      return false;
    }
    return _data[0] > other._data[0];
  }

  size_t size() const { return _data.size(); }

  void push_back(const Id i) { _data.push_back(i); }

  void clear() { _data.clear(); }

  const_ref back() const { return _data.back(); }
  ref back() { return _data.back(); }
  bool empty() { return _data.empty(); }

  std::vector<Id> _data;
};

// The type of the index used to access the data, and the type of the data
// stored in the strings.
template <typename IndexT, typename DataT>
/**
 * @brief Stores a list of variable length data of a single type (e.g.
 *        c-style strings). The data is stored in a single contiguous block
 *        of memory.
 */
class CompactStringVector {
 public:
  CompactStringVector()
      : _data(nullptr), _size(0), _indexEnd(0), _dataSize(0) {}

  CompactStringVector(const std::vector<std::vector<DataT>>& data) {
    build(data);
  }

  CompactStringVector(ad_utility::File& file, off_t offset = 0) {
    load(file, offset);
  }

  virtual ~CompactStringVector() { delete[] _data; }

  /**
   * @brief Fills this CompactStringVector with data.
   * @param The data from which to build the vector.
   */
  void build(const std::vector<std::vector<DataT>>& data) {
    _size = data.size();
    _indexEnd = (_size + 1) * sizeof(IndexT);
    size_t dataCount = 0;
    for (size_t i = 0; i < _size; i++) {
      dataCount += data[i].size();
    }
    if (dataCount > std::numeric_limits<IndexT>::max()) {
      throw std::runtime_error(
          "To much data for index type. (" + std::to_string(dataCount) + " > " +
          std::to_string(std::numeric_limits<IndexT>::max()));
    }
    _dataSize = _indexEnd + sizeof(DataT) * dataCount;
    _data = new uint8_t[_dataSize];
    IndexT currentLength = 0;
    size_t indPos = 0;
    for (IndexT i = 0; i < _size; i++) {
      // add an entry to the index
      std::memcpy(_data + (indPos * sizeof(IndexT)), &currentLength,
                  sizeof(IndexT));
      // copy the vectors actual data
      std::memcpy(_data + (_indexEnd + currentLength * sizeof(DataT)),
                  data[i].data(), data[i].size() * sizeof(DataT));
      indPos++;
      currentLength += data[i].size();
    }
    // add a final entry that stores the end of the data field
    std::memcpy(_data + (indPos * sizeof(IndexT)), &currentLength,
                sizeof(IndexT));
  }

  void load(ad_utility::File& file, off_t offset = 0) {
    file.read(&_size, sizeof(size_t), offset);
    file.read(&_dataSize, sizeof(size_t), offset + sizeof(size_t));
    _indexEnd = (_size + 1) * sizeof(IndexT);
    _data = new uint8_t[_dataSize];
    file.read(_data, _dataSize, offset + 2 * sizeof(size_t));
  }

  CompactStringVector& operator=(const CompactStringVector&) = delete;

  size_t size() const { return _size; }

  /**
   * @brief Stores the vector in the file at the current seek position.
   * @param The file to write into
   * @return The number of bytes written.
   */
  size_t write(ad_utility::File& file) {
    file.write(&_size, sizeof(size_t));
    file.write(&_dataSize, sizeof(size_t));
    file.write(_data, _dataSize);
    return _dataSize + 2 * sizeof(size_t);
  }

  bool ready() const { return _data != nullptr; }

  /**
   * @brief operator []
   * @param i
   * @return A std::pair containing a pointer to the data, and the number of
   *         elements stored at the pointers target.
   */
  const std::pair<DataT*, size_t> operator[](size_t i) const {
    IndexT ind, nextInd;
    std::memcpy(&ind, _data + (i * sizeof(IndexT)), sizeof(IndexT));
    std::memcpy(&nextInd, _data + ((i + 1) * sizeof(IndexT)), sizeof(IndexT));
    return std::pair<DataT*, size_t>(
        reinterpret_cast<DataT*>(_data + (_indexEnd + sizeof(DataT) * ind)),
        nextInd - ind);
  }

 private:
  uint8_t* _data;
  size_t _size;
  size_t _indexEnd;
  size_t _dataSize;
};

namespace std {
template <>
struct hash<Pattern> {
  std::size_t operator()(const Pattern& p) const {
    std::string_view s = std::string_view(
        reinterpret_cast<const char*>(p._data.data()), sizeof(Id) * p.size());
    return hash<std::string_view>()(s);
  }
};
}  // namespace std

inline std::ostream& operator<<(std::ostream& o, const Pattern& p) {
  for (size_t i = 0; i + 1 < p.size(); i++) {
    o << p[i] << ", ";
  }
  if (p.size() > 0) {
    o << p[p.size() - 1];
  }
  return o;
}
