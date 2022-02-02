// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#pragma once
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

#include "../util/File.h"
#include "../util/Generator.h"
#include "../util/Iterators.h"
#include "../util/Serializer/FileSerializer.h"
#include "../util/Serializer/SerializeVector.h"
#include "../util/TypeTraits.h"
#include "../util/UninitializedAllocator.h"
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

namespace detail {
template <typename DataT>
struct CompactStringVectorWriter;

}

/**
 * @brief Stores a list of variable length data of a single type (e.g.
 *        c-style strings). The data is stored in a single contiguous block
 *        of memory.
 */
template <typename data_type, typename index_type = uint64_t,
          typename difference_type = int64_t>
class CompactVectorOfStrings {
 public:
  using offset_type = uint64_t;
  using value_type =
      std::conditional_t<std::is_same_v<data_type, char>, std::string_view,
                         std::span<const data_type>>;
  using vector_type = std::conditional_t<std::is_same_v<data_type, char>,
                                         std::string, std::vector<data_type>>;

  using Writer = detail::CompactStringVectorWriter<data_type>;
  CompactVectorOfStrings() = default;

  explicit CompactVectorOfStrings(
      const std::vector<std::vector<data_type>>& input) {
    build(input);
  }

  void clear() { *this = CompactVectorOfStrings{}; }

  virtual ~CompactVectorOfStrings() = default;

  /**
   * @brief Fills this CompactVectorOfStrings with input.
   * @param The input from which to build the vector.
   */
  template <typename T>
  requires requires(T t) {
    { *(t.begin()->begin()) }
    ->ad_utility::SimilarTo<data_type>;
  }
  void build(const T& input) {
    // Also make room for the end offset of the last element.
    _offsets.reserve(input.size() + 1);
    size_t dataSize = 0;
    for (const auto& element : input) {
      _offsets.push_back(dataSize);
      dataSize += element.size();
    }
    // The last offset is the offset right after the last element.
    _offsets.push_back(dataSize);

    _data.reserve(dataSize);

    for (const auto& el : input) {
      _data.insert(_data.end(), el.begin(), el.end());
    }
  }

  // This is a move-only type.
  CompactVectorOfStrings& operator=(const CompactVectorOfStrings&) = delete;
  CompactVectorOfStrings& operator=(CompactVectorOfStrings&&) noexcept =
      default;
  CompactVectorOfStrings(const CompactVectorOfStrings&) = delete;
  CompactVectorOfStrings(CompactVectorOfStrings&&) noexcept = default;

  // There is one more offset than the number of elements.
  index_type size() const { return index_type{_offsets.size() - 1}; }

  bool ready() const { return _data != nullptr; }

  /**
   * @brief operator []
   * @param i
   * @return A std::pair containing a pointer to the data, and the number of
   *         elements stored at the pointers target.
   */
  const value_type operator[](index_type index) const {
    auto i = indexTypeToInt(index);
    offset_type offset = _offsets[i];
    const data_type* ptr = _data.data() + offset;
    size_t size = _offsets[i + 1] - offset;
    return {ptr, size};
  }

  // Forward iterator for a `CompactVectorOfStrings` that reads directly from
  // disk without buffering the whole `Vector`.
  static cppcoro::generator<vector_type> diskIterator(string filename);

  using Iterator = ad_utility::IteratorForAccessOperator<
      CompactVectorOfStrings, ad_utility::AccessViaBracketOperator, index_type,
      difference_type>;

  Iterator begin() const { return {this, index_type{0}}; }
  Iterator end() const { return {this, size()}; }

  using const_iterator = Iterator;

  using Accessor = decltype([](auto&& compactVector, auto index) {
    return compactVector[index_type{index}];
  });
  using StlConformingIterator =
      ad_utility::IteratorForAccessOperator<CompactVectorOfStrings, Accessor>;
  StlConformingIterator stlBegin() const { return {this, 0}; }

  StlConformingIterator stlEnd() const {
    return {this, indexTypeToInt(size())};
  }

  index_type upper_bound(auto&& needle, auto&& comparator) const {
    auto it = std::upper_bound(stlBegin(), stlEnd(), needle, comparator);
    return index_type(it - stlBegin());
  }

  index_type lower_bound(auto&& needle, auto&& comparator) const {
    auto it = std::lower_bound(stlBegin(), stlEnd(), needle, comparator);
    return index_type(it - stlBegin());
  }

  // Allow serialization via the ad_utility::serialization interface.
  template <typename Serializer>
  friend void serialize(Serializer& s, CompactVectorOfStrings& c) {
    s | c._data;
    s | c._offsets;
    ;
  }

 private:
  static auto indexTypeToInt(index_type t) {
    // TODO<joka921> proper requirements.
    if constexpr (requires(index_type tt) { t.get(); }) {
      return t.get();
    } else {
      return t;
    }
  }
  std::vector<data_type> _data;
  std::vector<offset_type> _offsets;
};

namespace detail {
// Allows the incremental writing of a `CompactVectorOfStrings` directly to a
// file.
template <typename data_type>
struct CompactStringVectorWriter {
  ad_utility::File _file;
  using offset_type = typename CompactVectorOfStrings<data_type>::offset_type;
  std::vector<offset_type> _offsets;
  bool _finished = false;
  offset_type _nextOffset = 0;
  explicit CompactStringVectorWriter(const std::string& filename)
      : _file{filename, "w"} {
    AD_CHECK(_file.isOpen());
    // We don't known the data size yet.
    size_t dataSizeDummy = 0;
    _file.write(&dataSizeDummy, sizeof(dataSizeDummy));
  }

  void push(const data_type* data, size_t elementSize) {
    AD_CHECK(!_finished);
    _offsets.push_back(_nextOffset);
    _nextOffset += elementSize;
    _file.write(data, elementSize * sizeof(data_type));
  }

  void finish() {
    _offsets.push_back(_nextOffset);
    _file.seek(0, SEEK_SET);
    _file.write(&_nextOffset, sizeof(size_t));
    _file.seek(0, SEEK_END);
    ad_utility::serialization::FileWriteSerializer f{std::move(_file)};
    f << _offsets;
    _finished = true;
  }

  ~CompactStringVectorWriter() {
    if (!_finished) {
      finish();
    }
  }
};
}  // namespace detail

// Forward iterator for a `CompactVectorOfStrings` that reads directly from
// disk without buffering the whole `Vector`.
template <typename DataT, typename IndexT, typename DifferenceT>
cppcoro::generator<
    typename CompactVectorOfStrings<DataT, IndexT, DifferenceT>::vector_type>
CompactVectorOfStrings<DataT, IndexT, DifferenceT>::diskIterator(
    string filename) {
  ad_utility::File dataFile{filename, "r"};
  ad_utility::File indexFile{filename, "r"};
  AD_CHECK(dataFile.isOpen());
  AD_CHECK(indexFile.isOpen());

  const size_t dataSizeInBytes = [&]() {
    size_t dataSize;
    dataFile.read(&dataSize, sizeof(dataSize));
    return dataSize * sizeof(DataT);
  }();

  indexFile.seek(sizeof(dataSizeInBytes) + dataSizeInBytes, SEEK_SET);
  size_t size;
  indexFile.read(&size, sizeof(size));

  size--;  // There is one more offset than the number of elements.

  size_t offset;
  indexFile.read(&offset, sizeof(offset));

  for (size_t i = 0; i < size; ++i) {
    size_t nextOffset;
    indexFile.read(&nextOffset, sizeof(nextOffset));
    auto currentSize = nextOffset - offset;
    vector_type result;
    result.resize(currentSize);
    dataFile.read(result.data(), currentSize * sizeof(DataT));
    co_yield result;
    offset = nextOffset;
  }
}

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
