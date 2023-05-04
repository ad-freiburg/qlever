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

#include "global/Id.h"
#include "util/ExceptionHandling.h"
#include "util/File.h"
#include "util/Generator.h"
#include "util/Iterators.h"
#include "util/Serializer/FileSerializer.h"
#include "util/Serializer/SerializeVector.h"
#include "util/TypeTraits.h"
#include "util/UninitializedAllocator.h"

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

  using const_iterator = ad_utility::IteratorForAccessOperator<
      Pattern, ad_utility::AccessViaBracketOperator, ad_utility::IsConst::True>;

  const_iterator begin() const { return {this, 0}; }

  const_iterator end() const { return {this, size()}; }

  bool operator==(const Pattern& other) const = default;

  size_t size() const { return _data.size(); }

  void push_back(value_type i) { _data.push_back(i); }

  void clear() { _data.clear(); }

  const_ref back() const { return _data.back(); }
  ref back() { return _data.back(); }
  bool empty() const { return _data.empty(); }

  const value_type* data() const { return _data.data(); }

  std::vector<value_type> _data;
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
template <typename data_type>
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
    { *(t.begin()->begin()) } -> ad_utility::SimilarTo<data_type>;
  } void build(const T& input) {
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
  size_t size() const { return ready() ? _offsets.size() - 1 : 0; }

  bool ready() const { return !_offsets.empty(); }

  /**
   * @brief operator []
   * @param i
   * @return A std::pair containing a pointer to the data, and the number of
   *         elements stored at the pointers target.
   */
  const value_type operator[](size_t i) const {
    offset_type offset = _offsets[i];
    const data_type* ptr = _data.data() + offset;
    size_t size = _offsets[i + 1] - offset;
    return {ptr, size};
  }

  // Forward iterator for a `CompactVectorOfStrings` that reads directly from
  // disk without buffering the whole `Vector`.
  static cppcoro::generator<vector_type> diskIterator(string filename);

  using Iterator = ad_utility::IteratorForAccessOperator<
      CompactVectorOfStrings, ad_utility::AccessViaBracketOperator,
      ad_utility::IsConst::True>;

  Iterator begin() const { return {this, 0}; }
  Iterator end() const { return {this, size()}; }

  using const_iterator = Iterator;

  // Allow serialization via the ad_utility::serialization interface.
  AD_SERIALIZE_FRIEND_FUNCTION(CompactVectorOfStrings) {
    serializer | arg._data;
    serializer | arg._offsets;
  }

 private:
  std::vector<data_type> _data;
  std::vector<offset_type> _offsets;
};

namespace detail {
// Allows the incremental writing of a `CompactVectorOfStrings` directly to a
// file.
template <typename data_type>
struct CompactStringVectorWriter {
  ad_utility::File _file;
  off_t _startOfFile;
  using offset_type = typename CompactVectorOfStrings<data_type>::offset_type;
  std::vector<offset_type> _offsets;
  bool _finished = false;
  offset_type _nextOffset = 0;

  explicit CompactStringVectorWriter(const std::string& filename)
      : _file{filename, "w"} {
    commonInitialization();
  }

  explicit CompactStringVectorWriter(ad_utility::File&& file)
      : _file{std::move(file)} {
    commonInitialization();
  }

  void push(const data_type* data, size_t elementSize) {
    AD_CONTRACT_CHECK(!_finished);
    _offsets.push_back(_nextOffset);
    _nextOffset += elementSize;
    _file.write(data, elementSize * sizeof(data_type));
  }

  // Finish writing, and return the moved file. If the return value is
  // discarded, then the file will be closed immediately by the destructor of
  // the `File` class.
  ad_utility::File finish() {
    if (_finished) {
      return {};
    }
    _finished = true;
    _offsets.push_back(_nextOffset);
    _file.seek(_startOfFile, SEEK_SET);
    _file.write(&_nextOffset, sizeof(size_t));
    _file.seek(0, SEEK_END);
    ad_utility::serialization::FileWriteSerializer f{std::move(_file)};
    f << _offsets;
    return std::move(f).file();
  }

  ~CompactStringVectorWriter() {
    if (!_finished) {
      ad_utility::terminateIfThrows(
          [this]() { finish(); },
          "Finishing the underlying File of a `CompactStringVectorWriter` "
          "during destruction failed");
    }
  }

 private:
  // Has to be run by all the constructors
  void commonInitialization() {
    AD_CONTRACT_CHECK(_file.isOpen());
    // We don't known the data size yet.
    _startOfFile = _file.tell();
    size_t dataSizeDummy = 0;
    _file.write(&dataSizeDummy, sizeof(dataSizeDummy));
  }
};
}  // namespace detail

// Forward iterator for a `CompactVectorOfStrings` that reads directly from
// disk without buffering the whole `Vector`.
template <typename DataT>
cppcoro::generator<typename CompactVectorOfStrings<DataT>::vector_type>
CompactVectorOfStrings<DataT>::diskIterator(string filename) {
  ad_utility::File dataFile{filename, "r"};
  ad_utility::File indexFile{filename, "r"};
  AD_CORRECTNESS_CHECK(dataFile.isOpen());
  AD_CORRECTNESS_CHECK(indexFile.isOpen());

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
