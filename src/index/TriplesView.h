#pragma once

#include "../global/Id.h"
#include "../util/File.h"
#include "../util/Generator.h"
#include "CompressedRelation.h"

/**
 * This allows iterating over one of the permutations of the index once.
 **/

cppcoro::generator<std::array<Id, 3>> TriplesView(const auto& permutation) {
  const auto& metaData = permutation._meta.data();
  for (auto it = metaData.ordered_begin(); it != metaData.ordered_end(); ++it) {
    uint64_t id = it.getId();
    std::vector<std::array<Id, 2>> buffer;
    CompressedRelationMetaData::scan(id, &buffer, permutation);
    for (const auto& [col1, col2] : buffer) {
      std::array<Id, 3> triple{id, col1, col2};
      co_yield triple;
    }
  }
}
/*
template <typename Permutation>
class TriplesView {
 private:
  const Permutation& permutation_;
  typename Permutation::MetaData::MapType::ConstOrderedIterator _iterator;
  typename Permutation::MetaData::MapType::ConstOrderedIterator _endIterator;

  // For an XYZ permutation, `_idPairs` is a vector of all YZ pairs for a fixed
  // X and `_index` is the index of a particular YZ pair.
  std::vector<std::array<Id, 2>> _idPairs;
  size_t _index;

 public:
  explicit TriplesView(const Permutation& permutation)
      : permutation_{permutation},
        _iterator(permutation._meta.data().ordered_begin()),
        _endIterator(permutation._meta.data().ordered_end()),
        _index(0) {
    scanCurrentPos();
  }

  cppcoro::generator<std::array<Id, 3>> generator(const auto& permutation) {
    const auto& metaData = permutation._meta.data();
    for (auto it = metaData.ordered_begin(); it != metaData.ordered_end; ++it) {
      uint64_t id = it.getId();
      std::vector<std::array<Id, 2>> buffer;
      CompressedRelationMetaData::scan(id, &buffer, permutation);
      for (const auto& [col1, col2] : buffer) {
        std::array<Id, 3> triple{id, col1, col2};
        co_yield triple;
      }
    }
  }

  // The following Methods give this class input iterator semantics
  struct IteratorEnd {};
  struct Iterator {
   private:
    TriplesView* _iterator;
    explicit Iterator(TriplesView* iterator) : _iterator{iterator} {}
    friend class TriplesView;

   public:
    decltype(auto) operator++() { return ++(*_iterator); }
    decltype(auto) operator*() { return **_iterator; }
    bool operator==(IteratorEnd) { return _iterator->empty(); }
  };
  Iterator begin() { return Iterator{this}; }
  IteratorEnd end() { return {}; }

 private:
  // prefix increment
  TriplesView& operator++() {
    if (empty()) {
      // don't do anything if we have already reached the end
      return *this;
    }
    ++_index;
    if (_index >= _idPairs.size()) {
      ++_iterator;
      if (empty()) {
        // don't do anything if we have already reached the end
        return *this;
      }
      scanCurrentPos();
      _index = 0;
    }
    return *this;
  }

  std::array<Id, 3> operator*() {
    return {_iterator.getId(), _idPairs[_index][0], _idPairs[_index][1]};
  }

  [[nodiscard]] bool empty() const { return _iterator == _endIterator; }

  void scanCurrentPos() {
    uint64_t id = _iterator.getId();
    CompressedRelationMetaData::scan(id, &_idPairs, permutation_);
  }
};
 */
