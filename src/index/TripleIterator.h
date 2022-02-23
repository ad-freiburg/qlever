#pragma once

#include "../global/Id.h"
#include "../util/File.h"
#include "CompressedRelation.h"

/**
 * This allows iterating over one of the permutations of the index once.
 **/
template <typename Permutation>
class TripleIterator {
 private:
  const Permutation& permutation_;
  typename Permutation::MetaData::MapType::ConstOrderedIterator _iterator;
  typename Permutation::MetaData::MapType::ConstOrderedIterator _endIterator;

  // For an XYZ permutation, `_idPairs` is a vector of all YZ pairs for a fixed
  // X and `_index` is the index of a particular YZ pair.
  std::vector<std::array<Id, 2>> _idPairs;
  size_t _index;

 public:
  explicit TripleIterator(const Permutation& permutation)
      : permutation_{permutation},
        _iterator(permutation._meta.data().ordered_begin()),
        _endIterator(permutation._meta.data().ordered_end()),
        _index(0) {
    scanCurrentPos();
  }

  // The following Methods give this class input iterator semantics
  struct IteratorSentinel {};
  struct Iterator {
   private:
    TripleIterator* _iterator;
    explicit Iterator(TripleIterator* iterator) : _iterator{iterator} {}
    friend class TripleIterator;

   public:
    decltype(auto) operator++() { return ++(*_iterator); }
    decltype(auto) operator*() { return **_iterator; }
    bool operator==(IteratorSentinel) { return _iterator->empty(); }
  };
  Iterator begin() { return Iterator{this}; }
  IteratorSentinel end() { return {}; }

 private:
  // prefix increment
  TripleIterator& operator++() {
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
