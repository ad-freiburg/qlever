// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#ifndef SERVER_LIST_H_
#define SERVER_LIST_H_

#include <vector>
#include <string>
#include <sstream>
#include <algorithm>
#include "./Globals.h"
#include "./Identifiers.h"
#include "./ListIdProvider.h"

using std::vector;
using std::string;

namespace ad_semsearch {
// List representing a raw list of postings from the full-text index.
template<class ListElement>
class List {
  public:

    List()
    : _listId(ListIdProvider::__nextListId++) {
    }

    virtual ~List() {
    }

    List<ListElement>& operator=(const List<ListElement>& rhs) {
      _data = rhs._data;
      return *this;
    }

    typedef typename std::vector<ListElement>::iterator iterator;
    typedef typename std::vector<ListElement>::const_iterator const_iterator;

    //! Iterator to the start
    iterator begin() {
      return _data.begin();
    }

    //! Iterator to the start, const
    const_iterator begin() const {
      return _data.begin();
    }

    //! Iterator to end()
    iterator end() {
      return _data.end();
    }

    //! Iterator to end(), const
    const_iterator end() const {
      return _data.end();
    }

    //! operator[] just like for std::vector, const
    const ListElement& operator[](size_t i) const {
      return _data[i];
    }

    //! operator[] just like for std::vector
    ListElement& operator[](size_t i) {
      return _data[i];
    }

    //! Append to the end of the list
    void push_back(const ListElement& elem) {
      _data.push_back(elem);
    }

    //! Get the last element
    const ListElement& back() const {
      return _data.back();
    }

    //! Get the last element, non const
    ListElement& back() {
      return _data.back();
    }

    //! Current size of the list
    size_t size() const {
      return _data.size();
    }

    //! Resize just like for std::vector
    void resize(size_t newSize, ListElement defValue = ListElement()) {
      _data.resize(newSize, defValue);
    }

    //! Clear just like for std::vector
    void clear() {
      _data.clear();
    }

    //! Reserve method like for std::vector.
    void reserve(size_t n) {
      _data.reserve(n);
    }

    //! Inserts the elements into the list.
    void insert(iterator position, const_iterator first,
        const_iterator exclusiveLast) {
      _data.insert(position, first, exclusiveLast);
    }

    template<class InputIterator>
    void assign(InputIterator first, InputIterator last) {
      _data.assign(first, last);
    }

    //! Get a string representation
    virtual string asString() const {
      std::ostringstream os;
      size_t upperBound = std::min(_data.size(), size_t(3));
      os << "[";
      for (size_t i = 0; i < upperBound; ++i) {
        if (i > 0) os << ", ";
        ListElement elem = _data[i];
        os << elem.asString();
      }
      if (_data.size() > 3) os << ", ...";
      os << "]";
      return os.str();
    }

    size_t getListId() const {
      return _listId;
    }

  protected:
    vector<ListElement> _data;
    size_t _listId;
};
}
#endif  // SERVER_LIST_H_
