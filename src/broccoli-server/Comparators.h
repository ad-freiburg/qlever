// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold <buchholb>

#ifndef  GLOBALS_COMPARATORS_H_
#define  GLOBALS_COMPARATORS_H_

#include <string>
#include <vector>
#include "./Globals.h"
#include "../util/StringUtils.h"

using std::string;
using std::vector;

// General purpose comparators. Only comparators that are
// used or suited for use in more than one subfolder / topic
// are moved into this file.
namespace ad_semsearch {
//! Comparator that is used to distribute word IDs.
//! Works just like the normal less than (<) operator
//! on std::string with the exception that words
//! starting with the ENTITY_PREFIX specified in Globals.h
//! are always considered greater than "normal" words.
class EntitiesLastLessThanStringComparator {
  public:
    bool operator()(const string& first, const string& second) const {
      bool firstIsEntity = ad_utility::startsWith(first, ENTITY_PREFIX);
      bool secondIsEntity = ad_utility::startsWith(second, ENTITY_PREFIX);
      if (firstIsEntity == secondIsEntity) {
        // Either both are entities or none of them is.
        // Use normal comparison.
        return first < second;
      } else {
        // Otherwise the first is "less than" the second iff the
        // second is an entity, because we can be sure exactly of of them
        // is an entity.
        return secondIsEntity;
      }
    }
};
//! Comparator used by the underlying map. Special in a way that
//! foo* < foo and foo* < fooa.
//! An alternative to making every word a prefix and having others
//! with a trailing special char.
class FulltextQueryComparator {
  public:
    bool operator()(const string& lhs, const string& rhs) {
      size_t i = 0;
      size_t j = 0;
      while (i < lhs.size() && j < rhs.size()) {
        if (lhs[i] == rhs[j]) {
          ++i;
          ++j;
        } else {
          return lhs[i] == PREFIX_CHAR || lhs[i] <= rhs[j];
        }
      }
      if (j == rhs.size()) {
        return i == lhs.size() || lhs[i] == PREFIX_CHAR;
      }
      return rhs[j] != PREFIX_CHAR;
    }
};
}
namespace ad_utility {
class StringLengthIsLessComparator {
  public:
    bool operator()(const string& first, const string& second) const {
      return first.size() < second.size();
    }
};

class StringLengthIsGreaterComparator {
  public:
    bool operator()(const string& first, const string& second) const {
      return first.size() > second.size();
    }
};

class VectorSizeIsLessComparator {
  public:
    template<class T>
    bool operator()(const vector<T>& first, const vector<T>& second) const {
      return first.size() < second.size();
    }
};

class VectorSizeIsGreaterComparator {
  public:
    template<class T>
    bool operator()(const vector<T>& first, const vector<T>& second) const {
      return first.size() > second.size();
    }
};

class CompareLhsComparator {
  public:
      template<class T>
      bool operator()(const T& first, const T& second) const {
        return first._lhs < second._lhs;
      }
};

class CompareRhsComparator {
  public:
      template<class T>
      bool operator()(const T& first, const T& second) const {
        return first._rhs < second._rhs;
      }
};

class CompareIdComparator {
  public:
      template<class T>
      bool operator()(const T& first, const T& second) const {
        return first._id < second._id;
      }
};

class CompareContextIdComparator {
  public:
      template<class T>
      bool operator()(const T& first, const T& second) const {
        return first._contextId < second._contextId;
      }
};

class CompareScoreComparatorLt {
  public:
      template<class T>
      bool operator()(const T& first, const T& second) const {
        return first.getScore() < second.getScore();
      }
};
class CompareScoreComparatorGt {
  public:
      template<class T>
      bool operator()(const T& first, const T& second) const {
        return first.getScore() > second.getScore();
      }
};
class CompareScoreOverContextIdComparatorGt {
  public:
    template<class T>
    bool operator()(const T& first, const T& second) const {
      return first.getScore() > second.getScore()
          || (first.getScore() == second.getScore()
              && first.getContextId() < second.getContextId());
    }
};
class CompareScoreGtOverIdLtComparator {
  public:
    template<class T>
    bool operator()(const T& first, const T& second) const {
      return first._score > second._score
          || (first._score == second._score && first._id < second._id);
    }
};
class CompareScoreLtOverIdLtComparator {
  public:
    template<class T>
    bool operator()(const T& first, const T& second) const {
      return first._score < second._score
          || (first._score == second._score && first._id < second._id);
    }
};
class CompareSecondComparatorGt {
  public:
      template<class T>
      bool operator()(const T& first, const T& second) const {
        return first.second > second.second;
      }
};
class MaxLhsComparatorLt {
  public:
    template<class T>
    bool operator()(const T& first, const T& second) const {
      return first._maxLhs < second._maxLhs;
    }
};
class AsStringComparatorLt {
  public:
    template<class T>
    bool operator()(const T& first, const T& second) const {
      return first.asString() < second.asString();
    }
};
class AsStringPtrComparatorLt {
  public:
    template<class T>
    bool operator()(const T* first, const T* second) const {
      return first->asString() < second->asString();
    }
};
class SizeComparatorLt {
  public:
    template<class T>
    bool operator()(const T& first, const T& second) const {
      return first.size() < second.size();
    }
};
class SizePtrComparatorLt {
  public:
    template<class T>
    bool operator()(const T* first, const T* second) const {
      return first->size() < second->size();
    }
};
class MinusLastStringComparatorlt {
  public:
    bool operator()(const string& first, const string& second) const {
      bool firstMinus = first.size() > 0 && first[0] == '-';
      bool secondMinus = second.size() > 0 && second[0] == '-';
      if (firstMinus == secondMinus) { return first < second; }
      return secondMinus;
    }
};
}

#endif  // GLOBALS_COMPARATORS_H_
