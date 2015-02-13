// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#ifndef SERVER_ENTITYLIST_H_
#define SERVER_ENTITYLIST_H_

#include <vector>
#include <string>
#include <sstream>
#include "./Globals.h"
#include "./Comparators.h"
#include "../util/Log.h"
#include "./List.h"
#include "./Identifiers.h"

using std::vector;
using std::string;

namespace ad_semsearch {
//! List Element.
//! An entity represented by its ID with a score that usually
//! results from aggregating several entity postings with scores.
class EntityWithScore {
  public:
    EntityWithScore(): _id(0), _score(0) {
    }

    EntityWithScore(Id id, AggregatedScore score) :
      _id(id), _score(score) {
    }

    ~EntityWithScore() {
    }

    string asString() {
      std::ostringstream os;
      os << "(EntityId: " << getPureValue(_id) << ", Score: "
          << static_cast<int> (_score) << ")";
      return os.str();
    }

    bool operator<(const EntityWithScore& other) const {
      return _id < other._id;
    }

    const AggregatedScore& getScore() const {
      return _score;
    }

    Id _id;
    AggregatedScore _score;
};

// List representing a list of entities like it is used
// as (intermediate) query result in many places.
// The list is well-formed iff entities are unique and
// ordered.
class EntityList: public List<EntityWithScore> {
  public:

    EntityList() {
    }

    ~EntityList() {
    }

    bool isWellFormed() {
      bool wellFormed = true;
      if (size() > 0) {
        Id last = List<EntityWithScore>::operator[](0)._id;
        for (size_t i = 1; i < size(); ++i) {
          wellFormed = wellFormed && last
              < List<EntityWithScore>::operator[](i)._id;
          last = List<EntityWithScore>::operator[](i)._id;
        }
      }
      return wellFormed;
    }

    bool areAllScoresEqual() const {
      LOG(DEBUG) << "Checking if all scores of " << _data.size()
          << " entities are the same.\n";
      if (_data.size() <= 1) {
        LOG(DEBUG) << "This is trivially the case.\n";
        return true;
      } else {
        AggregatedScore score = _data[0]._score;
        for (size_t i = 1; i < _data.size(); ++i) {
          if (_data[i]._score != score) {
            LOG(DEBUG) << "This is not the case.\n";
            LOG(DEBUG) << "Element 0 score: " << score << ", element " << i
                << " score: " << _data[i]._score << "\n";
            return false;
          }
        }
        LOG(DEBUG) << "This is the case.\n";
        return true;
      }
    }

    //! Check if the results conatins a certain entity.
    bool contains(Id entityId) const {
      if (_data.size() == 0) {
        return false;
      }

      EntityWithScore toFind(entityId, 0);
      EntityList::const_iterator it = std::lower_bound(_data.begin(),
          _data.end(), toFind, ad_utility::CompareIdComparator());
      return (it != _data.end() && it->_id == entityId);
    }
};
}
#endif  // SERVER_ENTITYLIST_H_
