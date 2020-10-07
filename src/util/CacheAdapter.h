//
// Created by johannes on 01.10.20.
//

#ifndef QLEVER_CACHEADAPTER_H
#define QLEVER_CACHEADAPTER_H
#include <utility>
#include <memory>
#include "./Synchronized.h"
#include "../util/HashMap.h"

namespace ad_utility {

using std::shared_ptr;
using std::make_shared;

template <typename Key, typename Value, typename OnFinishedAction>
struct TryEmplaceResult {
  TryEmplaceResult(Key&& key, shared_ptr<Value> todo, shared_ptr<Value> done, OnFinishedAction* action):
 _val{std::move(todo), std::move(done)}, _key{std::move(key)}, _onFinishedAction{action} {}
  std::pair<shared_ptr<Value>, shared_ptr<const Value>> _val;
  void finish() {
    if (_val.first) {
      (*_onFinishedAction)(*_val.first);
    }
  }

 private:
  Key _key;
  OnFinishedAction* _onFinishedAction;

};

template <typename Cache, typename OnFinishedAction>
class CacheAdapter {
 public:
  using Value = typename Cache::value_type;
  using Key = typename Cache::key_type;
  using SyncCache = ad_utility::Synchronized<Cache, std::mutex>;

  template <typename... CacheArgs>
  CacheAdapter(OnFinishedAction action, CacheArgs&&... cacheArgs ):
    _cache{SyncCache(std::forward<CacheArgs>(cacheArgs)...)}, _onFinishedAction{action} {}
 private:
  class Action {
   public:
    Action(bool pinned, SyncCache* cPtr, OnFinishedAction* action):
    _cachePtr{cPtr}, _singleAction{action}, _pinnedInsert{pinned} {}

    void operator()(Key k, shared_ptr<Value> vPtr) {
      _singleAction(*vPtr);
      if (_pinnedInsert) {
        _cachePtr->wlock()->insertPinned(std::move(k), std::move(vPtr));
      } else {
        _cachePtr->wlock()->insert(std::move(k), std::move(vPtr));
      }
    }
   private:
    ad_utility::Synchronized<Cache, std::mutex>* _cachePtr;
    OnFinishedAction* _singleAction;
    const bool _pinnedInsert;
  };

 public:
  using Res = TryEmplaceResult<Key, Value, Action>;
  template <class... Args>

  Res tryEmplace(const Key& key, Args&&... args) {
    _cache.withWriteLock([&](auto& cache) {
      if (cache.contains(key)) {
        return Res{key, shared_ptr<Value>{}, cache[key], makeAction()};
      } else if (_inProgress.contains(key)) {
        return Res{key, shared_ptr<Value>{}, _inProgress[key], makeAction()};
      }
      auto empl = make_shared<Value>(std::forward<Args>(args)...);
      // Todo: threadSafety, this whole thing has to be locked.
      // Maybe only use this within an ad_utility::synchronized
      _inProgress[key] = empl;
      return Res{key, empl, empl, makeAction()};
    });
  }
 private:
  ad_utility::Synchronized<Cache, std::mutex> _cache;
  OnFinishedAction _onFinishedAction;
  HashMap<Key, shared_ptr<Value>> _inProgress;

  Action makeAction(bool pinned=false) {
    return Action(pinned, _cache, &_onFinishedAction);
  }
};
}



#endif  // QLEVER_CACHEADAPTER_H
