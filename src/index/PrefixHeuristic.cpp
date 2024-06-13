// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#include "./PrefixHeuristic.h"

#include <algorithm>
#include <fstream>

#include "../parser/RdfEscaping.h"
#include "../parser/Tokenizer.h"
#include "../util/Exception.h"
#include "../util/File.h"
#include "../util/Log.h"
#include "../util/StringUtils.h"

using std::string;

namespace ad_utility {

// ______________________________________________________________________
TreeNode* Tree::insert(string_view value) { return _root->insert(value); }

// ______________________________________________________________________
TreeNode* Tree::insert(string_view value, TreeNode* startPoint) {
  if (!startPoint) {
    startPoint = _root.get();
  }
  return startPoint->insert(value);
}

// ______________________________________________________________
TreeNode* TreeNode::insertAfter(string_view value) {
  // exact match of the  value
  if (value == _value) {
    _ownCount++;
    return this;
  }

  // we now know that _value is a real prefix of value
  // check if one of the children also is a prefix of value
  for (auto& c : _children) {
    if (value.starts_with(c->_value)) {
      return c->insertAfter(value);
    }
  }

  // If we have reached here, we have to add a new child
  // Using std::make_unique here would require the constructor of TreeNode to be
  // public, which is not (better encapsulation)
  NodePtr newNode{new TreeNode(value)};
  newNode->_parent = this;

  // find children of current node which have to become children of the new node
  auto pred = [&value](const NodePtr& s) {
    return s->_value.starts_with(value);
  };
  auto itChildren = std::remove_if(_children.begin(), _children.end(), pred);

  // move these children to the new node
  auto& newChildren = newNode->_children;
  for (auto it = itChildren; it != _children.end(); ++it) {
    // unique ptrs are move only
    newChildren.push_back(std::move(*it));
  }
  _children.erase(itChildren, _children.end());

  // register the newly created node as a child of this node
  _children.push_back(std::move(newNode));
  return _children.back().get();
}

// ______________________________________________________________________
TreeNode* TreeNode::insert(string_view value) {
  if (value.starts_with(_value)) {
    // this node is a prefix of value, insert in subtree
    return insertAfter(value);
  }
  // search for insertion point one level above
  // Note: since the root is the empty string which is a prefix of everything
  // and all other nodes have a _parent this always works
  return _parent->insert(value);
}

// ___________________________________________________________________________
std::pair<size_t, TreeNode*> TreeNode::getMaximum(size_t codelength) {
  // _sharedCount = _ownCount + sum over childrens' _sharedCount
  _sharedCount = _ownCount;

  // get Maximum score and node from all the children
  size_t maxScore = 0;
  TreeNode* maxPtr(nullptr);
  for (auto& c : _children) {
    auto x = c->getMaximum(codelength);
    _sharedCount += c->_sharedCount;
    if (x.first >= maxScore) {
      maxScore = x.first;
      maxPtr = x.second;
    }
  }

  // score calculation

  // Example: if this node is "abab", and we already have compressed by "ab"
  // (_penaltyLength = 2) and our codes have length 1 then we actually gain only
  // one byte per word (relevantLength = 1) by compressing  with "abab"
  size_t relevantLength = 0;
  if (_value.size() > _penaltyLength + codelength) {
    relevantLength = _value.size() - _penaltyLength - codelength;
  }

  // if we have chosen this prefix before, we can not cover anything in its
  // subtree with a shorter prefix, so we do not propagate any _sharedCounts to
  // our parents
  if (!_active) {
    _sharedCount = 0;
  }

  _score = _sharedCount * relevantLength;

  // Check if our own score is greater than the score any of the childrens'
  // we choose >= so we get a valid pointer when there is only the root with
  // score 0 left.
  if (_score >= maxScore) {
    maxScore = _score;
    maxPtr = this;
  }
  return std::make_pair(maxScore, maxPtr);
}

// __________________________________________________________________
std::pair<size_t, string> Tree::getAndDeleteMaximum(size_t codelength) {
  // find the maximum
  auto x{_root->getMaximum(codelength)};
  // "delete" or deactivate the given node
  x.second->penaltize();
  return std::make_pair(x.first, x.second->_value);
}

// ___________________________________________________________________
void TreeNode::penaltizeChildren(size_t penaltyLength) {
  // always keep track of the longest ancestor that  has already been chosen for
  // compression
  _penaltyLength = std::max(_penaltyLength, penaltyLength);
  for (auto& c : _children) {
    c->penaltizeChildren(penaltyLength);
  }
}

// ___________________________________________________________________
void TreeNode::penaltize() {
  // reduce the compression gain of the children (because some of their
  // characters have already been compressed by this node
  penaltizeChildren(_value.size());

  _ownCount = 0;
  // choosing a shorter prefix will never gain anything in this node's subtree
  // from now on
  _active = false;
}
}  // namespace ad_utility

// ______________________________________________________________________________________
std::vector<string> calculatePrefixes(const std::vector<string>& vocabulary,
                                      size_t numPrefixes, size_t codelength,
                                      bool alwaysAddCode) {
  // same function, just for vector of strings
  size_t minPrefixLength = alwaysAddCode ? 1 : codelength + 1;
  size_t actualCodeLength = alwaysAddCode ? 0 : codelength;
  if (vocabulary.empty()) {
    return {};
  }
  ad_utility::Tree t;
  ad_utility::TreeNode* lastPos(nullptr);

  const string* lastWord = &(vocabulary[0]);
  for (size_t i = 1; i < vocabulary.size(); ++i) {
    auto pref = ad_utility::commonPrefix(*lastWord, vocabulary[i]);
    if (pref.size() >= minPrefixLength) {
      lastPos = t.insert(pref, lastPos);
    } else {
      // lastPos.reset(nullptr);
      lastPos = nullptr;
    }
    lastWord = &(vocabulary[i]);
  }
  std::vector<string> res;
  res.reserve(numPrefixes);
  for (size_t i = 0; i < numPrefixes; ++i) {
    res.push_back(t.getAndDeleteMaximum(actualCodeLength).second);
  }
  return res;
}
