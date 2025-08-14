// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#ifndef QLEVER_SRC_INDEX_PREFIXHEURISTIC_H
#define QLEVER_SRC_INDEX_PREFIXHEURISTIC_H

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

// A simple greedy algorithm the calculates prefixes of a given vocabulary which
// are suitable for compression.
//
// Arguments: vocabulary  - The vocabulary that is to be compressed. Must be
//                          sorted ascending by `std::less<std::string>`
//            numPrefixes - the number of prefixes we want to compute
//
// Returns:   Vector of suitable prefixes which have been selected by the
//            algorithm
//
std::vector<std::string> calculatePrefixes(
    const std::vector<std::string>& vocabulary, size_t numPrefixes);

namespace ad_utility {

// Node of the Tree used in the algorithm. Each TreeNode represents a prefix of
// the vocabulary that can be chosen for compression
class TreeNode {
 private:
  friend class Tree;

  using NodePtr = std::unique_ptr<TreeNode>;

  // Constructor
  explicit TreeNode(std::string_view value) : _value(value) {}

  // Recursive Insertion of value. If the value does not match _value we will
  // automatically call insert on a node that is closer to the actual position
  // of value in the Tree. Returns the node that was actually inserted
  TreeNode* insert(std::string_view value);

  // recursive insertion used when this nodes _value is a prefix of argument
  // value. Thus we know that we will insert into this TreeNode's subtree
  // Returns the node that was actually inserted
  TreeNode* insertAfter(std::string_view value);

  // Compute the scores of this Nodes subtree and find the maximum score and the
  // Node to which this value corresponds.
  // Argument: - the length of the code with which we will compress the maximum
  //             prefix, influences the score computation
  // Returns the maximum score and the node where it was achieved
  std::pair<size_t, TreeNode*> getMaximum(size_t codelength);

  // After choosing this node as a compression prefix we have to modify the tree
  // by recursively reducing some scores. This is handled by the penaltize
  // function
  void penaltize();

  // Recursive helper function for penaltize (see implementation)
  void penaltizeChildren(size_t penaltyLength);

  // _______________________________________________________
  TreeNode* _parent = nullptr;

  // children are owned by their parents, the root is owned by the Tree class so
  // memory management works  automatically
  std::vector<NodePtr> _children;

  // the prefix that is represented by this  node
  std::string _value;

  // the number of characters we will save by choosing this node as a prefix for
  // compression
  size_t _score = 0;

  // the maximum length of an ancestor's _value that already has been chosen for
  // compression. Reduces the score (e. g. if we already have
  // (greedily)compressed
  // with "wikidata" the gain of choosing "wikidata:property" is reduced)
  size_t _penaltyLength = 0;

  // the number of times this prefix was inserted to the tree
  size_t _ownCount = 1;

  // the number of times this prefix or an overlapping child-prefix was inserted
  // to the tree
  size_t _sharedCount = 1;

  // active nodes have not yet been chosen for compression
  bool _active = true;
};

// A rooted tree with string values. Holds the following invariant:
// node a is an ancestor of b if and only if a._value is a prefix of b._value
// The root's value always is the empty string "".
class Tree {
 private:
  // this actually sets the _count  to 1 which might be misleading,
  // but since the empty string has size 0 this does not influence our
  // algorithm because the empty prefix does not gain anything during
  // compression
  std::unique_ptr<TreeNode> _root{new TreeNode("")};

 public:
  using NodePtr = std::unique_ptr<TreeNode>;

  // insert a value to the tree. If value is already present, the _ownCount of
  // the corresponding TreeNode is increased  by one.
  TreeNode* insert(std::string_view value);

  // Same functionality as the simple insert. Additionally gets a node as a hint
  // where to start searching for the value's place in the tree. This is useful
  // when inserting in alphabetical order since many words will appear close to
  // each other in the tree
  // passing the nullptr as startPoint will start looking at the root
  // Returns the node that was actually inserted
  TreeNode* insert(std::string_view value, TreeNode* startPoint);

  // Recursively compute the score of all the nodes in the tree, find the
  // maximum, return and delete it.
  // "deletion" is performed by modifying the tree in a way that corresponds to
  // the compression with the chosen nodes _value as a prefix.
  // Returns : the number of characters/bytes we save by choosing this prefix
  // and the prefix itself
  std::pair<size_t, std::string> getAndDeleteMaximum(size_t codelength);
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_INDEX_PREFIXHEURISTIC_H
