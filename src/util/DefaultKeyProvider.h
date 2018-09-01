// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#pragma once

#include <limits>
#include <string>

using std::string;

namespace {  // NOLINT
template <class KeyType>
class DefaultKeyProvider {
 public:
  static const KeyType DEFAULT_EMPTY_KEY;
  static const KeyType DEFAULT_DELETED_KEY;
};

template <>
class DefaultKeyProvider<string> {
 public:
  static const string DEFAULT_EMPTY_KEY;
  static const string DEFAULT_DELETED_KEY;
};

const string DefaultKeyProvider<string>::DEFAULT_EMPTY_KEY =
    "__adutils_default_empty_key";  // NOLINT

const string DefaultKeyProvider<string>::DEFAULT_DELETED_KEY =  // NOLINT
    "__adutils_default_deleted_key";

template <class KeyType>
const KeyType DefaultKeyProvider<KeyType>::DEFAULT_EMPTY_KEY =
    std::numeric_limits<KeyType>::max();

template <class KeyType>
const KeyType DefaultKeyProvider<KeyType>::DEFAULT_DELETED_KEY =
    std::numeric_limits<KeyType>::max() - 1;
}  // namespace
