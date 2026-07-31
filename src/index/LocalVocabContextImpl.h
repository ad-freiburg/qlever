// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_LOCALVOCABCONTEXTIMPL_H
#define QLEVER_SRC_INDEX_LOCALVOCABCONTEXTIMPL_H

#include <memory>
#include <optional>
#include <string_view>

#include "global/Id.h"
#include "index/LocalVocabContext.h"
#include "index/vocabulary/EncodedIriManager.h"
#include "index/vocabulary/Vocabulary.h"
#include "util/BlankNodeManager.h"

// The only implementation of the `LocalVocabContext` interface. It stores
// exactly the three things that the interface needs, and in particular does not
// depend on the `IndexImpl` that owns it.
//
// NOTE: There must be exactly one instance of this per index, see
// `LocalVocabContext.h`.
class LocalVocabContextImpl : public LocalVocabContext {
 private:
  using BlankNodeManagerPtr = std::unique_ptr<ad_utility::BlankNodeManager>;

  const RdfsVocabulary* vocabulary_;
  const EncodedIriManager* encodedIriManager_;
  // NOTE: This is a pointer to the owning `std::unique_ptr` and not to the
  // manager itself, because the manager is only created while the index is
  // being read, long after this object has been constructed.
  const BlankNodeManagerPtr* blankNodeManager_;

 public:
  LocalVocabContextImpl(const RdfsVocabulary* vocabulary,
                        const EncodedIriManager* encodedIriManager,
                        const BlankNodeManagerPtr* blankNodeManager)
      : vocabulary_{vocabulary},
        encodedIriManager_{encodedIriManager},
        blankNodeManager_{blankNodeManager} {}

  int compareWords(std::string_view a, std::string_view b) const override;
  VocabBounds getPositionOfWord(std::string_view word) const override;
  std::optional<Id> encodeAsId(std::string_view word) const override;
  ad_utility::BlankNodeManager* getBlankNodeManager() const override;
};

#endif  // QLEVER_SRC_INDEX_LOCALVOCABCONTEXTIMPL_H
