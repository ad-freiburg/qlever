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
#include <string>
#include <string_view>

#include "global/Id.h"
#include "index/LocalVocabContext.h"
#include "index/vocabulary/EncodedIriManager.h"
#include "index/vocabulary/SecondaryVocabulary.h"
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

  using SecondaryVocabularyPtr = std::shared_ptr<const SecondaryVocabulary>;

  const RdfsVocabulary* vocabulary_;
  const EncodedIriManager* encodedIriManager_;
  // NOTE: This is a pointer to the owning `std::unique_ptr` and not to the
  // manager itself, because the manager is only created while the index is
  // being read, long after this object has been constructed.
  const BlankNodeManagerPtr* blankNodeManager_;
  // NOTE: This also is a pointer to the owning smart pointer, because the
  // secondary vocabulary is only set while the index is being read, see
  // `IndexImpl::secondaryVocab()`.
  const SecondaryVocabularyPtr* secondaryVocab_;

 public:
  LocalVocabContextImpl(const RdfsVocabulary* vocabulary,
                        const EncodedIriManager* encodedIriManager,
                        const BlankNodeManagerPtr* blankNodeManager,
                        const SecondaryVocabularyPtr* secondaryVocab)
      : vocabulary_{vocabulary},
        encodedIriManager_{encodedIriManager},
        blankNodeManager_{blankNodeManager},
        secondaryVocab_{secondaryVocab} {}

  int compareWords(std::string_view a, std::string_view b) const override;
  VocabBounds getPositionOfWord(std::string_view word) const override;
  int compareIdsSemantically(Id a, Id b) const override;
  VocabBounds getSemanticPositionInMainVocab(Id id) const override;
  bool hasSecondaryVocabulary() const override;
  std::optional<SecondaryVocabIndex> getSecondaryVocabIndex(
      std::string_view word) const override;
  std::optional<Id> encodeAsId(std::string_view word) const override;
  ad_utility::BlankNodeManager* getBlankNodeManager() const override;

 private:
  // Return the word of the given `Id`, which has to be of one of the
  // `ValueId::stringTypes_`.
  std::string getWordOfStringTypedId(Id id) const;

  // Semantically compare the word of `secondaryId`, which has to be of type
  // `Datatype::SecondaryVocabIndex`, to the word of `other`.
  int compareSecondaryVocabIdTo(Id secondaryId, Id other) const;
};

#endif  // QLEVER_SRC_INDEX_LOCALVOCABCONTEXTIMPL_H
