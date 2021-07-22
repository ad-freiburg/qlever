//
// Created by johannes on 09.05.21.
//

#ifndef QLEVER_RESULTTYPE_H
#define QLEVER_RESULTTYPE_H

namespace qlever {
/**
 * @brief Describes the type of a columns data
 */
enum class ResultType {
  // An entry in the knowledgebase
  KB,
  // An unsigned integer (size_t)
  VERBATIM,
  // A byte offset in the text index
  TEXT,
  // A 32 bit float, stored in the first 4 bytes of the entry. The last four
  // bytes have to be zero.
  FLOAT,
  // An entry in the ResultTable's _localVocab
  LOCAL_VOCAB
};
}  // namespace qlever

#endif  // QLEVER_RESULTTYPE_H
