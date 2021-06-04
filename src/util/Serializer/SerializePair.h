//
// Created by johannes on 04.06.21.
//

#ifndef QLEVER_SERIALIZEPAIR_H
#define QLEVER_SERIALIZEPAIR_H

#include <utility>

namespace ad_utility::serialization {

template<typename Serializer, typename First, typename Second>
void serialize(Serializer& serializer, std::pair<First, Second>& el) {
  serializer & el.first;
  serializer & el.second;
}

template<typename Serializer, typename First, typename Second>
void serialize(Serializer& serializer, const std::pair<First, Second>& el) {
  static_assert(Serializer::IsWriteSerializer);
  serializer & el.first;
  serializer & el.second;
}

}
#endif  // QLEVER_SERIALIZEPAIR_H
