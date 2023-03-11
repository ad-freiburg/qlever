// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#include "util/json.h"

/*
 * A rather basic wrapper for nlohmann::json, which only allows basic adding
 * of key value pairs and returning of the json string.
 */
class BenchmarkMetadata{
    // No real reason, to really build everything ourselves, when the
    // nlohmann::json object already containes everything, that we could need.
    nlohmann::json data_;

  public:

    /*
     * @brief Adds a key value pair to the metadata.
     */
    template<typename T>
    void addKeyValuePair(const std::string& key, const T& value){
      data_[key] = value;
    }

    /*
     * @brief Returns the metadata in form of a json string.
     */
    std::string asJsonString() const{
      return data_.dump();
    }

    // JSON (d)serialization.
    // TODO This works, but `"data_":null` looks a bit ugly. Is there a way,
    // to skip the `data_` and just have the content of `data_` as the
    // serialied form of this class?
    NLOHMANN_DEFINE_TYPE_INTRUSIVE(BenchmarkMetadata, data_)
};
