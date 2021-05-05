#pragma once

#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "base_attribute_vector.hpp"
#include "fixed_size_attribute_vector.hpp"
#include "types.hpp"
#include "base_segment.hpp"
#include "type_cast.hpp"

namespace opossum {

// Even though ValueIDs do not have to use the full width of ValueID (uint32_t), this will also work for smaller ValueID
// types (uint8_t, uint16_t) since after a down-cast INVALID_VALUE_ID will look like their numeric_limit::max()
constexpr ValueID INVALID_VALUE_ID{std::numeric_limits<ValueID::base_type>::max()};

// Dictionary is a specific segment type that stores all its values in a vector
template <typename T>
class DictionarySegment : public BaseSegment {
 public:
  /**
   * Creates a Dictionary segment from a given value segment.
   */
  explicit DictionarySegment(const std::shared_ptr<BaseSegment>& base_segment) {
    _dictionary = std::make_shared<std::vector<T>>();
    auto tmp_attribute_vector = std::vector<ValueID>{};

    const auto segment_size = base_segment->size();
    tmp_attribute_vector.reserve(segment_size);

    for (ChunkOffset chunk_offset = 0; chunk_offset < segment_size; chunk_offset++){
      const auto existing_value_to_add = (*base_segment)[chunk_offset];
      const auto append_value_id = _try_add_to_dictionary(existing_value_to_add);
      tmp_attribute_vector.emplace_back(append_value_id);
    }

    const auto local_unique_values_count = unique_values_count();

    if (local_unique_values_count <= std::numeric_limits<uint8_t>::max()){
      _attribute_vector = std::make_shared<FixedSizeAttributeVector<uint8_t>>(tmp_attribute_vector);
    }
    else if (local_unique_values_count <= std::numeric_limits<uint16_t>::max()){
      _attribute_vector = std::make_shared<FixedSizeAttributeVector<uint16_t>>(tmp_attribute_vector);
    }
    else if (local_unique_values_count <= std::numeric_limits<uint32_t>::max()){
      _attribute_vector = std::make_shared<FixedSizeAttributeVector<uint32_t>>(tmp_attribute_vector);
    }
    else{
      throw std::runtime_error("The segment contains more than UINT32_MAX distinct values.");
    }
  }

  // SEMINAR INFORMATION: Since most of these methods depend on the template parameter, you will have to implement
  // the DictionarySegment in this file. Replace the method signatures with actual implementations.

  // return the value at a certain position. If you want to write efficient operators, back off!
  AllTypeVariant operator[](const ChunkOffset chunk_offset) const override{
    return get(chunk_offset);
  }

  // return the value at a certain position.
  T get(const size_t chunk_offset) const{
    const auto valueId = _attribute_vector->get(chunk_offset);
    return value_by_value_id(valueId);
  }

  // dictionary segments are immutable
  void append(const AllTypeVariant& val) override{
    throw std::runtime_error("Appending to an immutable dictionary segment is not allowed.");
  }

  // returns an underlying dictionary
  std::shared_ptr<const std::vector<T>> dictionary() const{
    return _dictionary;
  }

  // returns an underlying data structure
  std::shared_ptr<const BaseAttributeVector> attribute_vector() const{
    return _attribute_vector;
  }

  // return the value represented by a given ValueID
  const T& value_by_value_id(ValueID value_id) const{
    return _dictionary->at(value_id);
  }

  // returns the first value ID that refers to a value >= the search value
  // returns INVALID_VALUE_ID if all values are smaller than the search value
  ValueID lower_bound(T value) const{
    return ValueID{0};
  }

  // same as lower_bound(T), but accepts an AllTypeVariant
  ValueID lower_bound(const AllTypeVariant& value) const{
    return ValueID{0};
  }

  // returns the first value ID that refers to a value > the search value
  // returns INVALID_VALUE_ID if all values are smaller than or equal to the search value
  ValueID upper_bound(T value) const{
    return ValueID{0};
  }

  // same as upper_bound(T), but accepts an AllTypeVariant
  ValueID upper_bound(const AllTypeVariant& value) const{
   return ValueID{0};
  }

  // return the number of unique_values (dictionary entries)
  size_t unique_values_count() const{
    return _dictionary->size();
  }

  // return the number of entries
  ChunkOffset size() const override{
    return _attribute_vector->size();
  }

  // returns the calculated memory usage
  size_t estimate_memory_usage() const final{
    return 0;
  }

 protected:
  std::shared_ptr<std::vector<T>> _dictionary;
  std::shared_ptr<BaseAttributeVector> _attribute_vector;
  // Returns the new ValueId of the added value or the existing ValueId if the value already exists in the dictionary.
  ValueID _try_add_to_dictionary(const AllTypeVariant& val){
    const auto typed_val = type_cast<T>(val);
    const auto insert_iter = std::lower_bound(_dictionary->begin(), _dictionary->end(), typed_val);

    if (insert_iter == _dictionary->end() || *insert_iter != typed_val){
      // Dictionary does not contain the value to add -> Add value to dictionary.
      _dictionary->emplace(insert_iter, typed_val);
    }

    const auto valueId = std::distance(_dictionary->begin(), insert_iter);
    return ValueID{static_cast<ValueID::base_type>(valueId)};
  }
};

}  // namespace opossum
