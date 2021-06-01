#pragma once

#include <limits>
#include <vector>

#include "base_attribute_vector.hpp"
#include "utils/assert.hpp"

namespace opossum {

template <typename uintX_t>
class FixedSizeAttributeVector : public BaseAttributeVector {
 public:
  explicit FixedSizeAttributeVector(const std::vector<ValueID>& attribute_vector_to_copy) {
    _attribute_vector.reserve(attribute_vector_to_copy.size());
    for (const auto value_id : attribute_vector_to_copy) {
      _attribute_vector.emplace_back(value_id);
    }
  }

  ValueID get(const size_t i) const override { return ValueID{_attribute_vector.at(i)}; }

  void set(const size_t i, const ValueID value_id) override {
    DebugAssert(i < size(), "The index is out of bounds.");
    _attribute_vector[i] = value_id;
  }

  size_t size() const override { return _attribute_vector.size(); }

  AttributeVectorWidth width() const override { return sizeof(uintX_t); }

  size_t estimate_memory_usage() const override { return _attribute_vector.capacity() * width(); }

 private:
  std::vector<uintX_t> _attribute_vector;
};

}  // namespace opossum
