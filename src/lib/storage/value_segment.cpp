#include "value_segment.hpp"
#include <string>
#include <vector>
#include "type_cast.hpp"

namespace opossum {

template <typename T>
AllTypeVariant ValueSegment<T>::operator[](const ChunkOffset chunk_offset) const {
  return _values.at(chunk_offset);
}

template <typename T>
void ValueSegment<T>::append(const AllTypeVariant& val) {
  _values.emplace_back(type_cast<T>(val));
}

template <typename T>
ChunkOffset ValueSegment<T>::size() const {
  return _values.size();
}

template <typename T>
const std::vector<T>& ValueSegment<T>::values() const {
  return _values;
}

// TODO: write test
template <typename T>
size_t ValueSegment<T>::estimate_memory_usage() const {
  return _values.capacity() * sizeof(T);
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(ValueSegment);

}  // namespace opossum
