#include "value_segment.hpp"

#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "type_cast.hpp"
#include "utils/assert.hpp"

namespace opossum {

template <typename T>
AllTypeVariant ValueSegment<T>::operator[](const ChunkOffset chunk_offset) const {
  Assert(chunk_offset >= 0 && chunk_offset < size(), "The parameter \"chunk_offset\" is out of range.");
  return _values[chunk_offset];
}

template <typename T>
void ValueSegment<T>::append(const AllTypeVariant& val) {
  auto typed_val = type_cast<T>(val);
  _values.emplace_back(typed_val);
}

template <typename T>
ChunkOffset ValueSegment<T>::size() const {
  return _values.size();
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(ValueSegment);

}  // namespace opossum
