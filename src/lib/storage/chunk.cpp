#include <iomanip>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "base_segment.hpp"
#include "chunk.hpp"

#include "utils/assert.hpp"

namespace opossum {

void Chunk::add_segment(std::shared_ptr<BaseSegment> segment){
  _columns.emplace_back(segment);
}

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  DebugAssert(values.size() == column_count(),
              "\"values\" size mismatched the column count while appending a new row.");
  const auto column_bounds = ColumnID{column_count()};
  for (auto column_index = ColumnID{0}; column_index < column_bounds; ++column_index) {
    _columns[column_index]->append(values[column_index]);
  }
}

std::shared_ptr<BaseSegment> Chunk::get_segment(ColumnID column_id) const {
  DebugAssert(column_id < column_count(), "\"column_id\" is out of range.");
  return _columns[column_id];
}

ColumnCount Chunk::column_count() const {
  // TODO(hig): Try to remove cast again
  const auto column_count = static_cast<uint16_t>(_columns.size());
  return ColumnCount{column_count};
}

ChunkOffset Chunk::size() const {
  if (_columns.empty()) {
    return 0;
  } else {
    // read row count from first column (each should have the same height)
    return _columns[0]->size();
  }
}

}  // namespace opossum
