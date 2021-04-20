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

void Chunk::add_segment(std::shared_ptr<BaseSegment> segment) {
  _columns.emplace_back(segment);
}

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  DebugAssert(values.size() == column_count(), "Row size mismatch while appending a new row.");
  for(ColumnID idx{0}; idx < values.size(); ++idx ){
    _columns[idx]->append(values[idx]);
  }
}

std::shared_ptr<BaseSegment> Chunk::get_segment(ColumnID column_id) const {
  DebugAssert(column_id < column_count(), "Column id out of bounds");
  return _columns[column_id];
}

ColumnCount Chunk::column_count() const {
  return ColumnCount{_columns.size()};
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
