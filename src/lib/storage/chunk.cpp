#include <iomanip>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "base_segment.hpp"
#include "chunk.hpp"

#include "utils/assert.hpp"

namespace opossum {

void Chunk::add_segment(std::shared_ptr<BaseSegment> segment) { _columns.emplace_back(segment); }

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  Assert(values.size() == column_count(), "\"values\" size mismatched the column count while appending a new row.");
  const auto column_bounds = ColumnID{column_count()};
  for (auto column_index = ColumnID{0}; column_index < column_bounds; ++column_index) {
    _columns[column_index]->append(values[column_index]);
  }
}

std::shared_ptr<BaseSegment> Chunk::get_segment(ColumnID column_id) const { return _columns.at(column_id); }

ColumnCount Chunk::column_count() const { return ColumnCount{static_cast<uint16_t>(_columns.size())}; }

ChunkOffset Chunk::size() const {
  // Read row count from first column (each should have the same height).
  return _columns.empty() ? 0 : _columns[0]->size();
}

size_t Chunk::estimate_memory_usage() const {
  return std::accumulate(_columns.begin(), _columns.end(), 0,
                         [](size_t sum, const auto& segment) { return sum + segment->estimate_memory_usage(); });
}

}  // namespace opossum
