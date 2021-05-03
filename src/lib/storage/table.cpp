#include "table.hpp"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "value_segment.hpp"

#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

Column::Column(std::string name, std::string type) : name(std::move(name)), type(std::move(type)) {}

Table::Table(const ChunkOffset target_chunk_size) : _target_chunk_size{target_chunk_size} {
  // create initial chunk
  _append_new_chunk();
}

void Table::add_column(const std::string& name, const std::string& type) {
  Assert(row_count() == 0, "The table already contains rows, column scheme can not be altered anymore.")
  _columns.emplace_back(name, type);
  _append_column_to_chunks(type);
}

void Table::append(const std::vector<AllTypeVariant>& values) {
  if (_chunks.back()->size() == _target_chunk_size) {
    _append_new_chunk();
  }
  _chunks.back()->append(values);
}

// TODO(max): write test
void Table::emplace_chunk(std::unique_ptr<Chunk> chunk) {
  auto last_chunk = _chunks.back();
  if(last_chunk->size() == 0) {
    last_chunk = std::move(chunk);
  } else {
    Assert(last_chunk->size() != _target_chunk_size,
                "Cannot emplace chunk because current last chunk is not full.");
    _chunks.emplace_back(std::move(chunk));
  }
}

ColumnCount Table::column_count() const { return ColumnCount{static_cast<uint16_t>(_columns.size())}; }

uint64_t Table::row_count() const {
  return std::accumulate(_chunks.begin(), _chunks.end(), 0,
                         [](uint64_t sum, const auto& current_chunk) { return sum + current_chunk->size(); });
}

ChunkCount Table::chunk_count() const { return ChunkCount{static_cast<uint32_t>(_chunks.size())}; }

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  const auto find_result_iter =
      std::find_if(_columns.begin(), _columns.end(), [&column_name](const Column& c) { return c.name == column_name; });
 Assert(find_result_iter != _columns.end(), "Column name does not exist.");
  const auto find_index = static_cast<uint16_t>(std::distance(_columns.begin(), find_result_iter));
  return ColumnID{find_index};
}

ChunkOffset Table::target_chunk_size() const { return _target_chunk_size; }

const std::vector<std::string> Table::column_names() const {
  auto column_names = std::vector<std::string>();
  column_names.reserve(_columns.size());
  for (const auto& column : _columns) {
    column_names.emplace_back(column.name);
  }
  return column_names;
}

const std::string& Table::column_name(const ColumnID column_id) const {
  return _columns.at(column_id).name;
}

const std::string& Table::column_type(const ColumnID column_id) const {
  return _columns.at(column_id).type;
}

Chunk& Table::get_chunk(ChunkID chunk_id) {
  return *_chunks.at(chunk_id);
}

const Chunk& Table::get_chunk(ChunkID chunk_id) const {
  return *_chunks.at(chunk_id);
}

void Table::_append_new_chunk() {
  auto new_chunk = std::make_unique<Chunk>();

  for (const auto& column : _columns) {
    // append existing columns as segments to new chunk
    new_chunk->add_segment(_create_value_segment_for_type(column.type));
  }
  _chunks.emplace_back(std::move(new_chunk));
}

std::shared_ptr<BaseSegment> Table::_create_value_segment_for_type(const std::string& type) {
  auto new_segment = std::shared_ptr<BaseSegment>{};
  resolve_data_type(type, [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;
    new_segment = std::make_shared<ValueSegment<ColumnDataType>>();
  });
  return new_segment;
}

void Table::_append_column_to_chunks(const std::string& type) {
  for (const auto& chunk : _chunks) {
    // append new segment to every existing chunk
    chunk->add_segment(_create_value_segment_for_type(type));
  }
}

}  // namespace opossum
