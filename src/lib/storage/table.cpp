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

Table::Table(const ChunkOffset target_chunk_size) : _target_chunk_size{target_chunk_size} {
  // create initial chunk
  _append_new_chunk();
}

void Table::add_column(const std::string& name, const std::string& type) {
  _column_names.emplace_back(name);
  _column_types.emplace_back(type);
  _append_column_to_chunks(type);
}

void Table::append(const std::vector<AllTypeVariant>& values) {
  if (_chunks.back()->size() == _target_chunk_size) {
    _append_new_chunk();
  }
  _chunks.back()->append(values);
}

void Table::emplace_chunk(const std::shared_ptr<Chunk>& chunk) {
  if (row_count() == 0) {
    // only existing chunk is empty -> replace chunk
    _chunks[0] = chunk;
  } else {
    // TODO(max): DebugAssert that the previous chunk is full
    _chunks.emplace_back(chunk);
  }
}

ColumnCount Table::column_count() const {
  // TODO(hig): Try to remove cast again
  return static_cast<ColumnCount>(_column_types.size());
}

uint64_t Table::row_count() const {
  return std::accumulate(
      _chunks.begin(), _chunks.end(), 0,
      [](uint64_t sum, const std::shared_ptr<Chunk>& current_chunk) { return sum + current_chunk->size(); });
}

ChunkID Table::chunk_count() const {
  // TODO(hig): Try to remove cast again
  return static_cast<ChunkID>(_chunks.size());
}

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  const auto search_index_iter = std::find(_column_names.begin(), _column_names.end(), column_name);
  if (search_index_iter == _column_names.end()) {
    throw std::invalid_argument("Column name does not exists.");
  }
  // TODO(hig): Try to remove cast again
  return static_cast<ColumnID>(std::distance(_column_names.begin(), search_index_iter));
}

ChunkOffset Table::target_chunk_size() const { return _target_chunk_size; }

const std::vector<std::string>& Table::column_names() const { return _column_names; }

const std::string& Table::column_name(const ColumnID column_id) const {
  DebugAssert(column_id < column_count(), "\"column_id\" is out of bounds.");
  return _column_names[column_id];
}

const std::string& Table::column_type(const ColumnID column_id) const {
  DebugAssert(column_id < column_count(), "\"column_id\" is out of bounds.");
  return _column_types[column_id];
}

Chunk& Table::get_chunk(ChunkID chunk_id) {
  DebugAssert(chunk_id < column_count(), "\"chunk_id\" is out of bounds.");
  return *_chunks[chunk_id];
}

const Chunk& Table::get_chunk(ChunkID chunk_id) const {
  DebugAssert(chunk_id < column_count(), "\"column_id\" is out of bounds.");
  return *_chunks[chunk_id];
}

std::shared_ptr<Chunk> Table::_create_new_chunk() {
  auto new_chunk = std::make_shared<Chunk>();
  _chunks.emplace_back(new_chunk);
  return new_chunk;
}

void Table::_append_new_chunk() {
  const auto& new_chunk = _create_new_chunk();
  for (const auto& column_type : _column_types) {
    // append existing columns as segments to new chunk
    new_chunk->add_segment(_create_value_segment_for_type(column_type));
  }
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
  DebugAssert(row_count() == 0, "Cannot append new columns to already existing chunks.");
  for (const auto& chunk : _chunks) {
    // append new segment to every existing chunk
    chunk->add_segment(_create_value_segment_for_type(type));
  }
}

}  // namespace opossum
