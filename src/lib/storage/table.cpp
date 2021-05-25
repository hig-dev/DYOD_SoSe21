#include "table.hpp"

#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "dictionary_segment.hpp"
#include "value_segment.hpp"

#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

ColumnDefinition::ColumnDefinition(std::string name, std::string type) : name(std::move(name)), type(std::move(type)) {}

Table::Table(const ChunkOffset target_chunk_size) : _target_chunk_size{target_chunk_size} {
  // create initial chunk
  create_new_chunk();
}

void Table::add_column_definition(const std::string& name, const std::string& type) {
  Assert(row_count() == 0, "The table already contains rows, column scheme can not be altered anymore.");
  _column_definitions.emplace_back(name, type);
}

void Table::copy_column_definition(const std::shared_ptr<const Table>& other_table, const ColumnID column_id) {
  _column_definitions.push_back(other_table->_column_definitions.at(column_id));
}

void Table::add_column(const std::string& name, const std::string& type) {
  add_column_definition(name, type);
  _append_column_to_chunks(type);
}

void Table::append(const std::vector<AllTypeVariant>& values) {
  if (_chunks.back()->size() == _target_chunk_size) {
    create_new_chunk();
  }
  _chunks.back()->append(values);
}

void Table::create_new_chunk() {
  auto new_chunk = std::make_shared<Chunk>();

  for (const auto& column : _column_definitions) {
    // append existing columns as segments to new chunk
    new_chunk->add_segment(_create_value_segment_for_type(column.type));
  }
  _chunks.emplace_back(new_chunk);
}

// TODO(max): write test
void Table::emplace_chunk(std::shared_ptr<Chunk> chunk) {
  if (row_count() == 0) {
    // only existing chunk is empty -> replace chunk
    _chunks[0] = chunk;
  } else {
    Assert(_chunks.back()->size() != _target_chunk_size,
           "Cannot emplace chunk because current last chunk is not full.");
    _chunks.emplace_back(chunk);
  }
}

bool Table::is_empty() const {
  if (chunk_count() == 0) return true;
  return column_count() == 0;
}

ColumnCount Table::column_count() const {
  return ColumnCount{static_cast<ColumnCount::base_type>(_column_definitions.size())};
}

uint64_t Table::row_count() const {
  return std::accumulate(_chunks.begin(), _chunks.end(), 0,
                         [](uint64_t sum, const auto& current_chunk) { return sum + current_chunk->size(); });
}

ChunkCount Table::chunk_count() const { return ChunkCount{static_cast<ChunkCount::base_type>(_chunks.size())}; }

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  const auto find_result_iter =
      std::find_if(_column_definitions.begin(), _column_definitions.end(),
                   [&column_name](const ColumnDefinition& c) { return c.name == column_name; });
  Assert(find_result_iter != _column_definitions.end(), "Column name does not exist.");
  const auto find_index = std::distance(_column_definitions.begin(), find_result_iter);
  return ColumnID{static_cast<ColumnID::base_type>(find_index)};
}

ChunkOffset Table::target_chunk_size() const { return _target_chunk_size; }

const std::vector<std::string> Table::column_names() const {
  auto column_names = std::vector<std::string>();
  column_names.reserve(_column_definitions.size());
  for (const auto& column : _column_definitions) {
    column_names.emplace_back(column.name);
  }
  return column_names;
}

const std::string& Table::column_name(const ColumnID column_id) const { return _column_definitions.at(column_id).name; }

const std::string& Table::column_type(const ColumnID column_id) const { return _column_definitions.at(column_id).type; }

Chunk& Table::get_chunk(ChunkID chunk_id) { return *_chunks.at(chunk_id); }

const Chunk& Table::get_chunk(ChunkID chunk_id) const { return *_chunks.at(chunk_id); }

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

void Table::compress_chunk(ChunkID chunk_id) {
  const auto& chunk_to_compress = get_chunk(chunk_id);
  const auto chunk_size = chunk_to_compress.size();

  auto threads = std::vector<std::thread>{};
  threads.reserve(chunk_size);

  // Compress each segment in parallel and store the compressed segments in a vector
  auto compressed_segments = std::vector<std::shared_ptr<BaseSegment>>{chunk_size};
  for (auto column_id = ColumnID{0}; column_id < chunk_to_compress.column_count(); ++column_id) {
    const auto segment_to_compress = chunk_to_compress.get_segment(column_id);
    const auto segment_type = _column_definitions[column_id].type;

    threads.emplace_back([&compressed_segments, segment_type, column_id, segment_to_compress] {
      resolve_data_type(segment_type, [&compressed_segments, column_id, segment_to_compress](const auto data_type_t) {
        using ColumnDataType = typename decltype(data_type_t)::type;
        compressed_segments[column_id] = std::make_shared<DictionarySegment<ColumnDataType>>(segment_to_compress);
      });
    });
  }

  // Wait for the completion of the threads
  for (auto& thread : threads) {
    thread.join();
  }

  // Create a new compressed chunk using the compressed segments vector
  auto compressed_chunk = std::make_shared<Chunk>();
  for (auto column_id = ColumnID{0}; column_id < chunk_size; ++column_id) {
    compressed_chunk->add_segment(compressed_segments[column_id]);
  }

  // Swap the uncompressed chunk with the newly created compressed chunk
  _chunks[chunk_id] = compressed_chunk;
}

}  // namespace opossum
