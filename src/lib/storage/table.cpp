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

Column::Column(std::string name, std::string type) : name(std::move(name)), type(std::move(type)) {}

Table::Table(const ChunkOffset target_chunk_size) : _target_chunk_size{target_chunk_size} {
  // create initial chunk
  _append_new_chunk();
}

void Table::add_column(const std::string& name, const std::string& type) {
  Assert(row_count() == 0, "The table already contains rows, column scheme can not be altered anymore.");
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
  if (row_count() == 0) {
    // only existing chunk is empty -> sreplace chunk
    _chunks[0] = std::move(chunk);
  } else {
    Assert(_chunks.back()->size() != _target_chunk_size,
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

const std::string& Table::column_name(const ColumnID column_id) const { return _columns.at(column_id).name; }

const std::string& Table::column_type(const ColumnID column_id) const { return _columns.at(column_id).type; }

Chunk& Table::get_chunk(ChunkID chunk_id) { return *_chunks.at(chunk_id); }

const Chunk& Table::get_chunk(ChunkID chunk_id) const { return *_chunks.at(chunk_id); }

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

void Table::compress_chunk(ChunkID chunk_id) {
  const auto& uncompressed_chunk = get_chunk(chunk_id);
  const auto chunk_size = uncompressed_chunk.size();

  // new empty chunk
  auto compressed_chunk = std::make_unique<Chunk>();

  auto threads = std::vector<std::thread>{};
  threads.reserve(chunk_size);

  auto compressed_segments = std::vector<std::shared_ptr<BaseSegment>>{chunk_size};
  for (auto column_id = ColumnID{0}; column_id < chunk_size; ++column_id) {
    const auto base_segment = uncompressed_chunk.get_segment(column_id);
    const auto segment_type = _columns[column_id].type;

    threads.emplace_back([column_id, &compressed_segments, &base_segment, &segment_type]() {
      // build dictionary compressed segment
      resolve_data_type(segment_type, [&](const auto data_type_t) {
        using ColumnDataType = typename decltype(data_type_t)::type;
        compressed_segments[column_id] = std::make_shared<DictionarySegment<ColumnDataType>>(base_segment);
      });
    });
    threads.back().detach();
  }

  for (auto& thread : threads) {
    thread.join();
  }

  for (auto column_id = ColumnID{0}; column_id < chunk_size; ++column_id) {
    // add segment to chunk
    compressed_chunk->add_segment(compressed_segments[column_id]);
  }

  // chunk exchange on pointer
  _chunks[chunk_id].reset(compressed_chunk.get());
}

}  // namespace opossum
