#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/storage/dictionary_segment.hpp"
#include "../lib/resolve_type.hpp"
#include "../lib/storage/table.hpp"

namespace opossum {

class StorageTableTest : public BaseTest {
 protected:
  void SetUp() override {
    t.add_column("col_1", "int");
    t.add_column("col_2", "string");
  }

  Table t{2};
};

TEST_F(StorageTableTest, ChunkCount) {
  EXPECT_EQ(t.chunk_count(), 1u);
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
  EXPECT_EQ(t.chunk_count(), 2u);
}

TEST_F(StorageTableTest, GetChunk) {
  auto& first_chunk = t.get_chunk(ChunkID{0});
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
  EXPECT_EQ(first_chunk.size(), 2);
  const auto& second_chunk = std::as_const(t).get_chunk(ChunkID{1});
  EXPECT_EQ(second_chunk.size(), 1);

  if constexpr (HYRISE_DEBUG) {
    EXPECT_THROW(t.get_chunk(ChunkID{42}), std::exception);
  }
}

TEST_F(StorageTableTest, ColumnCount) { EXPECT_EQ(t.column_count(), 2u); }

TEST_F(StorageTableTest, RowCount) {
  EXPECT_EQ(t.row_count(), 0u);
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
  EXPECT_EQ(t.row_count(), 3u);
}

TEST_F(StorageTableTest, GetColumnName) {
  EXPECT_EQ(t.column_name(ColumnID{0}), "col_1");
  EXPECT_EQ(t.column_name(ColumnID{1}), "col_2");
  if constexpr (HYRISE_DEBUG) {
    EXPECT_THROW(t.column_name(ColumnID{2}), std::exception);
  }
}

TEST_F(StorageTableTest, GetColumnNames) {
  auto column_names = t.column_names();
  EXPECT_EQ(column_names.size(), 2);
  EXPECT_EQ(column_names[0], "col_1");
  EXPECT_EQ(column_names[1], "col_2");
}

TEST_F(StorageTableTest, GetColumnType) {
  EXPECT_EQ(t.column_type(ColumnID{0}), "int");
  EXPECT_EQ(t.column_type(ColumnID{1}), "string");
  if constexpr (HYRISE_DEBUG) {
    EXPECT_THROW(t.column_type(ColumnID{2}), std::exception);
  }
}

TEST_F(StorageTableTest, GetColumnIdByName) {
  EXPECT_EQ(t.column_id_by_name("col_2"), 1u);
  EXPECT_THROW(t.column_id_by_name("no_column_name"), std::exception);
}

TEST_F(StorageTableTest, GetChunkSize) { EXPECT_EQ(t.target_chunk_size(), 2u); }

TEST_F(StorageTableTest, CompressChunk) {
  t.append({0, "Alexander"});
  t.append({1, "Alexander"});
  t.compress_chunk(ChunkID{0});
  auto& compressed_chunk = (t.get_chunk(ChunkID{0}));
  auto compressed_segment = compressed_chunk.get_segment(ColumnID{1});
  auto dictionary_segment = std::static_pointer_cast<DictionarySegment<std::string>>(compressed_segment);
  EXPECT_NE(dictionary_segment, nullptr);
  EXPECT_EQ(dictionary_segment->get(0), "Alexander");
  EXPECT_EQ(dictionary_segment->get(1), "Alexander");
}

}  // namespace opossum
