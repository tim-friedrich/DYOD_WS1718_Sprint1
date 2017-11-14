#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

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
  t.get_chunk(ChunkID{0});
  // TODO(anyone): Do we want checks here?
  //EXPECT_THROW(t.get_chunk(ChunkID{"q"}), std::exception);
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
  t.get_chunk(ChunkID{1});
  const auto& chunk = static_cast<const Table&>(t).get_chunk(ChunkID{1});
  EXPECT_EQ(chunk.size(), 1u);
  EXPECT_THROW(t.get_chunk(ChunkID{2}), std::exception);
}

TEST_F(StorageTableTest, ColCount) { EXPECT_EQ(t.col_count(), 2u); }

TEST_F(StorageTableTest, RowCount) {
  EXPECT_EQ(t.row_count(), 0u);
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
  EXPECT_EQ(t.row_count(), 3u);
}

TEST_F(StorageTableTest, GetColumnNames) {
  EXPECT_EQ(t.column_names().size(), 2u);
  EXPECT_EQ(t.column_names().at(0), "col_1");
  EXPECT_EQ(t.column_names().at(1), "col_2");
  EXPECT_THROW(t.column_names().at(2), std::exception);
}

TEST_F(StorageTableTest, GetColumnName) {
  EXPECT_EQ(t.column_name(ColumnID{0}), "col_1");
  EXPECT_EQ(t.column_name(ColumnID{1}), "col_2");
  // TODO(anyone): Do we want checks here?
  EXPECT_THROW(t.column_name(ColumnID{2}), std::exception);
}

TEST_F(StorageTableTest, GetColumnType) {
  EXPECT_EQ(t.column_type(ColumnID{0}), "int");
  EXPECT_EQ(t.column_type(ColumnID{1}), "string");
  // TODO(anyone): Do we want checks here?
  EXPECT_THROW(t.column_type(ColumnID{2}), std::exception);
}

TEST_F(StorageTableTest, GetColumnIdByName) {
  EXPECT_EQ(t.column_id_by_name("col_2"), 1u);
  EXPECT_THROW(t.column_id_by_name("no_column_name"), std::exception);
}

TEST_F(StorageTableTest, GetChunkSize) { EXPECT_EQ(t.chunk_size(), 2u); }

// DefineColumnThenAdd, AddColumnThenDefine:
// Those two methods should be mutual exclusive.
// All columns are created either eagerly or lazily in batch mode upon append.
// Trying to mix both approaches should result in error.
TEST_F(StorageTableTest, DefineColumnThenAdd) {
  Table table;

  table.add_column_definition("foo", "int");

  EXPECT_THROW(table.add_column("bar", "string"), std::exception);
}

TEST_F(StorageTableTest, AddColumnThenDefine) {
  Table table;

  table.add_column("bar", "string");

  EXPECT_THROW(table.add_column_definition("foo", "int"), std::exception);
}

// col_count() should count schema columns (= even those not yet created)
TEST_F(StorageTableTest, CreateColumnsLazily) {
  Table table;

  table.add_column_definition("foo", "string");
  table.add_column_definition("bar", "int");

  EXPECT_EQ(table.col_count(), 2u);

  table.append({"spam", 3});

  EXPECT_EQ(table.col_count(), 2u);
  EXPECT_EQ(table.row_count(), 1u);
}


TEST_F(StorageTableTest, CompressChunk) {
  t.get_chunk(ChunkID{0});
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
  EXPECT_EQ(t.get_chunk(ChunkID{0}).size(), 2u);
  t.compress_chunk(ChunkID{0});
  EXPECT_EQ(t.get_chunk(ChunkID{0}).size(), 2u);

  EXPECT_EQ(t.get_chunk(ChunkID{1}).size(), 1u);
  t.compress_chunk(ChunkID{1});
  EXPECT_EQ(t.get_chunk(ChunkID{1}).size(), 1u);

  EXPECT_EQ(t.row_count(), 3u);
  EXPECT_EQ(t.col_count(), 2u);
}

}  // namespace opossum


