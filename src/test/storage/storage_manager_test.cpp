#include <memory>
#include <sstream>
#include <string>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/storage/storage_manager.hpp"
#include "../lib/storage/table.hpp"

namespace opossum {

class StorageStorageManagerTest : public BaseTest {
 protected:
  void SetUp() override {
    auto& sm = StorageManager::get();
    auto t1 = std::make_shared<Table>();
    auto t2 = std::make_shared<Table>(4);

    sm.add_table("first_table", t1);
    sm.add_table("second_table", t2);
  }
};

TEST_F(StorageStorageManagerTest, GetTable) {
  auto& sm = StorageManager::get();
  auto t3 = sm.get_table("first_table");
  auto t4 = sm.get_table("second_table");
  EXPECT_THROW(sm.get_table("third_table"), std::exception);
}

TEST_F(StorageStorageManagerTest, DropTable) {
  auto& sm = StorageManager::get();
  sm.drop_table("first_table");
  EXPECT_THROW(sm.get_table("first_table"), std::exception);
  EXPECT_THROW(sm.drop_table("first_table"), std::exception);
}

TEST_F(StorageStorageManagerTest, ResetTable) {
  StorageManager::reset();
  auto& sm = StorageManager::get();
  EXPECT_THROW(sm.get_table("first_table"), std::exception);
}

TEST_F(StorageStorageManagerTest, DoesNotHaveTable) {
  auto& sm = StorageManager::get();
  EXPECT_EQ(sm.has_table("third_table"), false);
}

TEST_F(StorageStorageManagerTest, HasTable) {
  auto& sm = StorageManager::get();
  EXPECT_EQ(sm.has_table("first_table"), true);
}

TEST_F(StorageStorageManagerTest, TwiceGetIsSameObject) {
  auto& sm1 = StorageManager::get();
  auto& sm2 = StorageManager::get();

  EXPECT_EQ(&sm1, &sm2);
}

TEST_F(StorageStorageManagerTest, AfterResetStateVanished) {
  auto& sm = StorageManager::get();
  EXPECT_EQ(sm.table_names().size(), 2u);

  StorageManager::reset();

  EXPECT_EQ(sm.table_names().size(), 0u);
}

TEST_F(StorageStorageManagerTest, AddTableShouldNotOverwrite) {
  auto& sm = StorageManager::get();
  auto table = std::make_shared<Table>();
  std::string table_name("spam");

  sm.add_table(table_name, table);

  EXPECT_THROW(sm.add_table(table_name, table), std::exception);
}

TEST_F(StorageStorageManagerTest, StorageManagerInfo) {
  auto& sm = StorageManager::get();

  auto table = sm.get_table("first_table");
  table->add_column("pk", "int");
  table->add_column("name", "string");

  table->append({1, "foo"});
  table->append({2, "bar"});
  table->append({3, "spam"});

  sm.get_table("second_table")->create_new_chunk();

  std::stringstream ss;
  sm.print(ss);

  EXPECT_EQ(
      "Table \"first_table\": 2 columns, 3 rows, 1 chunks\n"
      "Table \"second_table\": 0 columns, 0 rows, 2 chunks\n",
      ss.str());
}

TEST_F(StorageStorageManagerTest, TableNames) {
  auto& sm = StorageManager::get();

  auto names = sm.table_names();

  std::vector<std::string> expected {"first_table", "second_table"};
  EXPECT_EQ(expected, names);
}
}  // namespace opossum
