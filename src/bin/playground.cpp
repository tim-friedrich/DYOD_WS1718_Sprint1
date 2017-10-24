#include <iostream>
#include <memory>
#include <string>

#include "../lib/storage/storage_manager.hpp"
#include "../lib/storage/table.hpp"
#include "../lib/utils/assert.hpp"

int main() {
  opossum::Assert(true, "We can use opossum files here :)");

  auto table = std::make_shared<opossum::Table>(2);

  table->add_column("pk", "int");
  table->add_column("name", "string");

  table->append({1, "foo"});
  table->append({2, "bar"});
  table->append({3, "spam"});
  table->append({4, "eggs"});
  table->append({5, "elephant"});

  auto& manager = opossum::StorageManager::get();
  manager.add_table(std::string("foobar"), table);

  // expected output: "Table "foobar": 2 columns, 5 rows, 3 chunks"
  manager.print(std::cout);

  return 0;
}
