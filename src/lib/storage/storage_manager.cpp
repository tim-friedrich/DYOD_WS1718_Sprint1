#include "storage_manager.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "utils/assert.hpp"

namespace opossum {

StorageManager& StorageManager::get() { return *StorageManager::_instance_ptr(); }

void StorageManager::add_table(const std::string& name, std::shared_ptr<Table> table) { this->_tables[name] = table; }

void StorageManager::drop_table(const std::string& name) {
  size_t erased = this->_tables.erase(name);
  DebugAssert(erased == 1, "exactly one table dropped");
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const { return this->_tables.at(name); }

bool StorageManager::has_table(const std::string& name) const { return this->_tables.count(name) > 0; }

std::vector<std::string> StorageManager::table_names() const {
  std::vector<std::string> keys;
  keys.reserve(this->_tables.size());
  for (auto& entry : this->_tables) {
    keys.push_back(entry.first);
  }
  return keys;
}

void StorageManager::print(std::ostream& out) const {
  for (auto& entry : this->_tables) {
    this->_print_table(out, entry.first, entry.second);
  }
}

void StorageManager::reset() {
  auto& ptr = StorageManager::_instance_ptr();
  ptr = std::make_unique<StorageManager>();
}

void StorageManager::_print_table(std::ostream& out, const std::string& name, std::shared_ptr<Table> table) const {
  out << "Table \"" << name << "\": ";
  out << table->col_count() << " columns, ";
  out << table->row_count() << " rows, ";
  out << table->chunk_count() << " chunks" << std::endl;
}

std::unique_ptr<StorageManager>& StorageManager::_instance_ptr() {
  static std::unique_ptr<StorageManager> ptr = std::make_unique<StorageManager>();
  return ptr;
}

}  // namespace opossum
