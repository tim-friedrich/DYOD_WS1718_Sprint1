#include "storage_manager.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "utils/assert.hpp"

namespace opossum {

StorageManager& StorageManager::get() {
  static StorageManager instance;
  return instance;
}

void StorageManager::add_table(const std::string& name, std::shared_ptr<Table> table) {
  Assert(_tables.count(name) == 0, "table already exists");
  _tables[name] = table;
}

void StorageManager::drop_table(const std::string& name) {
  const size_t erased = _tables.erase(name);
  Assert(erased == 1, "zero or more than one table has been removed");
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const { return _tables.at(name); }

bool StorageManager::has_table(const std::string& name) const { return _tables.count(name) > 0; }

std::vector<std::string> StorageManager::table_names() const {
  std::vector<std::string> keys;
  keys.reserve(_tables.size());
  for (auto& entry : _tables) {
    keys.emplace_back(entry.first);
  }
  return keys;
}

void StorageManager::print(std::ostream& out) const {
  for (const auto& entry : _tables) {
    _print_table(out, entry.first, entry.second);
  }
}

void StorageManager::reset() { get() = StorageManager(); }

void StorageManager::_print_table(std::ostream& out, const std::string& name,
                                  const std::shared_ptr<const Table> table) const {
  out << "Table \"" << name << "\": ";
  out << table->col_count() << " columns, ";
  out << table->row_count() << " rows, ";
  out << table->chunk_count() << " chunks" << std::endl;
}
}  // namespace opossum
