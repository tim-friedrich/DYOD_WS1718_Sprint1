#include <memory>
#include <string>

#include "../storage/storage_manager.hpp"
#include "get_table.hpp"

namespace opossum {

GetTable::GetTable(const std::string& name) : _name(name) {}

const std::string& GetTable::table_name() const { return _name; }

std::shared_ptr<const Table> GetTable::_on_execute() {
  const auto& manager = StorageManager::get();
  return manager.get_table(_name);
}
}  // namespace opossum
