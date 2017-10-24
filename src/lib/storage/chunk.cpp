#include <iomanip>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "base_column.hpp"
#include "chunk.hpp"

#include "utils/assert.hpp"

namespace opossum {

void Chunk::add_column(std::shared_ptr<BaseColumn> column) { this->columns.push_back(column); }

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  // Implementation goes here
}

std::shared_ptr<BaseColumn> Chunk::get_column(ColumnID column_id) const {
  // Implementation goes here
  return nullptr;
}

uint16_t Chunk::col_count() const { return this->columns.size(); }

uint32_t Chunk::size() const {
  if (this->columns.empty()) {
    return 0;
  } else {
    return this->columns.at(0)->size();
  }
}

}  // namespace opossum
