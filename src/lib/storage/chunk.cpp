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

void Chunk::add_column(std::shared_ptr<BaseColumn> column) { this->_columns.push_back(column); }

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  DebugAssert(values.size() == this->_columns.size(), "append: each column must have exactly one value assigned");

  for (unsigned int index = 0; index < this->_columns.size(); ++index) {
    this->_columns[index]->append(values[index]);
  }
}

std::shared_ptr<BaseColumn> Chunk::get_column(ColumnID column_id) const { return this->_columns.at(column_id); }

uint16_t Chunk::col_count() const { return this->_columns.size(); }

uint32_t Chunk::size() const {
  if (this->_columns.empty()) {
    return 0;
  } else {
    return this->_columns.at(0)->size();
  }
}

}  // namespace opossum
