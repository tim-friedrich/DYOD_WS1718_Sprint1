#include "reference_column.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "utils/assert.hpp"

namespace opossum {
ReferenceColumn::ReferenceColumn(const std::shared_ptr<const Table> referenced_table,
                                 const ColumnID referenced_column_id, const std::shared_ptr<const PosList> pos)
    : _referenced_table(referenced_table), _referenced_column_id(referenced_column_id), _pos(pos) {}

const AllTypeVariant ReferenceColumn::operator[](const size_t i) const {
  const RowID row = _pos->at(i);
  const auto& chunk = _referenced_table->get_chunk(row.chunk_id);
  const auto column = chunk.get_column(_referenced_column_id);
  return (*column)[row.chunk_offset];
}

size_t ReferenceColumn::size() const { return _pos->size(); }

const std::shared_ptr<const PosList> ReferenceColumn::pos_list() const { return _pos; }
const std::shared_ptr<const Table> ReferenceColumn::referenced_table() const { return _referenced_table; }

ColumnID ReferenceColumn::referenced_column_id() const { return _referenced_column_id; }
}  // namespace opossum
