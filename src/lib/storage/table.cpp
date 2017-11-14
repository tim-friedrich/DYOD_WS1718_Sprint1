#include "table.hpp"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "dictionary_column.hpp"
#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "value_column.hpp"

namespace opossum {

Table::Table(const uint32_t chunk_size) : _max_chunk_size(chunk_size) {
  _chunks.emplace_back(std::make_shared<Chunk>());
}

void Table::add_column_definition(const std::string& name, const std::string& type) {
  Assert(row_count() == 0 && chunk_count() == 1,
         "adding column only works on empty tables - padding with NULL values is not supported (yet)");
  Assert(_chunks.at(0)->col_count() == 0, "add_column_definition and add_column are mutually exclusive ");
  _add_column_definition(name, type);
}

void Table::add_column(const std::string& name, const std::string& type) {
  Assert(row_count() == 0 && chunk_count() == 1,
         "adding column only works on empty tables - padding with NULL values is not supported (yet)");
  Assert(_column_names.size() == _chunks.at(0)->col_count(),
         "add_column and add_column_definition are mutually exclusive");

  _add_column_definition(name, type);

  auto column = make_shared_by_column_type<BaseColumn, ValueColumn>(type);
  _chunks.front()->add_column(column);
}

void Table::_add_column_definition(const std::string& name, const std::string& type) {
  _column_names.emplace_back(name);
  _column_types.emplace_back(type);
}

void Table::append(const std::vector<AllTypeVariant> values) {
  DebugAssert(!_chunks.empty(), "chunks must not be empty");
  _add_columns_if_missing();
  auto chunk = _get_or_create_chunk();
  chunk->append(values);
}

void Table::create_new_chunk() {
  auto chunk = std::make_shared<Chunk>();

  for (const auto& type : _column_types) {
    auto column = make_shared_by_column_type<BaseColumn, ValueColumn>(type);
    chunk->add_column(column);
  }

  _chunks.emplace_back(chunk);
}

uint16_t Table::col_count() const { return _column_names.size(); }

uint64_t Table::row_count() const {
  return std::accumulate(_chunks.cbegin(), _chunks.cend(), 0,
                         [](uint64_t sum, const auto chunk) { return sum + chunk->size(); });
}

ChunkID Table::chunk_count() const { return ChunkID(_chunks.size()); }

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  const auto found = std::find(_column_names.cbegin(), _column_names.cend(), column_name);
  DebugAssert(found != _column_names.cend(), "column name not found");
  return ColumnID(std::distance(_column_names.cbegin(), found));
}

uint32_t Table::chunk_size() const { return _max_chunk_size; }

const std::vector<std::string>& Table::column_names() const { return _column_names; }

const std::string& Table::column_name(const ColumnID column_id) const { return _column_names.at(column_id); }

const std::string& Table::column_type(const ColumnID column_id) const { return _column_types.at(column_id); }

Chunk& Table::get_chunk(const ChunkID chunk_id) { return _get_chunk(chunk_id); }

const Chunk& Table::get_chunk(const ChunkID chunk_id) const { return _get_chunk(chunk_id); }

bool Table::_chunk_size_unlimited() const { return _max_chunk_size == 0; }

Chunk& Table::_get_chunk(const ChunkID chunk_id) const {
  const auto chunk = _chunks.at(chunk_id);
  return *chunk;
}

void Table::_add_columns_if_missing() {
  const size_t current_column_count = _chunks.at(0)->col_count();
  if (current_column_count == _column_types.size()) {
    return;
  }

  DebugAssert(current_column_count == 0, "expecting no existing columns");
  for (const auto& type : _column_types) {
    auto column = make_shared_by_column_type<BaseColumn, ValueColumn>(type);
    _chunks.front()->add_column(column);
  }
}

std::shared_ptr<Chunk> Table::_get_or_create_chunk() {
  DebugAssert(!_chunks.empty(), "chunks must not be empty");

  auto last = _chunks.back();
  if (_chunk_size_unlimited()) {
    return last;
  }

  if (last->size() == _max_chunk_size) {
    create_new_chunk();
  }

  return _chunks.back();
}

void Table::compress_chunk(const ChunkID chunk_id) {
  const Chunk& chunk = get_chunk(chunk_id);
  auto compressed_chunk = std::make_shared<Chunk>();
  for (ColumnID index{0}; index < chunk.col_count(); ++index) {
    const auto& type = _column_types.at(index);
    const auto& column = chunk.get_column(index);
    const auto dict_col = make_shared_by_column_type<BaseColumn, DictionaryColumn>(type, column);
    compressed_chunk->add_column(dict_col);
  }
  _chunks[chunk_id] = compressed_chunk;
}

}  // namespace opossum
