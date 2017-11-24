#include "table.hpp"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "value_column.hpp"

#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

Table::Table(const uint32_t chunk_size) : _max_chunk_size(chunk_size) {
  this->_chunks.push_back(std::make_shared<Chunk>());
}

void Table::add_column_definition(const std::string& name, const std::string& type) {
  this->_column_names.push_back(name);
  this->_column_types.push_back(type);
}

void Table::add_column(const std::string& name, const std::string& type) {
  DebugAssert(this->row_count() == 0 && this->chunk_count() == 1,
              "adding column only works on empty tables - padding with NULL values is not supported (yet)");
  this->add_column_definition(name, type);

  auto column = make_shared_by_column_type<BaseColumn, ValueColumn>(type);
  this->_chunks.back()->add_column(column);
}

void Table::append(std::vector<AllTypeVariant> values) {
  DebugAssert(!this->_chunks.empty(), "chunks must not be empty");
  auto chunk = _get_insert_chunk();
  chunk->append(values);
}

void Table::create_new_chunk() {
  auto chunk = std::make_shared<Chunk>();

  for (auto& type : this->_column_types) {
    auto column = make_shared_by_column_type<BaseColumn, ValueColumn>(type);
    chunk->add_column(column);
  }

  this->_chunks.push_back(chunk);
}

uint16_t Table::col_count() const { return this->_column_names.size(); }

uint64_t Table::row_count() const {
  return std::accumulate(this->_chunks.cbegin(), this->_chunks.cend(), 0,
                         [](uint64_t sum, const auto chunk) { return sum + chunk->size(); });
}

ChunkID Table::chunk_count() const { return ChunkID(this->_chunks.size()); }

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  auto found = std::find(this->_column_names.cbegin(), this->_column_names.cend(), column_name);
  DebugAssert(found != this->_column_names.cend(), "column name not found");
  return ColumnID(std::distance(this->_column_names.cbegin(), found));
}

uint32_t Table::chunk_size() const { return this->_max_chunk_size; }

const std::vector<std::string>& Table::column_names() const { return this->_column_names; }

const std::string& Table::column_name(ColumnID column_id) const { return this->_column_names.at(column_id); }

const std::string& Table::column_type(ColumnID column_id) const { return this->_column_types.at(column_id); }

Chunk& Table::get_chunk(ChunkID chunk_id) { return this->_get_chunk(chunk_id); }

const Chunk& Table::get_chunk(ChunkID chunk_id) const { return this->_get_chunk(chunk_id); }

bool Table::_chunk_size_unlimited() const { return this->_max_chunk_size == 0; }

Chunk& Table::_get_chunk(ChunkID chunk_id) const {
  auto chunk = this->_chunks.at(chunk_id);
  return *chunk;
}

std::shared_ptr<Chunk> Table::_get_insert_chunk() {
  DebugAssert(!this->_chunks.empty(), "chunks must not be empty");

  auto last = this->_chunks.back();
  if (this->_chunk_size_unlimited()) {
    return last;
  }

  if (last->size() == this->_max_chunk_size) {
    this->create_new_chunk();
  }

  return this->_chunks.back();
}

void emplace_chunk(Chunk chunk) {
  // Implementation goes here
}

void Table::compress_chunk(ChunkID chunk_id) { throw std::runtime_error("TODO"); }

}  // namespace opossum
