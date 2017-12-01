#include "table_scan.hpp"

#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "resolve_type.hpp"
#include "storage/chunk.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/reference_column.hpp"
#include "storage/table.hpp"
#include "storage/value_column.hpp"

namespace opossum {

namespace operators {

template <typename T>
using op = std::function<bool(const T&, const T&)>;

template <typename T>
op<T> get(const ScanType scan_type) {
  switch (scan_type) {
    case ScanType::OpEquals:
      return [](T t1, T t2) { return t1 == t2; };
    case ScanType::OpNotEquals:
      return [](T t1, T t2) { return t1 != t2; };
    case ScanType::OpLessThan:
      return [](T t1, T t2) { return t1 < t2; };
    case ScanType::OpLessThanEquals:
      return [](T t1, T t2) { return t1 <= t2; };
    case ScanType::OpGreaterThan:
      return [](T t1, T t2) { return t1 > t2; };
    case ScanType::OpGreaterThanEquals:
      return [](T t1, T t2) { return t1 >= t2; };
    default:
      throw std::logic_error("scan type not supported");
  }
}
}  // namespace operators

class BaseTableScanImpl {
 public:
  virtual ~BaseTableScanImpl() = default;

  virtual std::shared_ptr<const Table> execute() = 0;
};

class Chunk;

template <typename T>
class TableScanImpl : public BaseTableScanImpl {
 public:
  TableScanImpl(const std::shared_ptr<const Table> table, const ColumnID column_id, const ScanType scan_type,
                const AllTypeVariant search_value)
      : _table(table),
        _column_id(column_id),
        _op(operators::get<T>(scan_type)),
        _search_value(type_cast<T>(search_value)),
        _scan_type(scan_type) {}

  std::shared_ptr<const Table> execute() override {
    const auto result_table = std::make_shared<Table>();

    for (ColumnID index{0}; index < _table->col_count(); ++index) {
      result_table->add_column_definition(_table->column_name(index), _table->column_type(index));
    }

    for (ChunkID chunk_id{0}; chunk_id < _table->chunk_count(); ++chunk_id) {
      const Chunk& chunk = _table->get_chunk(chunk_id);
      const auto column = chunk.get_column(_column_id);

      const auto vc = std::dynamic_pointer_cast<ValueColumn<T>>(column);
      if (vc) {
        auto pos = _scan_value_column(chunk_id, vc);
        auto target_chunk = std::make_shared<Chunk>();
        for (ColumnID col{0}; col < _table->col_count(); ++col) {
          auto new_col = std::make_shared<ReferenceColumn>(_table, col, pos);
          target_chunk->add_column(new_col);
        }
        result_table->emplace_chunk(target_chunk);
      }

      const auto dc = std::dynamic_pointer_cast<DictionaryColumn<T>>(column);
      if (dc) {
          auto pos = _scan_dictionary_column(chunk_id, dc);
          auto target_chunk = std::make_shared<Chunk>();
          for (ColumnID col {0}; col < _table->col_count(); ++col) {
            auto new_col = std::make_shared<ReferenceColumn>(_table, col, pos);
            target_chunk->add_column(new_col);
          }
          result_table->emplace_chunk(target_chunk);
      }

      const auto rc = std::dynamic_pointer_cast<ReferenceColumn>(column);
      if (rc) {
        auto pos = _scan_reference_column(chunk_id, rc);
        auto target_chunk = std::make_shared<Chunk>();
        for (ColumnID col{0}; col < _table->col_count(); ++col) {
          auto new_col = std::make_shared<ReferenceColumn>(rc->referenced_table(), col, pos);
          target_chunk->add_column(new_col);
        }
        result_table->emplace_chunk(target_chunk);
      }
    }

    return result_table;
  }

  std::shared_ptr<PosList> _scan_value_column(const ChunkID chunk_id, const std::shared_ptr<ValueColumn<T>> vc) {
    const auto& content = vc->values();
    const auto pos = std::make_shared<PosList>();
    for (ChunkOffset index{0}; index < content.size(); ++index) {
      const T& value = content[index];
      if (_op(value, _search_value)) {
        pos->push_back(RowID{chunk_id, index});
      }
    }
    return pos;
  }
  std::shared_ptr<PosList> _scan_dictionary_column(const ChunkID chunk_id, const std::shared_ptr<DictionaryColumn<T>> dc) {
    const ValueID search_value_id = dc->lower_bound(_search_value);
    const auto pos = std::make_shared<PosList>();
    // check invalid id

    if(search_value_id == INVALID_VALUE_ID) {
      // -> value not found
      // != < <=  all
      if(_scan_type == ScanType::OpLessThan ||
          _scan_type == ScanType::OpLessThanEquals ||
          _scan_type == ScanType::OpNotEquals){
        for(ChunkOffset index{0}; index < dc->size(); index++){
          pos->push_back(RowID {chunk_id, index});
        }
      }
    } else{
      // -> value found
      if(_search_value == dc->value_by_value_id(search_value_id)){
        // -> exact match
        for(ChunkOffset index{0}; index < dc->size(); index++){
          const T& value = dc->get(index);
          if (_op(value, _search_value)) {
            pos->push_back(RowID {chunk_id, index});
          }
        }
      } else{
        // != all
        if(_scan_type == ScanType::OpNotEquals){
          for(ChunkOffset index{0}; index < dc->size(); index++){
            pos->push_back(RowID {chunk_id, index});
          }
        }

        auto op = _op;
        // > operator swap auf >=
        if(_scan_type == ScanType::OpGreaterThan){
          op = operators::get<T>(ScanType::OpGreaterThanEquals);
        }
        // < <= >= normal scan/check
        if(_scan_type == ScanType::OpGreaterThan ||
            _scan_type == ScanType::OpLessThan ||
            _scan_type == ScanType::OpLessThanEquals ||
            _scan_type == ScanType::OpGreaterThanEquals){
          for(ChunkOffset index{0}; index < dc->size(); index++){
            const T& value = dc->get(index);
            if (op(value, _search_value)) {
              pos->push_back(RowID {chunk_id, index});
            }
          }
        }
      }
    }
    return pos;
  }
  std::shared_ptr<PosList> _scan_reference_column(const ChunkID chunk_id, const std::shared_ptr<ReferenceColumn> rc) {
    const auto pos = std::make_shared<PosList>();
    const auto rp = rc->pos_list();
    for (size_t index = 0; index < rc->size(); ++index) {
      const RowID entry = (*rp)[index];
      const std::shared_ptr<BaseColumn> bc = rc->referenced_table()->get_chunk(entry.chunk_id).get_column(rc->referenced_column_id());
      const std::shared_ptr<ValueColumn<T>> vc = std::dynamic_pointer_cast<ValueColumn<T>>(bc);
      const std::shared_ptr<DictionaryColumn<T>> dc = std::dynamic_pointer_cast<DictionaryColumn<T>>(bc);
      if (vc) {
        const T& val = vc->values()[entry.chunk_offset];
        if (_op(val, _search_value)) {
          pos->push_back(entry);
        }
      } else if(dc) {
          const T& val = dc->get(entry.chunk_offset);
          if(_op(val, _search_value)) {
            pos->push_back(entry);
          }
      } else {
        Assert(false, "scan reference: underlying column was neither VC nor DC or the type parameter was incorrect");
      }
    }
    return pos;
  }

 protected:
  const std::shared_ptr<const Table> _table;
  const ColumnID _column_id;
  const operators::op<T> _op;
  const T _search_value;
  const ScanType _scan_type;
};

TableScan::TableScan(const std::shared_ptr<const AbstractOperator> in, const ColumnID column_id,
                     const ScanType scan_type, const AllTypeVariant search_value)
    : _in(in), _column_id(column_id), _scan_type(scan_type), _search_value(search_value) {}

TableScan::~TableScan() {}

ColumnID TableScan::column_id() const { return _column_id; }

ScanType TableScan::scan_type() const { return _scan_type; }

const AllTypeVariant& TableScan::search_value() const { return _search_value; }

std::shared_ptr<const Table> TableScan::_on_execute() {
  const auto in_table = _in->get_output();
  const auto& type = in_table->column_type(_column_id);
  _impl = make_unique_by_column_type<BaseTableScanImpl, TableScanImpl>(type, in_table, _column_id, _scan_type,
                                                                       _search_value);
  return _impl->execute();
}

}  // namespace opossum
