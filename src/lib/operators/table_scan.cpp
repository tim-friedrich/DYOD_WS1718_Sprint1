#include "table_scan.hpp"

#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "resolve_type.hpp"
#include "storage/reference_column.hpp"
#include "storage/dictionary_column.hpp"
#include "storage/table.hpp"


namespace opossum {

namespace operators {

template <typename T>
using op = std::function<bool(const T&, const T&)>;

template <typename T>
op<T> get_comparator(const ScanType scan_type) {
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

// T = datatype
template <typename T>
class TableScanImpl : public BaseTableScanImpl {
 public:
  TableScanImpl<T>(const std::shared_ptr<AbstractOperator> in, const ColumnID column_id, const ScanType scan_type,
                   const AllTypeVariant search_value)
      : _in(in), _column_id(column_id), _scan_type(scan_type), _search_value(search_value) {}

  std::shared_ptr<const Table> execute() override {
    T search_value = type_cast<T>(_search_value);
    auto pos_list = std::make_shared<PosList>();
    auto result_table = std::make_shared<Table>();
    const auto in_table = _in->get_output();

    for (ChunkID chunk_id{0}; in_table->chunk_count(); chunk_id++) {
      const Chunk& chunk = in_table->get_chunk(chunk_id);
      auto base_column = chunk.get_column(_column_id);
      auto value_column = std::dynamic_pointer_cast<ValueColumn<T>>(base_column);

      if (value_column != nullptr) {
        const std::vector<T>& col_values = value_column->values();
        for (ChunkOffset chunk_offset; chunk_offset < col_values.size(); chunk_offset++) {
          auto comparing_function = operators::get_comparator<T>(_scan_type);
          if (comparing_function(col_values[chunk_offset], search_value)) {
            pos_list->emplace_back(RowID{chunk_id, chunk_offset});
          }
        }
      }

      auto dictionary_column = std::dynamic_pointer_cast<DictionaryColumn<T>>(base_column);
      if (dictionary_column != nullptr) {
        // FIXME TODO magic
      }
    }

    auto ref_column = std::make_shared<ReferenceColumn>(in_table, _column_id, pos_list);
    auto result_chunk = Chunk();
    result_chunk.add_column(ref_column);
    result_table->emplace_chunk(std::move(result_chunk));
    return result_table;
  }

 protected:
  const std::shared_ptr<AbstractOperator> _in;
  const ColumnID _column_id;
  const ScanType _scan_type;
  const AllTypeVariant _search_value;
};

TableScan::TableScan(const std::shared_ptr<AbstractOperator> in, const ColumnID column_id, const ScanType scan_type,
                     const AllTypeVariant search_value)
    : _in(in), _column_id(column_id), _scan_type(scan_type), _search_value(search_value) {}

TableScan::~TableScan() {}

ColumnID TableScan::column_id() const { return _column_id; }

ScanType TableScan::scan_type() const { return _scan_type; }

const AllTypeVariant& TableScan::search_value() const { return _search_value; }

std::shared_ptr<const Table> TableScan::_on_execute() {
  _in->execute();
  const auto in_table = _in->get_output();
  const auto& type = in_table->column_type(_column_id);
  _impl =
      make_unique_by_column_type<BaseTableScanImpl, TableScanImpl>(type, _in, _column_id, _scan_type, _search_value);
  return _impl->execute();
}

}  // namespace opossum
