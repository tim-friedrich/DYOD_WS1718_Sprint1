#include <functional>
#include <memory>

#include "../resolve_type.hpp"
#include "../storage/table.hpp"
#include "table_scan.hpp"

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

template <typename T>
class TableScanImpl : public BaseTableScanImpl {
 public:
  std::shared_ptr<const Table> execute() override {
    // FIXME TODO magic
    return std::make_shared<Table>();
  }
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
  const auto& in_table = _in->get_output();
  const auto& type = in_table->column_type(_column_id);
  _impl = make_unique_by_column_type<BaseTableScanImpl, TableScanImpl>(type);
  return _impl->execute();
}

}  // namespace opossum
