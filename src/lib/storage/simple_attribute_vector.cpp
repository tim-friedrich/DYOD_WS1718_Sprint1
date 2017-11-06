#include <memory>
#include <vector>

#include "simple_attribute_vector.hpp"

namespace opossum {

SimpleAttributeVector::SimpleAttributeVector(const size_t size) {
  _content = std::make_shared<std::vector<uint32_t>>(size);
}

ValueID SimpleAttributeVector::get(const size_t i) const { return ValueID(_content->at(i)); }

void SimpleAttributeVector::set(const size_t i, const ValueID value_id) {
  DebugAssert(i < _content->size(), "index exceeds AV size");
  (*_content)[i] = value_id;
}

size_t SimpleAttributeVector::size() const { return _content->size(); }

AttributeVectorWidth SimpleAttributeVector::width() const { return AttributeVectorWidth(4); }
}  // namespace opossum
