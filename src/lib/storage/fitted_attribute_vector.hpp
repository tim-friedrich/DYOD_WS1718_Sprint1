#pragma once
#include <memory>
#include <vector>

#include "../types.hpp"
#include "../utils/assert.hpp"
#include "base_attribute_vector.hpp"

namespace opossum {
template <typename T>
class FittedAttributeVector : public BaseAttributeVector {
 public:
  explicit FittedAttributeVector(const size_t size) { _content = std::make_shared<std::vector<T>>(size); }
  FittedAttributeVector(FittedAttributeVector&&) = default;
  FittedAttributeVector& operator=(FittedAttributeVector&&) = default;
  virtual ~FittedAttributeVector() = default;

  ValueID get(const size_t i) const override { return ValueID(_content->at(i)); }
  void set(const size_t i, const ValueID value_id) override {
    DebugAssert(i < _content->size(), "index exceeds AV size");
    (*_content)[i] = value_id;
  };
  size_t size() const override { return _content->size(); };
  AttributeVectorWidth width() const override { return AttributeVectorWidth(sizeof(T)); };

 protected:
  std::shared_ptr<std::vector<T>> _content;
};
}  // namespace opossum
