#pragma once
#include <memory>
#include <vector>

#include "../types.hpp"
#include "../utils/assert.hpp"
#include "base_attribute_vector.hpp"

namespace opossum {

class SimpleAttributeVector : public BaseAttributeVector {
 public:
  explicit SimpleAttributeVector(const size_t size);
  SimpleAttributeVector(SimpleAttributeVector&&) = default;
  SimpleAttributeVector& operator=(SimpleAttributeVector&&) = default;
  virtual ~SimpleAttributeVector() = default;

  ValueID get(const size_t i) const override;
  void set(const size_t i, const ValueID value_id) override;
  size_t size() const override;
  AttributeVectorWidth width() const override;

 protected:
  std::shared_ptr<std::vector<uint32_t>> _content;
};
}  // namespace opossum
