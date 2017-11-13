#pragma once

#include <algorithm>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "../utils/performance_warning.hpp"
#include "all_type_variant.hpp"
#include "fitted_attribute_vector.hpp"
#include "type_cast.hpp"
#include "types.hpp"
#include "value_column.hpp"

namespace opossum {

class BaseAttributeVector;
class BaseColumn;

// Even though ValueIDs do not have to use the full width of ValueID (uint32_t), this will also work for smaller ValueID
// types (uint8_t, uint16_t) since after a down-cast INVALID_VALUE_ID will look like their numeric_limit::max()
constexpr ValueID INVALID_VALUE_ID{std::numeric_limits<ValueID::base_type>::max()};

// Dictionary is a specific column type that stores all its values in a vector
template <typename T>
class DictionaryColumn : public BaseColumn {
 public:
  /**
   * Creates a Dictionary column from a given value column.
   */
  explicit DictionaryColumn(const std::shared_ptr<BaseColumn>& base_column) {
    _dictionary = std::make_shared<std::vector<T>>();
    _build_dictionary(base_column);
    _assign_attribute_vector(base_column->size());
    _build_attribute_vector(base_column);
  }

  // return the value at a certain position. If you want to write efficient operators, back off!
  const AllTypeVariant operator[](const size_t i) const override { return get(i); }

  // return the value at a certain position.
  const T get(const size_t i) const { return _dictionary->at(_attribute_vector->get(i)); }

  // dictionary columns are immutable
  void append(const AllTypeVariant&) override {}

  // returns an underlying dictionary
  std::shared_ptr<const std::vector<T>> dictionary() const { return _dictionary; }

  // returns an underlying data structure
  std::shared_ptr<const BaseAttributeVector> attribute_vector() const { return _attribute_vector; }

  // return the value represented by a given ValueID
  const T& value_by_value_id(ValueID value_id) const { return _dictionary->at(value_id); }

  // returns the first value ID that refers to a value >= the search value
  // returns INVALID_VALUE_ID if all values are smaller than the search value
  ValueID lower_bound(T value) const { return _to_value_id(value); }

  // same as lower_bound(T), but accepts an AllTypeVariant
  ValueID lower_bound(const AllTypeVariant& value) const { return _to_value_id(type_cast<T>(value)); }

  // returns the first value ID that refers to a value > the search value
  // returns INVALID_VALUE_ID if all values are smaller than or equal to the search value
  ValueID upper_bound(T value) const {
    const auto found = std::upper_bound(_dictionary->cbegin(), _dictionary->cend(), value);
    if (found == _dictionary->cend()) {
      return INVALID_VALUE_ID;
    }
    return ValueID(found - _dictionary->cbegin());
  }

  // same as upper_bound(T), but accepts an AllTypeVariant
  ValueID upper_bound(const AllTypeVariant& value) const { return upper_bound(type_cast<T>(value)); }

  // return the number of unique_values (dictionary entries)
  size_t unique_values_count() const { return _dictionary->size(); }

  // return the number of entries
  size_t size() const override { return _attribute_vector->size(); }

 protected:
  std::shared_ptr<std::vector<T>> _dictionary;
  std::shared_ptr<BaseAttributeVector> _attribute_vector;

 protected:
  void _build_dictionary(const std::shared_ptr<BaseColumn>& base_column) {
    const auto value_column = std::dynamic_pointer_cast<ValueColumn<T>>(base_column);
    if (value_column) {
      (*_dictionary) = value_column->values();
    } else {
      PerformanceWarning("Element wise copy of column");
      for (size_t index = 0; index < base_column->size(); ++index) {
        _dictionary->push_back(type_cast<T>((*base_column)[index]));
      }
    }
    std::sort(_dictionary->begin(), _dictionary->end());
    _dictionary->erase(std::unique(_dictionary->begin(), _dictionary->end()), _dictionary->end());
    _dictionary->shrink_to_fit();
  }

  void _assign_attribute_vector(const size_t size) {
    Assert(size < std::numeric_limits<uint64_t>::max(), "Number of attributes out of range");
    if (size < std::numeric_limits<uint8_t>::max()) {
      _attribute_vector = std::make_shared<FittedAttributeVector<uint8_t>>(size);
    } else if (size < std::numeric_limits<uint16_t>::max()) {
      _attribute_vector = std::make_shared<FittedAttributeVector<uint16_t>>(size);
    } else if (size < std::numeric_limits<uint32_t>::max()) {
      _attribute_vector = std::make_shared<FittedAttributeVector<uint32_t>>(size);
    } else {
      _attribute_vector = std::make_shared<FittedAttributeVector<uint64_t>>(size);
    }
  }

  void _build_attribute_vector(const std::shared_ptr<BaseColumn>& base_column) {
    for (size_t index = 0; index < base_column->size(); ++index) {
      const T& value = type_cast<T>((*base_column)[index]);
      _attribute_vector->set(index, _to_value_id(value));
    }
  }

  const ValueID _to_value_id(const T& value) const {
    const auto found = std::lower_bound(_dictionary->cbegin(), _dictionary->cend(), value);
    if (found == _dictionary->cend()) {
      return INVALID_VALUE_ID;
    }
    return ValueID(found - _dictionary->cbegin());
  }
};

}  // namespace opossum
