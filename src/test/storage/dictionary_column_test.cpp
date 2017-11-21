#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "../../lib/resolve_type.hpp"
#include "../../lib/storage/base_column.hpp"
#include "../../lib/storage/dictionary_column.hpp"
#include "../../lib/storage/value_column.hpp"
namespace opossum {
class StorageDictionaryColumnTest : public ::testing::Test {
 protected:
  std::shared_ptr<ValueColumn<int>> vc_int = std::make_shared<ValueColumn<int>>();
  std::shared_ptr<ValueColumn<std::string>> vc_str = std::make_shared<ValueColumn<std::string>>();
};

TEST_F(StorageDictionaryColumnTest, CompressColumnString) {
  vc_str->append("Bill");
  vc_str->append("Steve");
  vc_str->append("Alexander");
  vc_str->append("Steve");
  vc_str->append("Hasso");
  vc_str->append("Bill");

  auto col = make_shared_by_column_type<BaseColumn, DictionaryColumn>("string", vc_str);
  auto dict_col = std::dynamic_pointer_cast<DictionaryColumn<std::string>>(col);

  // Test attribute_vector size
  EXPECT_EQ(dict_col->size(), 6u);

  // Test dictionary size (uniqueness)
  EXPECT_EQ(dict_col->unique_values_count(), 4u);

  // Test sorting
  auto dict = dict_col->dictionary();
  EXPECT_EQ((*dict)[0], "Alexander");
  EXPECT_EQ((*dict)[1], "Bill");
  EXPECT_EQ((*dict)[2], "Hasso");
  EXPECT_EQ((*dict)[3], "Steve");
}

TEST_F(StorageDictionaryColumnTest, LowerUpperBound) {
  for (int i = 0; i <= 10; i += 2) vc_int->append(i);
  auto col = make_shared_by_column_type<BaseColumn, DictionaryColumn>("int", vc_int);
  auto dict_col = std::dynamic_pointer_cast<DictionaryColumn<int>>(col);

  EXPECT_EQ(dict_col->lower_bound(4), (ValueID)2);
  EXPECT_EQ(dict_col->upper_bound(4), (ValueID)3);

  EXPECT_EQ(dict_col->lower_bound(5), (ValueID)3);
  EXPECT_EQ(dict_col->upper_bound(5), (ValueID)3);

  EXPECT_EQ(dict_col->lower_bound(15), INVALID_VALUE_ID);
  EXPECT_EQ(dict_col->upper_bound(15), INVALID_VALUE_ID);
}

struct StringColumn : public BaseColumn {
  explicit StringColumn(std::vector<std::string> stuff) : _content(stuff) {}
  virtual ~StringColumn() = default;
  const AllTypeVariant operator[](const size_t i) const override { return _content[i]; }
  void append(const AllTypeVariant& val) override { _content.emplace_back(type_cast<std::string>(val)); }
  size_t size() const override { return _content.size(); }
  std::vector<std::string> _content;
};

TEST_F(StorageDictionaryColumnTest, CompressionSlowPath) {
  std::vector<std::string> rows{"a", "b", "b"};
  auto col = std::make_shared<StringColumn>(rows);

  auto base_col = make_shared_by_column_type<BaseColumn, DictionaryColumn>("string", col);
  auto dict_col = std::dynamic_pointer_cast<DictionaryColumn<std::string>>(base_col);

  EXPECT_EQ(dict_col->size(), 3u);
  EXPECT_EQ(dict_col->unique_values_count(), 2u);
}

TEST_F(StorageDictionaryColumnTest, _assign_attribute_vector) {
  vc_str->append("Bill");
  auto col = make_shared_by_column_type<BaseColumn, DictionaryColumn>("string", vc_str);
  auto dict_col = std::dynamic_pointer_cast<DictionaryColumn<std::string>>(col);

  EXPECT_EQ(sizeof(uint8_t), dict_col->attribute_vector()->size());

  vc_str = std::make_shared<ValueColumn<std::string>>();
  for (uint16_t i = 0; i < std::numeric_limits<uint8_t>::max() + 1; i++) {
    vc_str->append("Bill");
  }

  col = make_shared_by_column_type<BaseColumn, DictionaryColumn>("string", vc_str);
  dict_col = std::dynamic_pointer_cast<DictionaryColumn<std::string>>(col);

  EXPECT_EQ(sizeof(uint16_t), dict_col->attribute_vector()->width());

  vc_str = std::make_shared<ValueColumn<std::string>>();
  for(uint32_t i=0; i<std::numeric_limits<uint16_t>::max() + 1; i++){
    vc_str->append("Bill");
  }

  col = make_shared_by_column_type<BaseColumn, DictionaryColumn>("string", vc_str);
  dict_col = std::dynamic_pointer_cast<DictionaryColumn<std::string>>(col);

  EXPECT_EQ(sizeof(uint32_t), dict_col->attribute_vector()->width());
}

TEST_F(StorageDictionaryColumnTest, value_by_value_id) {
  for (int i = 0; i <= 10; i += 2) vc_int->append(i);
  auto col = make_shared_by_column_type<BaseColumn, DictionaryColumn>("int", vc_int);
  auto dict_col = std::dynamic_pointer_cast<DictionaryColumn<int>>(col);

  EXPECT_EQ(dict_col->value_by_value_id(ValueID(0)), 0);
  EXPECT_EQ(dict_col->value_by_value_id(ValueID(2)), 4);
}

TEST_F(StorageDictionaryColumnTest, get) {
  for (int i = 0; i <= 10; i += 2) vc_int->append(i);
  auto col = make_shared_by_column_type<BaseColumn, DictionaryColumn>("int", vc_int);
  auto dict_col = std::dynamic_pointer_cast<DictionaryColumn<int>>(col);

  EXPECT_EQ((*dict_col)[0], AllTypeVariant{0});
  EXPECT_EQ((*dict_col)[2], AllTypeVariant{4});
}

TEST_F(StorageDictionaryColumnTest, immutable) {
  auto col = make_shared_by_column_type<BaseColumn, DictionaryColumn>("string", vc_str);

  EXPECT_THROW(col->append("anything"), std::logic_error);
}
}  // namespace opossum
