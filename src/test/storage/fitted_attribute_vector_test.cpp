#include <memory>
#include <string>

#include "gtest/gtest.h"

#include "../../lib/resolve_type.hpp"
#include "../../lib/storage/fitted_attribute_vector.hpp"

class FittedAttributeVectorTest : public ::testing::Test {
 protected:
  const size_t size = 10;
  opossum::FittedAttributeVector<uint8_t> _vector_8 = opossum::FittedAttributeVector<uint8_t>(size);
  opossum::FittedAttributeVector<uint16_t> _vector_16 = opossum::FittedAttributeVector<uint16_t>(size);
  opossum::FittedAttributeVector<uint32_t> _vector_32 = opossum::FittedAttributeVector<uint32_t>(size);
  opossum::FittedAttributeVector<uint64_t> _vector_64 = opossum::FittedAttributeVector<uint64_t>(size);
};

TEST_F(FittedAttributeVectorTest, width) {
  EXPECT_EQ(sizeof(uint8_t), _vector_8.width());
  EXPECT_EQ(sizeof(uint16_t), _vector_16.width());
  EXPECT_EQ(sizeof(uint32_t), _vector_32.width());
  EXPECT_EQ(sizeof(uint64_t), _vector_64.width());
}

TEST_F(FittedAttributeVectorTest, set) {
  _vector_8.set(0, opossum::ValueID(42));
  EXPECT_EQ(_vector_8.get(0), opossum::ValueID(42));
  EXPECT_THROW(_vector_8.set(size + 1, opossum::ValueID(42)), std::exception);
}
TEST_F(FittedAttributeVectorTest, size) { EXPECT_EQ(_vector_8.size(), size); }
