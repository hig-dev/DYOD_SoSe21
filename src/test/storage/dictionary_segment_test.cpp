#include <memory>
#include <string>

#include "gtest/gtest.h"

#include "../../lib/all_type_variant.hpp"
#include "../../lib/resolve_type.hpp"
#include "../../lib/storage/base_segment.hpp"
#include "../../lib/storage/dictionary_segment.hpp"
#include "../../lib/storage/value_segment.hpp"
#include "../../lib/type_cast.hpp"

namespace opossum {

class StorageDictionarySegmentTest : public ::testing::Test {
 protected:
  std::shared_ptr<opossum::ValueSegment<int>> vc_int = std::make_shared<opossum::ValueSegment<int>>();
  std::shared_ptr<opossum::ValueSegment<std::string>> vc_str = std::make_shared<opossum::ValueSegment<std::string>>();
};

TEST_F(StorageDictionarySegmentTest, CompressSegmentString) {
  vc_str->append("Bill");
  vc_str->append("Steve");
  vc_str->append("Alexander");
  vc_str->append("Steve");
  vc_str->append("Hasso");
  vc_str->append("Bill");

  std::shared_ptr<BaseSegment> col;
  resolve_data_type("string", [&](auto type) {
    using Type = typename decltype(type)::type;
    col = std::make_shared<DictionarySegment<Type>>(vc_str);
  });
  auto dict_col = std::dynamic_pointer_cast<opossum::DictionarySegment<std::string>>(col);

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

TEST_F(StorageDictionarySegmentTest, LowerUpperBound) {
  for (int i = 0; i <= 10; i += 2) vc_int->append(i);

  std::shared_ptr<BaseSegment> col;
  resolve_data_type("int", [&](auto type) {
    using Type = typename decltype(type)::type;
    col = std::make_shared<DictionarySegment<Type>>(vc_int);
  });
  auto dict_col = std::dynamic_pointer_cast<opossum::DictionarySegment<int>>(col);

  EXPECT_EQ(dict_col->lower_bound(4), (opossum::ValueID)2);
  EXPECT_EQ(dict_col->upper_bound(4), (opossum::ValueID)3);

  EXPECT_EQ(dict_col->lower_bound(5), (opossum::ValueID)3);
  EXPECT_EQ(dict_col->upper_bound(5), (opossum::ValueID)3);

  EXPECT_EQ(dict_col->lower_bound(15), opossum::INVALID_VALUE_ID);
  EXPECT_EQ(dict_col->upper_bound(15), opossum::INVALID_VALUE_ID);
}

TEST_F(StorageDictionarySegmentTest, MemoryUsage) {
  for (auto val = 0; val < 100; ++val) {
    vc_int->append(val);
  }
  std::shared_ptr<BaseSegment> col;
  resolve_data_type("int", [&](auto type) {
    using Type = typename decltype(type)::type;
    col = std::make_shared<DictionarySegment<Type>>(vc_int);
  });
  auto dict_col = std::dynamic_pointer_cast<opossum::DictionarySegment<int>>(col);

  const auto min_memory_use = 600;

  // For the calculation of the estimated memory use, we use the capacity of the container.
  // Therefore, the memory use is bigger than the min memory use.
  // So we use a margin for the test of the memory use.
  const auto memory_use_margin = 100;

  EXPECT_TRUE(min_memory_use < dict_col->estimate_memory_usage() &&
              min_memory_use + memory_use_margin > dict_col->estimate_memory_usage());
}

TEST_F(StorageDictionarySegmentTest, Append) {
  vc_str->append("Bill");
  vc_str->append("Steve");
  vc_str->append("Alexander");

  std::shared_ptr<BaseSegment> col;
  resolve_data_type("string", [&](auto type) {
    using Type = typename decltype(type)::type;
    col = std::make_shared<DictionarySegment<Type>>(vc_str);
  });
  auto dict_col = std::dynamic_pointer_cast<opossum::DictionarySegment<std::string>>(col);
  EXPECT_THROW(dict_col->append("Peter"), std::exception);
}

TEST_F(StorageDictionarySegmentTest, GetAndSubscribe) {
  vc_str->append("Bill");
  vc_str->append("Steve");
  vc_str->append("Alexander");
  vc_str->append("Steve");
  vc_str->append("Hasso");
  vc_str->append("Bill");

  std::shared_ptr<BaseSegment> col;
  resolve_data_type("string", [&](auto type) {
    using Type = typename decltype(type)::type;
    col = std::make_shared<DictionarySegment<Type>>(vc_str);
  });
  auto dict_col = std::dynamic_pointer_cast<opossum::DictionarySegment<std::string>>(col);

  EXPECT_EQ(dict_col->get(0), "Bill");
  EXPECT_EQ(dict_col->get(1), "Steve");
  EXPECT_EQ(dict_col->get(2), "Alexander");
  EXPECT_EQ(dict_col->get(3), "Steve");
  EXPECT_EQ(dict_col->get(4), "Hasso");
  EXPECT_EQ(dict_col->get(5), "Bill");

  EXPECT_EQ(type_cast<std::string>((*dict_col)[0]), "Bill");
  EXPECT_EQ(type_cast<std::string>((*dict_col)[1]), "Steve");
  EXPECT_EQ(type_cast<std::string>((*dict_col)[2]), "Alexander");
  EXPECT_EQ(type_cast<std::string>((*dict_col)[3]), "Steve");
  EXPECT_EQ(type_cast<std::string>((*dict_col)[4]), "Hasso");
  EXPECT_EQ(type_cast<std::string>((*dict_col)[5]), "Bill");

  EXPECT_THROW(dict_col->get(6), std::exception);
  EXPECT_THROW(type_cast<std::string>((*dict_col)[6]), std::exception);
}

TEST_F(StorageDictionarySegmentTest, ValueByValueId) {
  vc_str->append("Bill");
  vc_str->append("Steve");
  vc_str->append("Alexander");
  vc_str->append("Steve");
  vc_str->append("Hasso");
  vc_str->append("Bill");

  std::shared_ptr<BaseSegment> col;
  resolve_data_type("string", [&](auto type) {
    using Type = typename decltype(type)::type;
    col = std::make_shared<DictionarySegment<Type>>(vc_str);
  });
  auto dict_col = std::dynamic_pointer_cast<opossum::DictionarySegment<std::string>>(col);
  EXPECT_EQ(dict_col->value_by_value_id(ValueID{0}), "Alexander");
  EXPECT_EQ(dict_col->value_by_value_id(ValueID{1}), "Bill");
  EXPECT_EQ(dict_col->value_by_value_id(ValueID{2}), "Hasso");
  EXPECT_EQ(dict_col->value_by_value_id(ValueID{3}), "Steve");
  EXPECT_THROW(dict_col->value_by_value_id(ValueID{4}), std::exception);
}

TEST_F(StorageDictionarySegmentTest, FixedSizeAttributeVectorUsage) {
  int count = std::numeric_limits<uint8_t>::min();
  vc_int->append(count);
  count++;

  std::shared_ptr<BaseSegment> col;
  resolve_data_type("int", [&](auto type) {
    using Type = typename decltype(type)::type;
    col = std::make_shared<DictionarySegment<Type>>(vc_int);
  });
  auto dict_col = std::dynamic_pointer_cast<opossum::DictionarySegment<int>>(col);
  EXPECT_EQ(dict_col->attribute_vector()->width(), 1);

  while (count <= std::numeric_limits<uint8_t>::max() + 1) {
    vc_int->append(count);
    count++;
  }

  std::shared_ptr<BaseSegment> col2;
  resolve_data_type("int", [&](auto type) {
    using Type = typename decltype(type)::type;
    col2 = std::make_shared<DictionarySegment<Type>>(vc_int);
  });
  auto dict_col2 = std::dynamic_pointer_cast<opossum::DictionarySegment<int>>(col2);
  EXPECT_EQ(dict_col2->attribute_vector()->width(), 2);

  while (count <= std::numeric_limits<uint16_t>::max() + 1) {
    vc_int->append(count);
    count++;
  }

  std::shared_ptr<BaseSegment> col3;
  resolve_data_type("int", [&](auto type) {
    using Type = typename decltype(type)::type;
    col3 = std::make_shared<DictionarySegment<Type>>(vc_int);
  });
  auto dict_col3 = std::dynamic_pointer_cast<opossum::DictionarySegment<int>>(col3);
  EXPECT_EQ(dict_col3->attribute_vector()->width(), 4);
}
}  // namespace opossum
