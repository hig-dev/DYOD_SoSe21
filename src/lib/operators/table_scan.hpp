#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "storage/base_segment.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/reference_segment.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class BaseTableScanImpl;
class Table;

class TableScan : public AbstractOperator {
 public:
  TableScan(const std::shared_ptr<const AbstractOperator>& input_operator, const ColumnID column_id,
            const ScanType scan_type, const AllTypeVariant search_value);

  ColumnID column_id() const;
  ScanType scan_type() const;
  const AllTypeVariant& search_value() const;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  const std::shared_ptr<const AbstractOperator>& _input_operator;
  const ScanType _scan_type;
  const ColumnID _column_id;
  const AllTypeVariant _search_value;

  template <typename T>
  void _scan_value_segment(const ChunkID& chunk_id, ValueSegment<T>& segment, PosList& pos_list,
                           std::function<bool(const T)>& comparator_function);

  template <typename T>
  void _scan_reference_segment(const ChunkID& chunk_id, ReferenceSegment& segment, PosList& pos_list,
                               std::function<bool(const T)>& comparator_function);

  template <typename T>
  void _scan_dictionary_segment(const ChunkID& chunk_id, DictionarySegment<T>& segment, PosList& pos_list,
                                const T& typed_search_value);

  template <typename T>
  void _scan_segment(const ChunkID& chunk_id, std::shared_ptr<BaseSegment>& segment, PosList& pos_list,
                     std::function<bool(const T)>& comparator_function, const T& typed_search_value);

  template <typename T>
  T _get_typed_search_value();

  template <typename T>
  std::function<bool(const T)> _build_comparator_function(const T& right_operand);
  template <typename T>
  std::function<bool(const T)> _build_comparator_function(const T& right_operand, const ScanType& scan_type);
};

}  // namespace opossum
