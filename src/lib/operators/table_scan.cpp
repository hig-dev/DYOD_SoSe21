#include "table_scan.hpp"

#include <memory>

#include "resolve_type.hpp"
#include "storage/table.hpp"

namespace opossum {

TableScan::TableScan(const std::shared_ptr<const AbstractOperator>& input_operator, const ColumnID column_id,
                     const ScanType scan_type, const AllTypeVariant search_value)
    : _input_operator{input_operator}, _scan_type{scan_type}, _column_id{column_id}, _search_value{search_value} {}

ColumnID TableScan::column_id() const { return _column_id; }

ScanType TableScan::scan_type() const { return _scan_type; }

const AllTypeVariant& TableScan::search_value() const { return _search_value; }

template <typename T>
void TableScan::_scan_value_segment(const ChunkID& chunk_id, ValueSegment<T>& segment, PosList& pos_list,
                                    std::function<bool(const T)>& comparator_function) {
  const auto values = segment.values();
  const auto segment_size = segment.size();
  for (auto chunk_offset = ChunkOffset{0}; chunk_offset < segment_size; ++chunk_offset) {
    const auto value = values[chunk_offset];
    if (comparator_function(value)) {
      pos_list.emplace_back(chunk_id, chunk_offset);
    }
  }
}

template <typename T>
void _add_attribute_indexes(const ChunkID chunk_id, DictionarySegment<T>& segment, PosList& pos_list,
                            const std::function<bool(const ValueID)>& comparator_function) {
  const auto attribute_vector = segment.attribute_vector();
  const auto attribute_vector_size = attribute_vector->size();
  for (auto attribute_index = ChunkOffset{0}; attribute_index < attribute_vector_size; ++attribute_index) {
    const auto value_id = attribute_vector->get(attribute_index);
    if (comparator_function(value_id)) {
      pos_list.emplace_back(chunk_id, attribute_index);
    }
  }
}

template <typename T>
void TableScan::_scan_dictionary_segment(const ChunkID& chunk_id, DictionarySegment<T>& segment, PosList& pos_list,
                                         const T& typed_search_value) {
  switch (_scan_type) {
    case ScanType::OpEquals: {
      const auto lower_bound_value_id = segment.lower_bound(typed_search_value);
      const auto upper_bound_value_id = segment.upper_bound(typed_search_value);
      if (upper_bound_value_id != lower_bound_value_id) {
        const auto comparator_function = _build_comparator_function<ValueID>(lower_bound_value_id);
        _add_attribute_indexes(chunk_id, segment, pos_list, comparator_function);
      }
      break;
    }
    case ScanType::OpNotEquals: {
      const auto lower_bound_value_id = segment.lower_bound(typed_search_value);
      const auto upper_bound_value_id = segment.upper_bound(typed_search_value);
      if (lower_bound_value_id == upper_bound_value_id) {
        const auto comparator_function = [](const ValueID& _) { return true; };
        _add_attribute_indexes(chunk_id, segment, pos_list, comparator_function);
      } else {
        const auto comparator_function = _build_comparator_function<ValueID>(lower_bound_value_id);
        _add_attribute_indexes(chunk_id, segment, pos_list, comparator_function);
      }
      break;
    }
    case ScanType::OpGreaterThanEquals: {
      const auto lower_bound_value_id = segment.lower_bound(typed_search_value);
      const auto comparator_function = _build_comparator_function<ValueID>(lower_bound_value_id);
      _add_attribute_indexes(chunk_id, segment, pos_list, comparator_function);
      break;
    }
    case ScanType::OpGreaterThan: {
      const auto upper_bound_value_id = segment.upper_bound(typed_search_value);
      const auto comparator_function =
          _build_comparator_function<ValueID>(upper_bound_value_id, ScanType::OpGreaterThanEquals);
      _add_attribute_indexes(chunk_id, segment, pos_list, comparator_function);
      break;
    }
    case ScanType::OpLessThanEquals: {
      const auto lower_bound_value_id = segment.lower_bound(typed_search_value);
      const auto upper_bound_value_id = segment.upper_bound(typed_search_value);
      const auto comparator_function =
          (lower_bound_value_id == upper_bound_value_id)
              ? _build_comparator_function<ValueID>(lower_bound_value_id, ScanType::OpLessThan)
              : _build_comparator_function<ValueID>(lower_bound_value_id);
      _add_attribute_indexes(chunk_id, segment, pos_list, comparator_function);
      break;
    }
    case ScanType::OpLessThan: {
      const auto lower_bound_value_id = segment.lower_bound(typed_search_value);
      const auto comparator_function = _build_comparator_function<ValueID>(lower_bound_value_id);
      _add_attribute_indexes(chunk_id, segment, pos_list, comparator_function);
      break;
    }
    default:
      throw std::runtime_error("There is no comparison implemented for this scan_type.");
  }
}

template <typename T>
void TableScan::_scan_reference_segment(const ChunkID& chunk_id, ReferenceSegment& segment, PosList& pos_list,
                                        std::function<bool(const T)>& comparator_function) {
  const auto ref_seg_position_list = segment.pos_list();
  const auto segment_size = segment.size();
  for (auto segment_index = ChunkOffset{0}; segment_index < segment_size; ++segment_index) {
    const auto value = type_cast<T>(segment[segment_index]);
    if (comparator_function(value)) {
      const auto chunk_offset = (*ref_seg_position_list)[segment_index];
      pos_list.emplace_back(chunk_offset);
    }
  }
}

template <typename T>
void TableScan::_scan_segment(const ChunkID& chunk_id, std::shared_ptr<BaseSegment>& segment, PosList& pos_list,
                              std::function<bool(const T)>& comparator_function, const T& typed_search_value) {
  auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(segment);
  if (reference_segment) {
    _scan_reference_segment(chunk_id, *reference_segment, pos_list, comparator_function);
    return;
  }

  auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<T>>(segment);
  if (dictionary_segment) {
    _scan_dictionary_segment(chunk_id, *dictionary_segment, pos_list, typed_search_value);
    return;
  }

  auto value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(segment);
  if (value_segment) {
    _scan_value_segment(chunk_id, *value_segment, pos_list, comparator_function);
    return;
  }

  throw std::runtime_error("There is no segment scan implemented for this segment type.");
}

template <typename T>
T TableScan::_get_typed_search_value() {
  try {
    return type_cast<T>(_search_value);
  } catch (...) {
    throw std::invalid_argument("Target column type and compare value type are incompatible.");
  }
}

std::shared_ptr<const Table> TableScan::_on_execute() {
  const auto input_table = _input_operator->get_output();
  const auto column_count = input_table->column_count();

  auto output_table = std::make_shared<Table>();
  for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    output_table->copy_column_definition(input_table, column_id);
  }

  if (input_table->is_empty()) {
    return output_table;
  }

  const auto column_type_name = input_table->column_type(_column_id);
  resolve_data_type(column_type_name, [&](auto type) {
    using Type = typename decltype(type)::type;
    auto typed_search_value = _get_typed_search_value<Type>();
    auto comparator_function = _build_comparator_function(typed_search_value);

    auto reference_position_list = std::make_shared<PosList>();

    // scan input segments
    const auto chunk_count = input_table->chunk_count();
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      auto segment = input_table->get_chunk(chunk_id).get_segment(_column_id);
      _scan_segment<Type>(chunk_id, segment, *reference_position_list, comparator_function, typed_search_value);
    }

    auto first_reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(
        input_table->get_chunk(ChunkID{0}).get_segment(ColumnID{0})
      );
    // check if input_table contains reference segments, if true we have to get the original table from it
    const auto referenced_table = (first_reference_segment)
      ? first_reference_segment->referenced_table()
      : input_table;

    // build referenced segments
    auto referenced_data_chunk = std::make_shared<Chunk>();
    for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
      auto reference_segment = std::make_shared<ReferenceSegment>(referenced_table, column_id, reference_position_list);
      referenced_data_chunk->add_segment(reference_segment);
    }
    output_table->emplace_chunk(referenced_data_chunk);
  });
  return output_table;
}

template <typename T>
std::function<bool(const T)> TableScan::_build_comparator_function(const T& right_operand) {
  return _build_comparator_function(right_operand, _scan_type);
}

template <typename T>
std::function<bool(const T)> TableScan::_build_comparator_function(const T& right_operand, const ScanType& scan_type) {
  switch (scan_type) {
    case ScanType::OpEquals:
      return [&right_operand](const T left_operand) -> bool { return left_operand == right_operand; };
    case ScanType::OpNotEquals:
      return [&right_operand](const T left_operand) -> bool { return left_operand != right_operand; };
    case ScanType::OpGreaterThanEquals:
      return [&right_operand](const T left_operand) -> bool { return left_operand >= right_operand; };
    case ScanType::OpGreaterThan:
      return [&right_operand](const T left_operand) -> bool { return left_operand > right_operand; };
    case ScanType::OpLessThanEquals:
      return [&right_operand](const T left_operand) -> bool { return left_operand <= right_operand; };
    case ScanType::OpLessThan:
      return [&right_operand](const T left_operand) -> bool { return left_operand < right_operand; };
    default:
      throw std::runtime_error("There is no comparison implemented for this scan_type.");
  }
}
}  // namespace opossum
