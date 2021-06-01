#include "abstract_operator.hpp"

#include <memory>
#include <string>
#include "storage/table.hpp"
#include "utils/assert.hpp"

namespace opossum {

AbstractOperator::AbstractOperator(const std::shared_ptr<const AbstractOperator> left,
                                   const std::shared_ptr<const AbstractOperator> right)
    : _left_input(left), _right_input(right) {}

void AbstractOperator::execute() {
  Assert(!_is_executing && !_has_been_executed, "Operators shall not be executed twice.");
  _is_executing = true;
  _output = _on_execute();
  _is_executing = false;
  _has_been_executed = true;
}

std::shared_ptr<const Table> AbstractOperator::get_output() const {
  Assert(_has_been_executed, "The method execute() must be called before calling get_output().");
  return _output;
}

std::shared_ptr<const AbstractOperator> AbstractOperator::left_input() const { return _left_input; }

std::shared_ptr<const AbstractOperator> AbstractOperator::right_input() const { return _right_input; }

std::shared_ptr<const Table> AbstractOperator::_left_input_table() const { return _left_input->get_output(); }

std::shared_ptr<const Table> AbstractOperator::_right_input_table() const { return _right_input->get_output(); }

}  // namespace opossum
