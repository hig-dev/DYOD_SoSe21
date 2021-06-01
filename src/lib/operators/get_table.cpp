#include "get_table.hpp"

#include <memory>
#include <string>

#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

namespace opossum {

GetTable::GetTable(const std::string& name) : _name{name} {}

const std::string& GetTable::table_name() const { return _name; }

std::shared_ptr<const Table> GetTable::_on_execute() { return StorageManager::get().get_table(_name); }

}  // namespace opossum
