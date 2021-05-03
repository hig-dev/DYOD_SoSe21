#include "storage_manager.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "utils/assert.hpp"

namespace opossum {

StorageManager& StorageManager::get() {
  static StorageManager _instance;
  return _instance;
}

void StorageManager::add_table(const std::string& name, std::shared_ptr<Table> table) {
  const auto insertion_result = _tables.insert({name, table});
  Assert(insertion_result.second,
         "Table could not be inserted because there was an existing table with the same name.");
}

void StorageManager::drop_table(const std::string& name) {
  const auto dropped_table_count = _tables.erase(name);
  Assert(dropped_table_count == 1, "Table could not be removed because it was not found.");
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const { return _tables.at(name); }

bool StorageManager::has_table(const std::string& name) const { return _tables.contains(name); }

std::vector<std::string> StorageManager::table_names() const {
  auto table_names = std::vector<std::string>{};
  table_names.reserve(_tables.size());
  for (const auto& [table_name, _] : _tables) {
    table_names.emplace_back(table_name);
  }
  return table_names;
}

void StorageManager::print(std::ostream& out) const {
  out << _tables.size() << " tables available:" << std::endl;
  for (const auto& [table_name, table_value] : _tables) {
    out << " - \"" << table_name << "\" ["
        << "column_count=" << table_value->column_count() << ","
        << " row_count=" << table_value->row_count() << ","
        << " chunk_count=" << table_value->chunk_count() << "]\n";
  }
}

void StorageManager::reset() {
  // clear all registered tables
  _tables.clear();
}

}  // namespace opossum
