#include "duckdb/storage/checkpoint/table_data_reader.hpp"
#include "duckdb/storage/metadata/metadata_reader.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"

#include "duckdb/storage/table/persistent_table_data.hpp"
#include "duckdb/parser/column_list.hpp"

namespace duckdb {

TableDataReader::TableDataReader(MetadataReader &reader, ColumnList &columns, PersistentTableData &data,
                                 MetaBlockPointer table_pointer)
    : reader(reader), columns(columns), data(data) {
	data.base_table_pointer = table_pointer;
}

void TableDataReader::ReadTableData() {
	D_ASSERT(!columns.empty());

	// We stored the table statistics as a unit in FinalizeTable.
	BinaryDeserializer stats_deserializer(reader);

	stats_deserializer.Begin();
	data.table_stats.Deserialize(stats_deserializer, columns);
	stats_deserializer.End();

	// Deserialize the row group pointers (lazily, just set the count and the pointer to them for now)
	data.row_group_count = reader.Read<uint64_t>();
	data.block_pointer = reader.GetMetaBlockPointer();
}

} // namespace duckdb
