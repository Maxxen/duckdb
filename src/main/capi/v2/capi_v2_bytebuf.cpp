#include "duckdb/main/capi_v2/capi_v2_internal.hpp"

namespace duckdb {
namespace capiv2 {

static void ReleaseBytebufText(duckdb_v2_bytebuf *buf) {
	delete static_cast<string *>(buf->private_data);
	ResetBytebuf(*buf);
}

void SetBytebufText(duckdb_v2_bytebuf &buf, string text) {
	// The holder is heap-allocated so data stays put, even for strings stored inline in the string object
	auto holder = make_uniq<string>(std::move(text));
	buf.data = &(*holder)[0];
	buf.size = holder->size();
	buf.private_data = holder.release();
	buf.release = ReleaseBytebufText;
}

} // namespace capiv2
} // namespace duckdb

//----------------------------------------------------------------------------------------------------------------------
// Public API
//----------------------------------------------------------------------------------------------------------------------

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_bytebuf_destroy(duckdb_v2_bytebuf *buf) {
	if (!buf) {
		return DUCKDB_V2_ERROR_NONE;
	}
	if (buf->release) {
		buf->release(buf);
	}
	ResetBytebuf(*buf);
	return DUCKDB_V2_ERROR_NONE;
}
