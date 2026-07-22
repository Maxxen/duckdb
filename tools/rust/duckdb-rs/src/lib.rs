use libduckdb_sys as ffi;
use std::fmt;
use std::ptr;

//-------------------------------------------------------------------------------------------------
// Error Handling
//-------------------------------------------------------------------------------------------------

/// Success status returned by every `duckdb_v2_*` call (`DUCKDB_V2_ERROR_NONE`).
const OK: ffi::DUCKDB_V2_ERROR = 0;

/// An error returned by the DuckDB C API.
#[derive(Debug, Clone)]
pub struct Error {
    /// The raw `DUCKDB_V2_ERROR`.
    pub code: u32,
    /// Human-readable message, empty if the API provided none.
    pub message: String,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.message.is_empty() {
            write!(f, "DuckDB error (code {:#x})", self.code)
        } else {
            write!(f, "{} (code {:#x})", self.message, self.code)
        }
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

/// Turn a C-API status + optional error handle into a `Result`, consuming (destroying)
/// the error handle. `err` may be null (success, or an API that reported no detail).
unsafe fn check(
    code: ffi::DUCKDB_V2_ERROR,
    mut err: ffi::duckdb_v2_error_info_handle,
) -> Result<()> {
    if code == OK {
        return Ok(());
    }
    let mut message = String::new();

    unsafe {
        if !err.is_null() {
            let mut text = ffi::duckdb_v2_str {
                ptr: ptr::null(),
                len: 0,
            };
            if ffi::duckdb_v2_error_info_get_text(err, &mut text) == OK
                && !text.ptr.is_null()
                && text.len > 0
            {
                // Borrowed view, valid until we destroy the handle below — copy it out.
                let bytes = std::slice::from_raw_parts(text.ptr as *const u8, text.len as usize);
                message = String::from_utf8_lossy(bytes).into_owned();
            }
            ffi::duckdb_v2_error_info_destroy(&mut err);
        }
    }
    Err(Error { code, message })
}

//-------------------------------------------------------------------------------------------------
// Connection and Database
//-------------------------------------------------------------------------------------------------

/// A connection to a DuckDB database, owning its environment and database.
/// TODO: This is not correct, env/database should be separate entities.
pub struct Connection {
    conn: ffi::duckdb_v2_connection_handle,
    db: ffi::duckdb_v2_database_handle,
    env: ffi::duckdb_v2_environment_handle,
}

impl Connection {
    /// Open a fresh in-memory database and connect to it.
    pub fn open_in_memory() -> Result<Self> {
        // An empty path view (`{NULL, 0}`) selects an in-memory database.
        Self::open("")
    }

    /// Open the database at `path`, creating it if needed, and connect to it.
    pub fn open(path: &str) -> Result<Self> {
        unsafe {
            let mut env: ffi::duckdb_v2_environment_handle = ptr::null_mut();
            let mut err: ffi::duckdb_v2_error_info_handle = ptr::null_mut();
            check(ffi::duckdb_v2_create_environment(&mut env, &mut err), err)?;

            // The path is a borrowed (ptr, len) view — not NUL-terminated.
            let path_view = ffi::duckdb_v2_str {
                ptr: if path.is_empty() {
                    ptr::null()
                } else {
                    path.as_ptr() as *const _
                },
                len: path.len() as ffi::idx_t,
            };
            let mut db: ffi::duckdb_v2_database_handle = ptr::null_mut();
            let mut err: ffi::duckdb_v2_error_info_handle = ptr::null_mut();
            let opened = check(
                ffi::duckdb_v2_open(env, path_view, ptr::null_mut(), 0, &mut db, &mut err),
                err,
            );
            if let Err(e) = opened {
                ffi::duckdb_v2_destroy_environment(&mut env);
                return Err(e);
            }

            let mut conn: ffi::duckdb_v2_connection_handle = ptr::null_mut();
            let mut err: ffi::duckdb_v2_error_info_handle = ptr::null_mut();
            let connected = check(ffi::duckdb_v2_connect(db, &mut conn, &mut err), err);
            if let Err(e) = connected {
                ffi::duckdb_v2_close(&mut db);
                ffi::duckdb_v2_destroy_environment(&mut env);
                return Err(e);
            }

            Ok(Connection { conn, db, env })
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Reverse of construction: connection, then database, then environment.
        // destroy_environment refuses while a database is still open, so order matters.
        unsafe {
            ffi::duckdb_v2_disconnect(&mut self.conn);
            ffi::duckdb_v2_close(&mut self.db);
            ffi::duckdb_v2_destroy_environment(&mut self.env);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_in_memory_and_drop() {
        let conn = Connection::open_in_memory().expect("open in-memory");
        drop(conn);
    }
}