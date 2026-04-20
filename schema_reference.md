# IDL Schema Reference

## metadata.yaml

Global settings shared across all modules.

| Field | Required | Description |
|---|---|---|
| `schema_version` | yes | Semver string. Declares which schema version this spec complies with. |
| `versions` | yes | List of known API version strings (semver). Used to validate `added`/`deprecated` on functions. |
| `suffixes` | yes | ABI naming suffixes per construct type. See below. |
| `primitives` | yes | List of primitive type definitions. See below. |

### suffixes

| Key | Description | Example value |
|---|---|---|
| `handles` | Appended to handle names | `"_ptr"` |
| `callbacks` | Appended to callback names | `"_cb"` |
| `aliases` | Appended to alias names | `"_t"` |

### primitives (list items)

| Field | Required | Description |
|---|---|---|
| `name` | yes | Abstract name used in the spec (e.g. `u32`, `opaque`, `char`) |
| `c_type` | yes | Corresponding C type in the ABI (e.g. `uint32_t`, `void`, `char`) |

---

## Module YAML files

Each module file describes one area of the API. All sections except `module` are optional and default to empty.

```yaml
module: database    # required — module name
handles: {}         # opaque handle types
callbacks: {}       # callback function pointers
aliases: {}         # named type aliases
structs: {}         # structs with visible fields
enums: {}           # enumerations
constants: {}       # named constants
error_groups: {}    # hierarchical error codes
functions: {}       # API functions
```

---

## handles

Opaque pointer types. Consumers cannot see the internal structure. Canonical C name: `name` + suffix from `metadata.suffixes.handles`.

```yaml
handles:
  duckdb_connection:
    description: An opaque handle to a DuckDB connection
    cleanup_with: duckdb_disconnect
```

| Field | Required | Default | Description |
|---|---|---|---|
| `description` | no | `""` | Documentation string |
| `cleanup_with` | no | — | Name of the destructor function for this handle |

---

## callbacks

Function pointer typedefs. Canonical C name: `name` + suffix from `metadata.suffixes.callbacks`.

```yaml
callbacks:
  duckdb_scalar_func_bind:
    return_type: opaque
    parameters:
      context:
        type: duckdb_ctx
      args:
        type: duckdb_scalar_func_bind_args
```

| Field | Required | Default | Description |
|---|---|---|---|
| `return_type` | yes | — | Return type name (primitive or declared type) |
| `return_pointer` | no | `0` | Pointer indirection on return type |
| `return_const` | no | `false` | Whether return type is const-qualified |
| `parameters` | no | `{}` | Map of parameter name to Parameter (see below) |

---

## aliases

Named type aliases. Canonical C name: `name` + suffix from `metadata.suffixes.aliases`.

```yaml
aliases:
  duckdb_error_code:
    underlying: u32
    description: 'Full 32-bit error code: (group_id << 16) | code'
```

| Field | Required | Default | Description |
|---|---|---|---|
| `underlying` | yes | — | Type being aliased (primitive name or declared type) |
| `description` | no | `""` | Documentation string |

---

## structs

Composite types with consumer-visible fields. Use `handles` for opaque types instead.

```yaml
structs:
  duckdb_date:
    fields:
      - name: days
        type: i32
```

| Field | Required | Default | Description |
|---|---|---|---|
| `pointer_alias` | no | `false` | If true, also generate a pointer typedef (name + aliases suffix) |
| `fields` | no | `[]` | Ordered list of struct fields |

### Struct field

| Field | Required | Default | Description |
|---|---|---|---|
| `name` | yes | — | Field name |
| `type` | yes | — | Type name (primitive or declared) |
| `pointer` | no | `0` | Pointer indirection level |
| `const` | no | `false` | Whether the field is const-qualified |

---

## enums

Enumerations with auto-numbered values.

```yaml
enums:
  DUCKDB_TYPE:
    description: DuckDB's internal types
    values:
      DUCKDB_TYPE_INVALID:
        value: 0
        description: Invalid type
      DUCKDB_TYPE_BOOLEAN:
        description: bool
      DUCKDB_TYPE_TINYINT: {}
```

| Field | Required | Default | Description |
|---|---|---|---|
| `description` | no | `""` | Documentation string |
| `values` | no | `{}` | Map of member name to enum value |

### Enum value

| Field | Required | Default | Description |
|---|---|---|---|
| `value` | no | — | Explicit integer. If omitted, auto-increments from previous. |
| `description` | no | `""` | Documentation string |

---

## constants

Named compile-time values.

```yaml
constants:
  DUCKDB_API_ERROR:
    value: "0xFFFFFFFF"
    description: Sentinel for an unspecified internal API error
```

| Field | Required | Default | Description |
|---|---|---|---|
| `value` | yes | — | Integer or string expression |
| `description` | no | `""` | Documentation string |

---

## error_groups

Hierarchical error codes. Full 32-bit value: `(group_id << 16) | code`.

```yaml
error_groups:
  IO:
    group_id: 0x0001
    description: Input/Output errors
    entries:
      DUCKDB_ERROR_IO_FILE_NOT_FOUND:
        code: 0x0001
        description: File not found
```

| Field | Required | Default | Description |
|---|---|---|---|
| `group_id` | yes | — | Integer, upper 16 bits of the error code |
| `description` | no | `""` | Documentation string |
| `entries` | yes | — | Map of error name to entry |

### Error entry

| Field | Required | Default | Description |
|---|---|---|---|
| `code` | yes | — | Integer, lower 16 bits of the error code |
| `description` | no | `""` | Documentation string |

---

## functions

API function declarations.

```yaml
functions:
  duckdb_open:
    summary: Open a database at the given path
    role: constructor
    belongs_to: duckdb_database
    parameters:
      context:
        type: duckdb_ctx
      path:
        type: char
        indirection: 1
        const: true
        description: Path to the database file
      out_database:
        type: duckdb_database
        indirection: 1
        direction: out
        description: The opened database handle
    return_type: DUCKDB_API_CALL
    added: "1.2.0"
```

| Field | Required | Default | Description |
|---|---|---|---|
| `summary` | yes | — | One-line description (used in generated doc comments) |
| `description` | no | `""` | Extended description |
| `role` | no | `"method"` | One of: `constructor`, `destructor`, `getter`, `setter`, `method` |
| `belongs_to` | no | — | Type this function operates on |
| `parameters` | no | `{}` | Map of parameter name to Parameter (see below) |
| `return_type` | no | `"duckdb_error_code"` | Return type name |
| `return_pointer` | no | `0` | Pointer indirection on return type |
| `return_const` | no | `false` | Whether return type is const-qualified |
| `added` | no | — | API version when introduced (semver) |
| `deprecated` | no | — | API version when deprecated (semver) |

---

## Parameter

Used by both `functions` and `callbacks`.

| Field | Required | Default | Description |
|---|---|---|---|
| `type` | yes | — | Type name (primitive or declared) |
| `indirection` | no | `0` | Pointer level: 0 = value, 1 = pointer, 2 = pointer-to-pointer |
| `const` | no | `false` | Whether const-qualified |
| `direction` | no | `"in"` | `"in"` or `"out"`. Out parameters must have indirection >= 1. |
| `description` | no | `""` | Documentation string |
