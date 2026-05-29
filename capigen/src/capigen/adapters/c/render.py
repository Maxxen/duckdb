"""C-language render objects for the Jinja2 template layer.

These dataclasses encode C output semantics, not API spec semantics.
The resolver (resolve.py) is the only place that reads spec dicts.
"""

from dataclasses import dataclass, field


@dataclass
class CField:
    name: str
    base: str
    pointer: int = 0
    const: bool = False
    array_size: int | None = None


@dataclass
class CTypeDef:
    name: str
    canonical_name: str
    base: str
    is_pointer: bool
    tagged_struct: bool = False  # True -> implies a pointer typedef + is_pointer=True
    is_qualified: bool = False
    description: str = ""


@dataclass
class CStruct:
    name: str
    template_alias: str
    pointer_alias: bool
    fields: list[CField]
    description: str = ""


@dataclass
class CFuncPtrParam:
    name: str
    base: str
    pointer: int = 0
    const: bool = False


@dataclass
class CFuncPtr:
    name: str
    template_alias: str
    return_base: str
    return_pointer: int
    return_const: bool
    params: list[CFuncPtrParam]
    description: str = ""


@dataclass
class CParam:
    name: str
    c_decl: str
    description: str | None = None


@dataclass
class CFunction:
    name: str
    summary: str | None
    description: str | None
    deprecated: str | None
    return_c: str
    static_inline: bool = False
    parameters: dict[str, CParam] = field(default_factory=dict)


@dataclass
class CEnumValue:
    value: int
    description: str = ""


@dataclass
class CEnum:
    name: str
    description: str
    values: dict[str, CEnumValue]


@dataclass
class CConstant:
    name: str
    value: int | str
    description: str = ""


@dataclass
class CErrorEntry:
    name: str
    code: int
    description: str = ""


@dataclass
class CErrorGroup:
    category: str
    group_id: int
    description: str
    entries: list[CErrorEntry]


@dataclass
class CModule:
    name: str
    types: list[CTypeDef]
    structs: list[CStruct]
    enums: list[CEnum]
    constants: list[CConstant]
    error_groups: list[CErrorGroup]
    function_ptrs: list[CFuncPtr]
    functions: dict[str, CFunction]
