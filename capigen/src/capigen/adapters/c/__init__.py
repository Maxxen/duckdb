"""C language adapter for capigen."""

from pathlib import Path

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from .resolve import resolve_c_options, resolve_modules

_TEMPLATES_DIR = Path(__file__).parent / "templates"


def generate(modules: list[dict], metadata: dict, output_path: Path) -> None:
    render_modules = resolve_modules(modules, metadata)
    c_opts = resolve_c_options(metadata)

    def _c_line_comment(description: str) -> str:
        """Emit a description as //! lines, one per non-empty line of input."""
        lines = description.strip().splitlines()
        return "\n//! ".join(line.strip() for line in lines if line.strip())

    env = Environment(
        loader=FileSystemLoader(str(_TEMPLATES_DIR)),
        keep_trailing_newline=True,
        trim_blocks=True,
        lstrip_blocks=True,
        undefined=StrictUndefined,
    )
    env.filters["c_line_comment"] = _c_line_comment
    template = env.get_template("header.h.j2")
    output = template.render(
        modules=render_modules,
        metadata=metadata,
        c_opts=c_opts,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(output)
    print(f"Generated {output_path}")
