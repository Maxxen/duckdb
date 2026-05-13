"""C language adapter for capigen."""

from pathlib import Path

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from .resolve import resolve_modules

_TEMPLATES_DIR = Path(__file__).parent / "templates"


def generate(modules: list[dict], metadata: dict, output_path: Path) -> None:
    render_modules = resolve_modules(modules, metadata)

    env = Environment(
        loader=FileSystemLoader(str(_TEMPLATES_DIR)),
        keep_trailing_newline=True,
        trim_blocks=True,
        lstrip_blocks=True,
        undefined=StrictUndefined,
    )
    template = env.get_template("duckdb_v2.h.j2")
    output = template.render(modules=render_modules, metadata=metadata)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(output)
    print(f"Generated {output_path}")
