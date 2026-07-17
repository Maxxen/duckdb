"""capigen: a declarative C API generator driven by YAML specs."""

from importlib.metadata import version

__version__ = version("capigen")
# Major.minor of the package version; the spec schema version tracks it.
SCHEMA_VERSION = ".".join(__version__.split(".")[:2])

__all__ = ["__version__", "SCHEMA_VERSION"]
