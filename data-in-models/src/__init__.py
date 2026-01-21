from importlib import import_module
from pathlib import Path
from pkgutil import iter_modules

# Ensure Base is defined and SQLModel.metadata is the shared metadata
from . import base as _base  # noqa: F401

_pkg_dir = Path(__file__).resolve().parent

# Import every top‑level module in this package so their SQLModel classes
# register on SQLModel.metadata.
for module_info in iter_modules([str(_pkg_dir)]):
    if module_info.ispkg:
        continue
    if module_info.name in {"__init__", "base"}:
        continue
    import_module(f"{__name__}.{module_info.name}")
