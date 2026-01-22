from importlib import import_module
from pathlib import Path
from pkgutil import iter_modules

_PKG_DIR = Path(__file__).resolve().parent

# Import every top‑level module in this package so their SQLModel classes
# register on SQLModel.metadata when Alembic (or anything else) imports
# data_in_models.
for module_info in iter_modules([str(_PKG_DIR)]):
    if module_info.ispkg:
        continue
    if module_info.name in {"__init__"}:
        continue
    import_module(f"{__name__}.{module_info.name}")
