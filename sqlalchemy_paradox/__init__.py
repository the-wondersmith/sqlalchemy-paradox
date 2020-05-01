from sqlalchemy.dialects import registry as _registry

from .base import ParadoxDialect, Binary, LongVarChar, AlphaNumeric, Number, Short, PdoxDate

import pyodbc

__version__ = "0.0.1"

pyodbc.pooling = False  # Left from SQLAlchemy-Access
_registry.register(
    "paradox.pyodbc", "sqlalchemy_paradox.pyodbc", "ParadoxDialect_pyodbc"
)

__all__ = [
    "ParadoxDialect",
    "Binary",
    "LongVarChar",
    "AlphaNumeric",
    "Number",
    "Short",
    "PdoxDate"
]
