""" SQLAlchemy support for Borland Paradox tables
"""

import sqlalchemy.dialects as dialects
import pyodbc
from .base import ParadoxDialect, Binary, LongVarChar, AlphaNumeric, Number, Short, PdoxDate

__version__ = "0.1.0"

pyodbc.pooling = False  # Left from SQLAlchemy-Access
dialects.registry.register(
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
