""" SQLAlchemy support for Borland Paradox tables
"""

import sqlalchemy.dialects as dialects
import pyodbc
from .base import ParadoxSQLCompiler, ParadoxTypeCompiler, ParadoxExecutionContext, ParadoxDialect
from .base import Binary, LongVarChar, AlphaNumeric, Number, Short, PdoxDate

__version__ = "0.1.2"

pyodbc.pooling = False  # Left from SQLAlchemy-Access
dialects.registry.register(
    "paradox.pyodbc", "sqlalchemy_paradox.pyodbc", "ParadoxDialect_pyodbc"
)

__all__ = [
    "ParadoxDialect",
    "ParadoxSQLCompiler",
    "ParadoxTypeCompiler",
    "ParadoxExecutionContext",
    "Binary",
    "LongVarChar",
    "AlphaNumeric",
    "Number",
    "Short",
    "PdoxDate"
]
