"""SQLAlchemy support for Borland / Corel Paradox databases."""
# coding=utf-8

import pyodbc
from sqlalchemy.dialects import registry as _registry

from .base import (
    nc,
    cg,
    Char,
    Date,
    Time,
    cl_in,
    BigInt,
    Double,
    Binary,
    Decimal,
    Logical,
    SmallInt,
    strtobool,
    Timestamp,
    LongVarChar,
    LongVarBinary,
)

__version__ = "0.0.1"

pyodbc.pooling = True  # Makes the ODBC overhead a little more manageable
_registry.register(
    "paradox.pyodbc", "sqlalchemy_paradox.pyodbc", "ParadoxDialect_pyodbc"
)

__all__ = (
    "nc",
    "cg",
    "Char",
    "Date",
    "Time",
    "cl_in",
    "BigInt",
    "Double",
    "Binary",
    "Decimal",
    "Logical",
    "SmallInt",
    "strtobool",
    "Timestamp",
    "__version__",
    "LongVarChar",
    "LongVarBinary",
)
