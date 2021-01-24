"""SQLAlchemy Support for the Borland / Corel Paradox databases."""
# coding=utf-8

from sqlalchemy import pool, types as sqla_types
from sqlalchemy.util import raise_
from sqlalchemy.exc import SQLAlchemyError, UnsupportedCompilationError, CompileError
from sqlalchemy.sql.sqltypes import STRINGTYPE
from sqlalchemy.sql import (
    compiler,
    elements,
    functions,
    operators as sqla_operators,
)
from sqlalchemy.sql import CompoundSelect
from sqlalchemy.engine import default, reflection
from typing import Any, Set, List, Dict, Tuple, Iterable, Callable, Optional
from datetime import date, time, datetime
from decimal import Decimal as PyDecimal
from numbers import Number
from unicodedata import normalize
from uuid import uuid4

import pyodbc


def normalize_caseless(text: Any) -> str:
    """Normalize mixed-case text to be case-agnostic."""
    return str(normalize("NFKD", str(text).casefold()))


nc = normalize_caseless


def caseless_in(key: str, value: Iterable) -> bool:
    """Caseless-ly determine if the supplied key exists in the supplied
    iterable."""
    if isinstance(value, str):
        values_: Iterable = normalize_caseless(value)
    else:
        values_ = map(normalize_caseless, value)

    return bool(normalize_caseless(key) in values_)


cl_in = caseless_in


def strtobool(string: Any) -> bool:
    """Convert a string representation of truth to true (1) or false (0).

    True values are 'y', 'yes', 't', 'true', 'on', and '1' False values
    are 'n', 'no', 'f', 'false', 'off', and '0' Raises ValueError if
    'string' is anything else.
    """
    return caseless_in(string, ("y", "yes", "t", "true", "on", "1", "1.0", "1.00"))


def caseless_get(
    mapping: Dict[Any, Any], key: str, fallback: Optional[Any] = None
) -> Any:
    """Get the value for the specified key from the supplied dictionary if it exists, caseless-ly."""
    if caseless_in(key=key, value=mapping.keys()):
        key = next(filter(lambda item: nc(item) == nc(key), mapping.keys()))
        return mapping.get(key)
    return fallback


cg = caseless_get


class LongVarBinary(sqla_types.BINARY):
    """SQLAlchemy type class for the Paradox LongVarBinary datatype."""

    __odbc_datatype__ = -4
    __visit_name__ = "BINARY"
    __intersolv_name__ = "Binary"

    def get_dbapi_type(self, dbapi):
        """The equivalent type from pyodbc."""
        return getattr(dbapi, "Binary", None)

    @property
    def python_type(self):
        """The equivalent Python type."""
        return bytes

    def literal_processor(self, dialect):
        """A callable to transform the value into a string."""
        return lambda value: "".join(map(str, iter(str(value).encode("utf8"))))


class LongVarChar(sqla_types.Text):
    """SQLAlchemy type class for the Paradox LongVarChar datatype."""

    __odbc_datatype__ = -1
    __visit_name__ = "text"
    __intersolv_name__ = "Memo"

    def get_dbapi_type(self, dbapi):
        """The equivalent type from pyodbc."""
        return getattr(dbapi, "STRING", None)

    @property
    def python_type(self):
        """The equivalent Python type."""
        return str

    def literal_processor(self, dialect):
        """A callable to transform the value into a string."""
        return lambda value: str(value)


class Char(sqla_types.CHAR):
    """A straight copy of the SQLAlchemy CHAR type."""

    __odbc_datatype__ = 1
    __visit_name__ = "CHAR"
    __intersolv_name__ = "Alpha"

    def get_dbapi_type(self, dbapi):
        """The equivalent type from pyodbc."""
        return getattr(dbapi, "STRING", None)

    @property
    def python_type(self):
        """The equivalent Python type."""
        return str

    def literal_processor(self, dialect):
        """A callable to transform the value into a string."""
        return lambda value: str(value)


class SmallInt(sqla_types.SmallInteger):
    """A straight copy of the SQLAlchemy SmallInteger type."""

    __odbc_datatype__ = 5
    __visit_name__ = "integer"
    __intersolv_name__ = "Short"

    def get_dbapi_type(self, dbapi):
        """The equivalent type from pyodbc."""
        return getattr(dbapi, "Integer", getattr(dbapi, "ROWID", None))

    @property
    def python_type(self):
        """The equivalent Python type."""
        return int

    def literal_processor(self, dialect):
        """A callable to transform the value."""
        return lambda value: int(value)


class BigInt(sqla_types.Integer):
    """A Straight copy of the SQLAlchemy Integer type."""

    __odbc_datatype__ = 4
    __visit_name__ = "BIGINT"
    __intersolv_name__ = "Long Integer"

    def get_dbapi_type(self, dbapi):
        """The equivalent type from pyodbc."""
        return getattr(dbapi, "Integer", getattr(dbapi, "ROWID", None))

    @property
    def python_type(self):
        """The equivalent Python type."""
        return int

    def literal_processor(self, dialect):
        """A callable to transform the value."""
        return lambda value: int(value)


class Decimal(sqla_types.DECIMAL):
    """A straight copy of the SQLAlchemy Decimal type."""

    __odbc_datatype__ = 3
    __visit_name__ = "decimal"
    __intersolv_name__ = "Number"

    def get_dbapi_type(self, dbapi):
        """The equivalent type from pyodbc."""
        return getattr(dbapi, "Number", None)

    @property
    def python_type(self):
        """The equivalent Python type."""
        return PyDecimal

    def literal_processor(self, dialect):
        """A callable to transform the value."""
        return lambda value: PyDecimal(str(value))


class DoublePrecision(sqla_types.Float):
    """SQLAlchemy type class for the Paradox DoublePrecision datatype."""

    __odbc_datatype__ = 8
    __visit_name__ = "float"
    __intersolv_name__ = "Number"

    def get_dbapi_type(self, dbapi):
        """The equivalent type from pyodbc."""
        return getattr(dbapi, "Number", None)

    @property
    def python_type(self):
        """The equivalent Python type."""
        return float

    def literal_processor(self, dialect):
        """A callable to transform the value."""
        return lambda value: float(str(value))


Double = DoublePrecision


class Binary(sqla_types.BINARY):
    """A straight copy of the SQLAlchemy Binary type."""

    __odbc_datatype__ = -2
    __visit_name__ = "BINARY"
    __intersolv_name__ = "Binary"

    def get_dbapi_type(self, dbapi):
        """The equivalent type from pyodbc."""
        return getattr(dbapi, "BINARY", None)

    @property
    def python_type(self):
        """The equivalent Python type."""
        return bytes

    def literal_processor(self, dialect):
        """A callable to transform the value."""
        return lambda value: "".join(map(str, iter(str(value).encode("utf8"))))


class Logical(sqla_types.Boolean):
    """A straight copy of the SQLAlchemy Boolean type."""

    __odbc_datatype__ = -7
    __visit_name__ = "boolean"
    __intersolv_name__ = "Logical"

    def get_dbapi_type(self, dbapi):
        """The equivalent type from pyodbc."""
        return getattr(
            dbapi, "Logical", getattr(dbapi, "Boolean", getattr(dbapi, "Bit", None))
        )

    @property
    def python_type(self):
        """The equivalent Python type."""
        return bool

    def literal_processor(self, dialect):
        """A callable to transform the value."""
        return strtobool


class Date(sqla_types.Date):
    """A straight copy of the SQLAlchemy Date type."""

    __odbc_datatype__ = 9
    __visit_name__ = "date"
    __intersolv_name__ = "Date"

    def get_dbapi_type(self, dbapi):
        """The equivalent type from pyodbc."""
        return getattr(dbapi, "Date", None)

    @property
    def python_type(self):
        """The equivalent Python type."""
        return date

    def literal_processor(self, dialect):
        """A callable to transform the value."""
        return lambda value: "".join(("{", value.strftime("%m/%d/%Y"), "}"))


class Time(sqla_types.Time):
    """A straight copy of the SQLAlchemy Time type."""

    __odbc_datatype__ = 10
    __visit_name__ = "time"
    __intersolv_name__ = "Time"

    def get_dbapi_type(self, dbapi):
        """The equivalent type from pyodbc."""
        return getattr(dbapi, "Time", None)

    @property
    def python_type(self):
        """The equivalent Python type."""
        return time

    def literal_processor(self, dialect):
        """A callable to transform the value."""
        return lambda value: "".join(("{", value.strftime("%H:%M:%S"), "}"))


class Timestamp(sqla_types.TIMESTAMP):
    """A straight copy of the SQLAlchemy TIMESTAMP type."""

    __odbc_datatype__ = 11
    __visit_name__ = "timestamp"
    __intersolv_name__ = "TimeStamp"

    def get_dbapi_type(self, dbapi):
        """The equivalent type from pyodbc."""
        return getattr(dbapi, "Timestamp", None)

    @property
    def python_type(self):
        """The equivalent Python type."""
        return datetime

    def literal_processor(self, dialect):
        """A callable to transform the value."""
        return lambda value: "".join(("{", value.strftime("%m/%d/%Y %H:%M:%S"), "}"))


# Map names returned by the "type_name" column of pyodbc's
# Cursor.columns method to the Paradox dialect-specific sqla_types.

ischema_names = {
    "ALPHA": LongVarChar,
    "AUTOINCREMENT": BigInt,
    "BCD": Decimal,
    "BINARY": LongVarBinary,
    "BYTES": Binary,
    "DATE": Date,
    "FORMATTEDMEMO": LongVarBinary,
    "GRAPHIC": LongVarBinary,
    "LOGICAL": Logical,
    "LONGINTEGER": BigInt,
    "LONG INTEGER": BigInt,
    "MEMO": LongVarChar,
    "MONEY": Double,
    "NUMBER": Double,
    "OLE": LongVarBinary,
    "SHORT": SmallInt,
    "TIME": Time,
    "TIMESTAMP": Timestamp,
}


sqla_functions = {
    functions.coalesce: "COALESCE",
    functions.current_date: "CURRENT_DATE",
    functions.current_time: "CURRENT_TIME",
    functions.current_timestamp: "CURRENT_TIMESTAMP",
    functions.current_user: "CURRENT_USER",
    functions.localtime: "LOCALTIME",
    functions.localtimestamp: "LOCALTIMESTAMP",
    functions.random: "RANDOM",
    functions.sysdate: "SYSDATE",
    functions.session_user: "SESSION_USER",
    functions.user: "USER",
    # functions.cube: "CUBE",
    # functions.rollup: "ROLLUP",
    # functions.grouping_sets: "GROUPING SETS",
    functions.max: "MAX",
}

# string constants must be enclosed in single quotes
# date constants must be enclosed in curly braces - {}
# dates must be formatted MM/DD/YYYY
# times must be formatted HH:MM:SS
# booleans must be .T. or 1 and .F. or 0


class ParadoxTypeCompiler(compiler.GenericTypeCompiler):
    """Paradox Type Compiler."""


class ParadoxExecutionContext(default.DefaultExecutionContext):
    """Paradox Execution Context."""

    def get_lastrowid(self):
        """Get the id of the last inserted row."""
        # self.cursor.execute("SELECT @@identity AS lastrowid")
        # return self.cursor.fetchone()[0]
        super(ParadoxExecutionContext, self).get_lastrowid()


class ParadoxSQLCompiler(compiler.SQLCompiler):
    """Paradox Compiler."""

    created_tables: Dict[str, Dict[str, Any]] = dict()
    deferred: Set[str] = set()

    intersolv_type_map: Dict[str, str] = {
        "ARRAY": None,
        "BIGINT": "Long Integer",
        "BINARY": "Binary",
        "BLOB": "Binary",
        "BOOLEAN": "Logical",
        "big_integer": "Long Integer",
        "large_binary": "Binary",
        "boolean": "Logical",
        "CHAR": "Alpha",
        "CLOB": None,
        "DATE": "Date",
        "DATETIME": "TimeStamp",
        "DECIMAL": "Number",
        "date": "Date",
        "datetime": "TimeStamp",
        "enum": None,
        "FLOAT": "Number",
        "float": "Number",
        "INTEGER": "Long Integer",
        "integer": "Long Integer",
        "type_decorator": None,
        "JSON": "Memo",
        "NCHAR": "Alpha",
        "null": None,
        "NUMERIC": "Number",
        "NVARCHAR": "Alpha",
        "numeric": "Number",
        "REAL": "Number",
        "SMALLINT": "Short",
        "small_integer": "Short",
        "string": "Alpha",
        "TEXT": "Alpha",
        "TIME": "Time",
        "TIMESTAMP": "TimeStamp",
        "text": "Alpha",
        "time": "Time",
        "unicode": "Alpha",
        "unicode_text": "Alpha",
        "user_defined": None,
        "VARBINARY": "Binary",
        "VARCHAR": "Alpha",
    }

    operators = compiler.OPERATORS
    operators.update({sqla_operators.concat_op: " + "})

    intersolv_numeric_operators = {
        "+": "addition",
        "-": "subtraction",
        "*": "multiplication",
        "/": "division",
        "^": "exponentiation",
    }
    intersolv_character_operators = {
        "+": "concat (keep trailing blanks)",
        "-": "contact (move trailing blanks to end)",
    }
    intersolv_date_operators = {
        "+": "add a number of days to produce a new date",
        "-": "the number of days between two dates, or subtract a number of days to produce a new date",
    }
    intersolv_relational_operators = {
        "=": "equal",
        "<>": "not equal",
        ">": "greater than",
        ">=": "greater than or equal",
        "<": "less than",
        "<=": "less than or equal",
        "like": "matching a pattern",
        "not like": "not matching a pattern",
        "is null": "equal to null or none",
        "is not null": "not equal to null or none",
        "between": "range of values between a lower and upper bound",
        "in": "member of a set or subquery",
        "exists": "true if a subquery returns at least one row",
        "any": "compares a value to each value returned by a subquery, interchangeable with in",
        "all": "compares a value to each value returned by a subquery",
    }
    intersolv_logical_operators = {
        "and": "",
        "or": "",
    }
    intersolv_operator_precedence = [
        "-",
        "+",
        "**",
        "*",
        "/",
        "+",
        "-",
        "=",
        "<>",
        "<",
        "<=",
        ">",
        ">=",
        "like",
        "not like",
        "is null",
        "is not null",
        "between",
        "in",
        "exists",
        "any",
        "all",
        "not",
        "and",
        "or",
    ]

    intersolv_built_in_functions = {
        # Functions Returning Character Strings
        "CHR": "Converts an ASCII code into a one-character string",
        "RTRIM": "Removes trailing blanks from a string",
        "TRIM": "Removes trailing blanks from a string",
        "LTRIM": "Removes leading blanks from a string",
        "UPPER": "Changes each letter of a string to uppercase",
        "LOWER": "Changes each letter of a string to lowercase",
        "LEFT": "Returns leftmost n characters of a string",
        "RIGHT": "Returns rightmost n characters of a string",
        "SUBSTR": "Returns a substring of a string. Parameters are the string, start, and (optional) end positions.",
        "SPACE": "Generates a string of blanks",
        "DTOC": "Converts a date to a character string, with optional parameters for format and separator.",
        "DTOS": "Converts a date to a character string using the format YYYYMMDD",
        "IIF": """Returns one of two values. Parameters are a logical expression, the true value,
                  and the false value. If the logical expression evaluates to True, the function
                  returns the true value. Otherwise, it returns the false value
               """,
        "STR": """Converts a number to a character string. Parameters are the number, the total number
                  of output characters (including the decimal point), and optionally the number of digits
                  to the right of the decimal point.
               """,
        "STRVAL": "Converts a value of any type to a character string",
        "TIME": "Returns the time of day as a string",
        "TTOC": "Converts a timestamp to a character string with an optional second parameter for the format",
        "USERNAME": "The user name specified during configuration or connection, if supported, or an empty string",
        # Functions Returning Numbers
        "MOD": "Divides two numbers and returns the remainder of the division.",
        "LEN": "Returns the length of a string",
        "MONTH": "Returns the month part of a date",
        "DAY": "Returns the day part of a date",
        "YEAR": "Returns the year part of a date",
        "MAX": "Returns the larger of two numbers",
        "DAYOFWEEK": "Returns the day of week (1-7) of a date expression",
        "MIN": "Returns the smaller of two numbers",
        "POW": "Raises a number to a power",
        "INT": "Returns the integer part of a decimal number",
        "ROUND": "Rounds a decimal value to the specified number of spaces",
        "NUMVAL": "Converts a character string to a number. Returns 0 if the character string is not a valid number",
        "VAL": "Identical to NUMVAL",
        # Functions Returning Dates
        "DATE": "Returns today's date",
        "TODAY": "Identical to DATE",
        "DATEVAL": "Converts a character string to a date",
        "CTOD": "Converts a character string to a date with an optional second parameter for the format",
    }
    intersolv_supported_odbc_api_functions = {
        "SQLAllocConnect",
        "SQLAllocEnv",
        "SQLAllocHandle",
        "SQLAllocStmt",
        "SQLBindCol",
        "SQLBindParameter",
        "SQLBrowseConnect",
        "SQLBulkOperations",
        "SQLCancel",
        "SQLCloseCursor",
        "SQLColAttribute",
        "SQLColAttributes",
        "SQLColumns",
        "SQLConnect",
        "SQLCopyDesc",
        "SQLDataSources",
        "SQLDescribeCol",
        "SQLDisconnect",
        "SQLDriverConnect",
        "SQLDrivers",
        "SQLEndTran",
        "SQLError",
        "SQLExecDirect",
        "SQLExecute",
        "SQLExtendedFetch",
        "SQLFetch",
        "SQLFetchScroll",
        "SQLFreeConnect",
        "SQLFreeEnv",
        "SQLFreeHandle",
        "SQLFreeStmt",
        "SQLGetConnectAttr",
        "SQLGetConnectOption",
        "SQLGetCursorName",
        "SQLGetData",
        "SQLGetDescField",
        "SQLGetDescRec",
        "SQLGetDiagField",
        "SQLGetDiagRec",
        "SQLGetEnvAttr",
        "SQLGetFunctions",
        "SQLGetInfo",
        "SQLGetStmtAttr",
        "SQLGetStmtOption",
        "SQLGetTypeInfo",
        "SQLMoreResults",
        "SQLNativeSql",
        "SQLNumParams",
        "SQLNumParens",
        "SQLNumResultCols",
        "SQLParamData",
        "SQLParamOptions",
        "SQLPrepare",
        "SQLPutData",
        "SQLRowCount",
        "SQLSetConnectAttr",
        "SQLSetConnectOption",
        "SQLSetCursorName",
        "SQLSetDescField",
        "SQLSetDescRec",
        "SQLSetEnvAttr",
        "SQLSetScrollOptions",
        "SQLSetStmtAttr",
        "SQLSetStmtOption",
        "SQLSpecialColumns",
        "SQLStatistics",
        "SQLTables",
        "SQLTransact",
    }
    intersolv_aggregate_functions = {
        "SUM": "The total of the values in a numeric field expression",
        "AVG": "The average of the values in a numeric field expression",
        "COUNT": "The number of values in any field expression",
        "MAX": "The maximum value in any field expression",
        "MIN": "The minimum value in any field expression",
    }
    intersolv_scalar_functions = {
        # String Functions
        "ASCII": "",
        "BIT_LENGTH": "",
        "CHAR": "",
        "CHAR_LENGTH": "",
        "CHARACTER_LENGTH": "",
        "CONCAT": "",
        "DIFFERENCE": "",
        "INSERT": "",
        "LCASE": "",
        "LEFT": "",
        "LENGTH": "",
        "LOCATE": "",
        "LTRIM": "",
        "OCTET_LENGTH": "",
        "POSITION": "",
        "REPEAT": "",
        "REPLACE": "",
        "RIGHT": "",
        "RTRIM": "",
        "SOUNDEX": "",
        "SPACE": "",
        "SUBSTRING": "",
        "UCASE": "",
        # Numeric Functions
        "ABS": "",
        "ACOS": "",
        "ASIN": "",
        "ATAN": "",
        "ATAN2": "",
        "CEILING": "",
        "COS": "",
        "COT": "",
        "DEGREES": "",
        "EXP": "",
        "FLOOR": "",
        "LOG": "",
        "LOG10": "",
        "MOD": "",
        "PI": "",
        "POWER": "",
        "RADIANS": "",
        "RAND": "",
        "ROUND": "",
        "SIGN": "",
        "SIN": "",
        "SQRT": "",
        "TAN": "",
        "TRUNCATE": "",
        # Date / Time Functions
        "CURRENT_DATE": "",
        "CURRENT_TIME": "",
        "CURRENT_TIMESTAMP": "",
        "CURDATE": "",
        "CURTIME": "",
        "DAYNAME": "",
        "DAYOFMONTH": "",
        "DAYOFWEEK": "",
        "DAYOFYEAR": "",
        "HOUR": "",
        "MINUTE": "",
        "MONTH": "",
        "MONTHNAME": "",
        "NOW": "",
        "QUARTER": "",
        "SECOND": "",
        "TIMESTAMPADD": "",
        "TIMESTAMPDIFF": "",
        "WEEK": "",
        "YEAR": "",
        # System Functions
        "DATABASE": "",
        "IFNULL": "",
        "USER": "",
    }

    function_rewrites = {
        "current_date": "now",
        "current_timestamp": "now",
        "length": "len",
    }

    _setup_crud_hints: Callable
    _generate_prefixes: Callable
    _render_cte_clause: Callable
    _truncated_identifier: Callable
    _get_operator_dispatch: Callable
    _generate_generic_binary: Callable
    _generate_generic_unary_modifier: Callable
    _generate_generic_unary_operator: Callable

    def visit_label(
        self,
        label,
        add_to_result_map=None,
        within_label_clause=False,
        within_columns_clause=False,
        render_label_as_label=None,
        **kw,
    ):
        """Render labels."""
        # only render labels within the columns clause
        # or ORDER BY clause of a select.  dialect-specific compilers
        # can modify this behavior.
        render_label_with_as = within_columns_clause and not within_label_clause
        render_label_only = render_label_as_label is label

        labelname = ""

        if render_label_only or render_label_with_as:
            if isinstance(label.name, getattr(elements, "_truncated_label", None)):
                labelname = self._truncated_identifier("colident", label.name)
            else:
                labelname = label.name

        if render_label_with_as:
            if add_to_result_map is not None:
                add_to_result_map(
                    labelname,
                    label.name,
                    (label, labelname) + label._alt_names,
                    label.type,
                )

            return (
                label.element._compiler_dispatch(
                    self, within_columns_clause=True, within_label_clause=True, **kw
                )
                + self.operators[sqla_operators.as_]
                + self.preparer.format_label(label, labelname)
            )
        elif render_label_only:
            return self.preparer.format_label(label, labelname)
        else:
            return label.element._compiler_dispatch(
                self, within_columns_clause=False, **kw
            )

    def visit_function(self, func, add_to_result_map=None, **kwargs):
        """Ensure that function calls work properly."""
        # For the most part, this is *identical* to the implementation
        # of the super method, it just surrounds any scalar function calls
        # with {fn FUNCTION} so that the Intersolv driver will pick them
        # up correctly

        if add_to_result_map is not None:
            add_to_result_map(func.name, func.name, (), func.type)

        disp = getattr(self, f"visit_{func.name.lower()}_func", None)
        if disp:
            return disp(func, **kwargs)
        else:
            name = sqla_functions.get(func.__class__, None)
            if name:
                name = name.upper()
                if getattr(func, "_has_args", False):
                    name += "%(expr)s"
            else:
                name = func.name
                if any(
                    (
                        getattr(self.preparer, "_requires_quotes_illegal_chars", bool)(
                            name
                        ),
                        isinstance(name, elements.quoted_name),
                    )
                ):
                    name = self.preparer.quote(name)
                name = f"{name.upper()}%(expr)s"

            ret_list = [name]
            for tok in func.packagenames:
                if any(
                    (
                        getattr(self.preparer, "_requires_quotes_illegal_chars", bool)(
                            tok
                        ),
                        isinstance(name, elements.quoted_name),
                    )
                ):
                    ret_list.append(self.preparer.quote(tok))
                else:
                    ret_list.append(tok)

            arg_spec = self.function_argspec(func, **kwargs)
            ret_string = ".".join(ret_list)
            ret_string %= {
                "expr": arg_spec,
            }

            if cl_in(func.name, set(self.intersolv_scalar_functions.keys())):
                ret_string = "".join(("{fn " + ret_string + "}"))

            return ret_string

    def visit_clauselist(self, clauselist, **kw):
        """Render clauses."""
        sep = clauselist.operator
        if sep is None:
            sep = " "
        else:
            sep = self.operators[clauselist.operator]

        text = sep.join(
            s
            for s in (c._compiler_dispatch(self, **kw) for c in clauselist.clauses)
            if s
        )
        if clauselist._tuple_values and getattr(self.dialect, "tuple_in_values", False):
            text = "VALUES " + text
        return text

    def limit_clause(self, *args, **kwargs):
        """The Intersolv Paradox driver doesn't support limit or top."""
        return ""

    def order_by_clause(self, select, **kw):
        """Ensure that order by clauses use numeric column ids."""

        # The Intersolv driver will accept named columns for simple selects,
        # but wants numeric column positions instead for union-ed or joined selects.

        dispatch = select._order_by_clause._compiler_dispatch(self, **kw)

        if not isinstance(select, CompoundSelect):
            ret_val = f" ORDER BY {dispatch}"
            return ret_val

        # Attempt to find the columns numeric position in the selectable
        # noinspection PyTypeChecker
        column_order: List[Tuple[int, str]] = list(enumerate(map(str, select.columns)))
        col_index = next(
            filter(lambda col: col[1] in dispatch, column_order), (-1, -1)
        )[0]

        sort_order = ""

        if any(
            (
                dispatch.casefold().endswith(" desc"),
                dispatch.casefold().endswith(" asc"),
            )
        ):
            sort_order = dispatch.split(" ")[-1]

        # Compensate for python's zero-indexing and then take the highest column number
        # which will either be the correct one, or the index would have come back as -1
        # which means we'll use 1 instead
        col_index = max((col_index + 1, 1))
        ret_val = f" ORDER BY {col_index}"

        if sort_order:
            ret_val += f" {sort_order}"

        return ret_val

    def visit_compound_select(
        self, cs, asfrom=False, parens=True, compound_index=0, **kwargs
    ) -> str:
        """Emit correctly formatted compound select statements."""
        ret_val = super().visit_compound_select(
            cs, asfrom, parens, compound_index, **kwargs
        )

        # The Intersolv driver doesn't allow more than one order_by clause
        # to be included in compound select statements, so we'll need to
        # strip out any extras we come across.

        if ret_val.casefold().count("order by") > 1:
            last_order_by = ret_val[ret_val.casefold().rfind("order by") :]
            ret_val = ret_val.split("UNION")

            for pos, val in enumerate(ret_val):
                val = val[val.find("(") + 1 :]
                val = val[: val.rfind(")")].strip()

                ret_val[pos] = f"({val[:val.rfind('ORDER BY')].strip()})"

            ret_val = " UNION ".join(ret_val) + f" {last_order_by}"

        return ret_val

    def visit_case(self, clause, **kwargs: Any) -> str:
        """Ensure CASE statements are handled correctly."""

        template = "IIF(condition, truthy, falsy)"
        whens = list()
        clause_value = None
        else_ = "NULL"

        ret_val = ""

        for cond, result in clause.whens:
            whens.append(
                (
                    cond._compiler_dispatch(self, **kwargs),
                    result._compiler_dispatch(self, **kwargs),
                )
            )

        if clause.else_ is not None:
            else_ = clause.else_._compiler_dispatch(self, **kwargs)

        if clause.value is not None:
            clause_value = clause.value._compiler_dispatch(self, **kwargs)
            raise RuntimeError("Encountered non-null clause value!")

        if whens:
            ret_val = template.replace("condition", whens[0][0]).replace(
                "truthy", whens[0][1]
            )

        if len(whens) > 1:
            for when in whens:
                ret_val = ret_val.replace(
                    "falsy",
                    template.replace("condition", when[0]).replace("truthy", when[1]),
                )

        ret_val = ret_val.replace("falsy", else_)

        return ret_val

    def visit_select(
        self,
        select,
        asfrom=False,
        parens=True,
        fromhints=None,
        compound_index=0,
        nested_join_translation=False,
        select_wraps_for=None,
        lateral=False,
        **kwargs,
    ) -> str:
        """Emit correctly formatted SELECT statements."""
        ret_val = super(ParadoxSQLCompiler, self).visit_select(
            select,
            asfrom,
            parens,
            fromhints,
            compound_index,
            nested_join_translation,
            select_wraps_for,
            lateral,
            **kwargs,
        )

        ret_val = ret_val.replace("`*`", "*")

        if cl_in("exists", ret_val):
            from_table = ret_val[ret_val.casefold().find("from ") :].split()[1]
            split_pos = ret_val.casefold().find("where exists")
            ret_val = " ".join(
                map(
                    str.strip,
                    (ret_val[:split_pos], f"FROM {from_table}", ret_val[split_pos:]),
                )
            )

        return ret_val

    def visit_unary(self, unary, **kw):
        """Render unary statements."""
        if unary.operator:
            if unary.modifier:
                raise CompileError(
                    "Unary expression does not support operator "
                    "and modifier simultaneously"
                )
            disp = self._get_operator_dispatch(unary.operator, "unary", "operator")
            if disp:
                return disp(unary, unary.operator, **kw)
            else:
                return self._generate_generic_unary_operator(
                    unary, self.operators[unary.operator], **kw
                )
        elif unary.modifier:
            disp = self._get_operator_dispatch(unary.modifier, "unary", "modifier")
            if disp:
                return disp(unary, unary.modifier, **kw)
            else:
                return self._generate_generic_unary_modifier(
                    unary, self.operators[unary.modifier], **kw
                )
        else:
            raise CompileError("Unary expression has no operator or modifier")

    def visit_binary(self, binary, override_operator=None, eager_grouping=False, **kw):
        """Render binary statements."""

        if (
            self.ansi_bind_rules
            and isinstance(binary.left, elements.BindParameter)
            and isinstance(binary.right, elements.BindParameter)
        ):
            kw["literal_binds"] = True

        operator_ = override_operator or binary.operator
        disp = self._get_operator_dispatch(operator_, "binary", None)
        if disp:
            return disp(binary, operator_, **kw)
        else:
            try:
                opstring = self.operators[operator_]
            except KeyError as err:
                raise_(
                    UnsupportedCompilationError(self, operator_), replace_context=err,
                )
            else:
                return self._generate_generic_binary(binary, opstring, **kw)

    def visit_like_op_binary(self, binary, operator, **kw):
        """Emit properly formatted LIKE comparisons."""
        escape = binary.modifiers.get("escape", None)

        left, right = (
            binary.left._compiler_dispatch(self, **kw),
            binary.right._compiler_dispatch(self, **kw),
        )

        ret_val = "%s LIKE %s" % (left, right)

        if "||" in ret_val:
            ret_val = ret_val.replace("||", "+")

        if escape:
            escape = self.render_literal_value(escape, STRINGTYPE)
            escape = escape.replace("'", "") if set(iter(escape)) != {"'"} else None
            if escape:
                # NOTE: "\u0192" = ƒ
                escape = "".join((" ", "\u0192", escape, "\u0192"))
                ret_val += escape

        return ret_val


# noinspection SqlNoDataSourceInspection
class ParadoxDDLCompiler(compiler.DDLCompiler):
    """Paradox DDL Compiler."""

    sql_compiler: ParadoxSQLCompiler
    _verify_index_table: Callable
    _prepared_index_name: Callable

    def __column_def(self, col):
        """Create a valid Paradox column definition."""
        if col.element.autoincrement is True:
            return f"`{col.element.name}` AutoIncrement"

        intersolv_name = getattr(col.element.type, "__intersolv_name__", None)
        visit_name = getattr(col.element.type, "__visit_name__", None)

        type_name = intersolv_name or cg(
            getattr(self.sql_compiler, "intersolv_type_map", dict()), visit_name, None,
        )

        if not type_name:
            raise SQLAlchemyError(
                f"No usable column type provided for column '{col.element.name}' !"
            )

        col_def = f"`{col.element.name}` {type_name}"

        if type_name.casefold() == "alpha":
            col_length = getattr(col.element.type, "length", 255)
            col_length = {None: 255, 0: 1}.get(col_length, col_length)
            col_def += f"({col_length or 1})"

        if all(
            (
                type_name.casefold() != "logical",
                col.element.nullable is False,
                col.element.autoincrement is not True,
            )
        ):
            col_def += " NOT NULL"

        if any(
            (
                type_name.casefold() == "logical",
                all(
                    (
                        col.element.autoincrement is not True,
                        getattr(col.element.default, "arg", None) is not None,
                        not isinstance(
                            getattr(col.element.default, "arg", None),
                            (elements.BinaryExpression, elements.BindParameter,),
                        ),
                    )
                ),
            )
        ):
            def_value = (
                int(bool(getattr(col.element.default, "arg", None)))
                if type_name.casefold() == "logical"
                else col.element.default.arg
            )
            col_def += f" DEFAULT {def_value}"

        return col_def

    def visit_create_table(self, create):
        """Create the specified table."""

        table = create.element
        columns = create.columns
        preparer = self.preparer
        table_name = preparer.format_table(table)
        pk_cols = list(filter(lambda item: item.element.primary_key is True, columns))
        non_pk_cols = list(
            filter(lambda item: item.element.primary_key is not True, columns)
        )

        # Add a record of the table we're about to create
        # to the semi-persistent dictionary `self.sql_compiler.created_tables`
        # so that we can any indexes that need to be created can
        # be created in the correct order
        self.sql_compiler.created_tables[table.name] = {
            "primary_keys": [col.element.name for col in pk_cols],
            "created_indexes": set(),
        }

        # Create the proper Paradox-formatted table creation statement
        # NOTE: primary-key columns are filtered from non-primary-key
        # columns below as Paradox requires that all primary-key columns
        # are contiguous in the resulting primary index
        statement = "CREATE "
        if table._prefixes:
            statement += " ".join(table._prefixes) + " "
        statement += f"TABLE {table_name} "

        create_table_suffix = self.create_table_suffix(table)
        if create_table_suffix:
            statement += create_table_suffix + " "

        statement += "("
        # statement = f"CREATE TABLE `{create.element.name}` ("

        if pk_cols:
            statement += ", ".join(map(self.__column_def, pk_cols))
            index_statement = "".join(
                (
                    f"CREATE UNIQUE INDEX PRIMARY ON {preparer.format_table(table)} (",
                    ", ".join(map(preparer.quote, (pk.element.name for pk in pk_cols))),
                    ")",
                )
            )
            self.sql_compiler.deferred.add(index_statement)
            self.sql_compiler.created_tables[table.name]["created_indexes"].add(
                "PRIMARY"
            )
        if pk_cols and non_pk_cols:
            statement += ", "
        if non_pk_cols:
            statement += ", ".join(map(self.__column_def, non_pk_cols))

        statement += ")"

        return statement

    def visit_create_index(
        self, create, include_schema=False, include_table_schema=True
    ):
        """Create the specified index(s) on the specified table."""

        index = create.element
        self._verify_index_table(index)
        preparer = self.preparer
        not_grave = lambda item: item if str(item) != "`" else ""
        index_name = index_name = f"`ix_{uuid4().hex.upper()[:8]}`"
        table_name = preparer.format_table(index.table, use_schema=include_table_schema)

        if index.name is not None:
            index_name = self._prepared_index_name(
                index, include_schema=include_schema
            ).replace(" ", "_")

        # Create a quoted list of columns in the index
        columns = "".join(
            (
                "(",
                ", ".join(
                    map(preparer.quote, (col.name for col in create.element.columns))
                ),
                ")",
            )
        )

        # Assume we're not going to create the primary index and format the index creation statement accordingly
        statement = f"CREATE "
        if index.unique:
            statement += "UNIQUE "
        statement += f"INDEX {index_name} /CASE_INSENSITIVE ON {table_name} {columns}"

        # Check to see if the supplied table was during this session and whether or not the primary index was
        # too, as non-primary indexes can't be created on tables that don't already have a primary index
        if all(
            (
                cl_in(index.table.name, self.sql_compiler.created_tables.keys()),
                not cl_in(
                    "primary",
                    cg(
                        cg(self.sql_compiler.created_tables, index.table.name, dict(),),
                        "created_indexes",
                        set(),
                    ),
                ),
            )
        ):
            # Assuming that we did in fact create the table but didn't find a record telling us we've already
            # created the primary index, check to see if the statement we already created is trying to do so
            if "".join(map(not_grave, index_name)).casefold() != "primary":
                # If the statement we already created is intended to create a non-primary index
                # but it doesn't look like the table in question already has one, deffer the index's
                # creation until the "next round" so to speak and swap out the statement with one
                # that will create the required primary index instead
                self.sql_compiler.deferred.add(statement)

            # Scrub the previously created index name and statement values
            index_name, statement = "", ""

            # Pull the stored set of primary keys
            primary_keys = cg(
                cg(self.sql_compiler.created_tables, index.table.name, dict(),),
                "primary_keys",
                set(),
            )

            if primary_keys:
                # Replace them with known-good ones instead
                index_name = "PRIMARY"
                statement = "".join(
                    (
                        f"CREATE UNIQUE INDEX {index_name} ON {table_name} (",
                        ", ".join(map(preparer.quote, primary_keys)),
                        ")",
                    )
                )

        try:
            # Make sure we keep track of the newly created index
            self.sql_compiler.created_tables[index.table.name]["created_indexes"].add(
                index_name
            )
        except KeyError:
            pass

        del columns

        if all(
            (
                cl_in(index.table.name, self.sql_compiler.created_tables.keys()),
                cl_in(
                    "primary",
                    cg(
                        cg(self.sql_compiler.created_tables, index.table.name, dict(),),
                        "created_indexes",
                        set(),
                    ),
                ),
                cl_in("primary", statement),
                cl_in("case_insensitive", statement),
            )
        ):
            statement = ""

        # If we created the table in question ourselves, we should be returning
        # the correct index creation statement in it's correct order of creation
        #
        # If we are adding indexes to a pre-existing table that we didn't create,
        # none of the code above should have been called and we should be returning
        # a correctly formatted index creation statement anyway

        return statement


class ParadoxIdentifierPreparer(compiler.IdentifierPreparer):
    """Paradox Identifier Preparer."""

    _double_percents: bool = True

    # The Intersolv Paradox driver is almost disgustingly permissive about
    # table and column names, so there really aren't any illegal characters
    illegal_initial_characters = set()

    # Paradox tables are actually flat files though, so there *are* characters
    # that are illegal  for the name of individual tables
    character_substitutions = {
        chr(34): chr(39) * 2,
        chr(96): "",
    }

    reserved_words = compiler.RESERVED_WORDS.copy()
    reserved_words.update(
        {
            "as",
            "at",
            "any",
            "avg",
            "are",
            "add",
            "asc",
            "ada",
            "all",
            "and",
            "alter",
            "asser",
            "absolute",
            "allocate",
            "authorization",
            "by",
            "bit",
            "begin",
            "between",
            "bit_length",
            "char",
            "case",
            "cast",
            "cobol",
            "cross",
            "close",
            "check",
            "count",
            "curre",
            "column",
            "commit",
            "create",
            "cursor",
            "collate",
            "convert",
            "compute",
            "connect",
            "cascade",
            "current",
            "catalog",
            "coalesce",
            "cascaded",
            "continue",
            "collation",
            "character",
            "connection",
            "constraint",
            "constraints",
            "char_length",
            "current_time",
            "corresponding",
            "character_length",
            "current_timestamp",
            "dec",
            "day",
            "desc",
            "drop",
            "date",
            "double",
            "delete",
            "domain",
            "declare",
            "decimal",
            "distinct",
            "deferred",
            "describe",
            "disconnect",
            "deferrable",
            "dictionary",
            "deallocate",
            "descriptor",
            "diagnostics",
            "displacement",
            "end",
            "exec",
            "else",
            "except",
            "exists",
            "escape",
            "execute",
            "extract",
            "external",
            "end-exec",
            "exception",
            "for",
            "from",
            "full",
            "float",
            "fetch",
            "false",
            "first",
            "found",
            "foreign",
            "fortran",
            "go",
            "get",
            "goto",
            "grant",
            "group",
            "global",
            "hour",
            "having",
            "is",
            "in",
            "into",
            "inner",
            "index",
            "input",
            "ignore",
            "insert",
            "integer",
            "include",
            "interval",
            "identity",
            "isolation",
            "immediate",
            "intersect",
            "initially",
            "indicator",
            "insensitive",
            "join",
            "key",
            "last",
            "like",
            "left",
            "lower",
            "level",
            "local",
            "language",
            "min",
            "max",
            "month",
            "match",
            "mumps",
            "module",
            "minute",
            "not",
            "null",
            "next",
            "none",
            "names",
            "nchar",
            "nullif",
            "natural",
            "numeric",
            "national",
            "of",
            "or",
            "on",
            "off",
            "only",
            "open",
            "order",
            "outer",
            "output",
            "option",
            "options",
            "overlaps",
            "octet_length",
            "pli",
            "page",
            "prior",
            "pascal",
            "public",
            "partial",
            "prepare",
            "primary",
            "preserve",
            "position",
            "procedure",
            "precision",
            "privileges",
            "rows",
            "right",
            "revoke",
            "restrict",
            "rollback",
            "set",
            "sql",
            "sum",
            "some",
            "size",
            "sqlca",
            "system",
            "schema",
            "second",
            "scroll",
            "select",
            "section",
            "sqlcode",
            "sqlerror",
            "sqlstate",
            "smallint",
            "sequence",
            "substring",
            "sqlwarning",
            "to",
            "true",
            "time",
            "then",
            "table",
            "temporary",
            "timestamp",
            "translate",
            "transaction",
            "translation",
            "timezone_hour",
            "timezone_minute",
            "user",
            "using",
            "upper",
            "union",
            "usage",
            "update",
            "unique",
            "unknown",
            "view",
            "value",
            "values",
            "varying",
            "varchar",
            "when",
            "with",
            "work",
            "where",
            "whenever",
            "year",
        }
    )

    @staticmethod
    def _not_grave(item: Any) -> str:
        """Allow any non-` character through the filter"""
        if str(item) != "`":
            return str(item)
        return ""

    def __init__(self, dialect):
        # The Intersolv driver uses the grave character (ASCII 96) to quote *everything*
        super(ParadoxIdentifierPreparer, self).__init__(
            dialect, initial_quote="`", final_quote="`"
        )

    def _requires_quotes(self, value):
        """Return True if the given identifier requires quoting."""

        # Does this look weird as hell? Absolutely. Should you mess with it?
        # Absolutely not.
        #
        # The Intersolv driver is so permissive that it's a pretty decent idea to
        # just quote *everything*, but changing this to just return True will trigger
        # code quality tooling to flag this method as "potentially being static" and
        # then pitching a fit over it not being decorated with @staticmethod.
        # Doing so, however, will cause SQLAlchemy to throw an error, so instead
        # we'll just convert the resulting boolean value of the underlying super method
        # logic to an integer, then return the boolean value of a call to max() between
        # that and the number 5, which is a convoluted but ultimately functional way
        # to force the method to *always* return True without causing the code quality
        # tooling to pitch a fit about it.

        lc_value = value.lower()
        result = int(
            bool(
                any(
                    (
                        lc_value in self.reserved_words,
                        value[0] in self.illegal_initial_characters,
                        lc_value != value,
                    )
                )
            )
        )
        return bool(max((result, 5)))

    def quote(self, ident, force=None):
        """Unconditionally quote an identifier.

        The identifier is quoted if it is a reserved word, contains
        quote-necessary characters, or is an instance of
        :class:`.quoted_name` which includes ``quote`` set to ``True``.

        Additionally, any characters designated as illegal and therefore
        requiring replacement are replaced with the values specified in
        :class:`.character_substitutions`.
        """
        ret_val = "".join(map(self._not_grave, super().quote(ident, force)))
        for key, value in self.character_substitutions.items():
            ret_val = ret_val.replace(key, value)
            del key, value
        return f"`{ret_val}`"


# noinspection PyArgumentList
class ParadoxDialect(default.DefaultDialect):
    """Paradox Dialect."""

    name = "paradox"
    dbapi = pyodbc

    # Supported parameter styles: ["qmark", "numeric", "named", "format", "pyformat"]
    default_paramstyle = "pyformat"
    dbapi.paramstyle = default_paramstyle

    poolclass = pool.SingletonThreadPool
    statement_compiler = ParadoxSQLCompiler
    ddl_compiler = ParadoxDDLCompiler
    type_compiler = ParadoxTypeCompiler
    preparer = ParadoxIdentifierPreparer
    execution_ctx_cls = ParadoxExecutionContext

    postfetch_lastrowid = False
    inline_comments = False

    supports_alter = True
    supports_views = False
    supports_comments = False
    supports_sequences = False
    supports_native_enum = False
    supports_empty_insert = False
    supports_native_boolean = False
    supports_sane_rowcount = False
    supports_for_update_of = False
    supports_default_values = True
    supports_native_decimal = False
    supports_is_distinct_from = False
    supports_right_nested_joins = False
    supports_multivalues_insert = False
    supports_sane_multi_rowcount = False
    supports_server_side_cursors = False
    supports_simple_order_by_label = False

    @staticmethod
    def _check_unicode_returns(*args: Any, **kwargs: Any):
        """Check if the local system supplies unicode returns."""
        # The driver should pretty much always be running on a modern
        # Windows system, so it's more or less safe to assume we'll
        # always get a unicode string back for string values
        return True

    @reflection.cache
    def has_table(self, connection, table_name, schema=None, **kw):
        """Check the existence of a particular table in the database.

        Given a :class:`_engine.Connection` object and a string
        `table_name`, return True if the given table (possibly within
        the specified `schema`) exists in the database, False
        otherwise.
        """
        table_name = self.identifier_preparer.quote(table_name)
        try:
            return (
                connection.engine.raw_connection()
                .cursor()
                .tables(table_name)
                .fetchone()
                is not None
            )
        except pyodbc.Error:
            return False

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        """Get the names of all the local tables."""

        # pyodbc table objects have the following properties:
        #
        # table_cat: The catalog name.
        # table_schem: The schema name.
        # table_name: The table name.
        # table_type: One of TABLE, VIEW, SYSTEM TABLE, GLOBAL TEMPORARY, LOCAL TEMPORARY, ALIAS, SYNONYM,
        #             or a data source-specific type name.
        # remarks: A description of the table.

        cursor = connection.engine.raw_connection().cursor()
        table_names = {
            table.table_name for table in cursor.tables(tableType="TABLE").fetchall()
        }
        return list(table_names)

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        """Get the names of all local views."""
        return [
            row[2]
            for row in connection.engine.raw_connection()
            .cursor()
            .tables(tableType="VIEW")
            .fetchall()
        ]

    def get_columns(self, connection, table_name, schema=None, **kw):
        """Get the column names and data-types for a given table."""

        table_name = self.identifier_preparer.quote(table_name)
        pyodbc_connection = connection.engine.raw_connection()
        pyodbc_cursor = pyodbc_connection.cursor()
        result = []

        for column in pyodbc_cursor.columns(table=table_name):

            column_class = ischema_names[column.type_name]
            column_type = column_class()
            if issubclass(column_class, (sqla_types.String, sqla_types.Text)):
                column_type.length = column.column_size
            elif issubclass(column_class, (sqla_types.DECIMAL, sqla_types.Float)):
                column_type.precision = column.column_size
                column_type.scale = column.decimal_digits
            result.append(
                {
                    "name": column.column_name,
                    "type": column_type,
                    "nullable": bool(
                        all((strtobool(column.nullable), strtobool(column.is_nullable)))
                    ),
                    "default": column.column_def,
                    "autoincrement": all(
                        (
                            column.ordinal_position == 1,
                            column.column_def is None,
                            cl_in("id", column.column_name),
                            cl_in(column.type_name, "LONG INTEGER"),
                            strtobool(column.nullable) is False,
                            strtobool(column.is_nullable) is False,
                        )
                    ),
                }
            )
        return result

    def get_pk_constraint(
        self, connection, table_name, schema=None, *args: Any, **kwargs: Any
    ):
        """ Return information about the primary key constraint on `table_name`.

            Given a :class:`_engine.Connection`, a string
            `table_name`, and an optional string `schema`, return primary
            key information as a dictionary with these keys:

            constrained_columns
              a list of column names that make up the primary key

            name
              optional name of the primary key constraint.
        """
        table_name = self.identifier_preparer.quote(table_name)
        pks = (
            connection.engine.raw_connection()
            .cursor()
            .primaryKeys(table_name)
            .fetchall()
        )

        if not pks:
            return {
                "name": None,
                "constrained_columns": [],
            }

        pk_name = max(set((row[5] for row in pks)) or {"PRIMARY"})

        return {
            "name": pk_name,
            "constrained_columns": [row[3] for row in pks],
        }

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        """Get the list of foreign keys from a given table."""
        # The Intersolv driver's support for foreign keys is semantic, at best
        # so it's *extremely* likely that this list will always be empty
        table_name = self.identifier_preparer.quote(table_name)
        return [
            key[3]
            for key in connection.engine.raw_connection()
            .cursor()
            .foreignKeys(table_name)
        ]

    @reflection.cache
    def get_indexes(self, connection, table_name, *args, **kwargs):
        """Return information about indexes in `table_name`.

        Given a :class:`_engine.Connection`, a string
        `table_name` and an optional string `schema`, return index
        information as a list of dictionaries with these keys:

        name
          the index's name

        column_names
          list of column names in order

        unique
          boolean
        """

        catalog = kwargs.get("catalog", None)
        schema = kwargs.get("schema", None)
        unique = kwargs.get("unique", False)
        quick = kwargs.get("catalog", False)
        pyodbc_cursor = connection.engine.raw_connection().cursor()
        table_name = self.identifier_preparer.quote(table_name)

        stat_cols = (
            "table_cat",
            "table_schem",
            "table_name",
            "non_unique",
            "index_qualifier",
            "index_name",
            "type",
            "ordinal_position",
            "column_name",
            "asc_or_desc",
            "cardinality",
            "pages",
            "filter_condition",
        )

        indexes = dict()

        for row in pyodbc_cursor.statistics(
            table_name, catalog=catalog, schema=schema, unique=unique, quick=quick
        ).fetchall():
            if getattr(row, "index_name", None) is not None:
                if row.index_name in indexes.keys():
                    indexes[row.index_name]["column_names"].append(row.column_name)
                else:
                    indexes[row.index_name] = {
                        "name": getattr(row, "index_name", None),
                        "unique": getattr(row, "non_unique", 1) == 0,
                        "column_names": [
                            getattr(row, "column_name", f"column_{str(uuid4())}")
                        ],
                    }
                    indexes[row.index_name].update(
                        {key: getattr(row, key, None) for key in stat_cols}
                    )

        return list(indexes.values())

    @reflection.cache
    def get_temp_table_names(self, connection, schema=None, **kw):
        """Get the names of any extant temporary tables."""
        return []

    @staticmethod
    def get_temp_view_names(*args: Any, **kwargs: Any):
        """Get the names of any temporary views."""
        # Paradox doesn't supply View functionality so this will always be empty
        return []

    @staticmethod
    def get_view_definition(*args, **kwargs):
        """Get the definition of a specific local view."""
        # Paradox doesn't supply View functionality
        return {}

    @reflection.cache
    def get_unique_constraints(self, *args, **kwargs):
        r"""Return information about unique constraints in `table_name`.

        Given a string `table_name` and an optional string `schema`, return
        unique constraint information as a list of dicts with these keys:

        name
          the unique constraint's name

        column_names
          list of column names in order

        \**kw
          other options passed to the dialect's get_unique_constraints()
          method
        """
        indexes = self.get_indexes(*args, **kwargs)

        return list(filter(lambda index: index.get("unique", False) is True, indexes))

    @staticmethod
    def get_check_constraints(*args: Any, **kwargs: Any):
        """The Intersolv driver doesn't really support constraints, other than NOT NULL."""
        return []

    @reflection.cache
    def get_table_comment(self, connection, table_name, *args, **kwargs):
        r"""Return the "comment" for the table identified by `table_name`.

        Given a string `table_name` and an optional string `schema`, return
        table comment information as a dictionary with this key:

        text
           text of the comment

        Raises ``NotImplementedError`` for dialects that don't support
        comments.
        """

        catalog = kwargs.get("catalog", None)
        schema = kwargs.get("schema", None)
        table_type = kwargs.get("tableType", None)
        pyodbc_cursor = connection.engine.raw_connection().cursor()
        table_name = self.identifier_preparer.quote(table_name)

        table_data = pyodbc_cursor.tables(
            table=table_name, catalog=catalog, schema=schema, tableType=table_type
        ).fetchone()
        comments = getattr(table_data, "remarks", "")

        return {"text": comments}

    @staticmethod
    def has_sequence(*args, **kwargs):
        """Paradox doesn't support sequences, so it will never have a queried sequence."""
        return False

    @staticmethod
    def __stringify(value: Any, **kwargs: Any) -> str:
        """Return a properly string-ified representation of the supplied value."""

        # Regardless of which driver we're using, Paradox only supports a handful of
        # data types. As such, we can use Python's ifinstance() builtin to check for
        # what kind of data we were given and proceed accordingly.

        escape = kwargs.get("escape", None)

        if value is None:
            return "NULL"
        if value is True:
            return "1"
        if value is False:
            return "0"
        if isinstance(value, Number):
            return str(value)
        if isinstance(value, bytes):
            return "".join(map(str, iter(value)))
        if isinstance(value, str):
            if escape:
                value = value.replace(escape, "\\", 1)
            value = value.replace("'", "''")
            return f"'{value}'"
        if all((isinstance(value, date), not isinstance(value, datetime))):
            return "".join(("{", value.strftime("%m/%d/%Y"), "}"))
        if all((isinstance(value, time), not isinstance(value, datetime))):
            return "".join(("{", value.strftime("%H:%M:%S"), "}"))
        if isinstance(value, datetime):
            return "".join(("{", value.strftime("%m/%d/%Y %H:%M:%S"), "}"))

        # This probably needs to be revisited, to account for extraneous types
        return f"{value}"

    def do_executemany(self, cursor, statement, parameters, context=None):
        """Insert DocString Here."""
        for num, param_set in enumerate(parameters):
            self.do_execute(cursor, statement, param_set, context)

    def do_execute(self, cursor, statement, parameters, context=None):
        """Insert DocString Here."""

        def log_statement(st, prms=tuple()):
            """Log the supplied about-to-be-executed statement."""
            from pathlib import Path

            statement_log = (Path().home() / "Downloads" / "statements.txt").resolve()
            if not isinstance(st, str):
                st = str(st)

            if statement_log.exists():
                try:
                    with statement_log.open("ab+") as writer:
                        st_ = st.replace("\n", " ").encode("utf8")
                        st_ = (
                            f"\nStatement: ".encode("utf8") + st_ + "\n".encode("utf8")
                        )
                        writer.write(st_)
                        if prms:
                            writer.write(f"Params: {prms}\n".encode("utf8"))
                except Exception as err:
                    print(f"{type(err)} -> {err}")

        # The Intersolv driver doesn't appear to like statements with placeholders
        # for values. It seems to behave much more reliably when handed pre-formatted
        # statements.

        if all(
            (
                "create index" in statement.casefold(),
                "primary" in statement.casefold(),
                "case_insensitive" in statement.casefold(),
            )
        ):
            statement = None

        escape = None
        if "\u0192" in statement:  # NOTE: "\u0192" = ƒ
            escape = statement[statement.find("\u0192") + 1 : statement.rfind("\u0192")]
            statement = statement.replace(
                statement[statement.find(" \u0192") : statement.rfind("\u0192") + 1], ""
            )

        parameters = {
            key: self.__stringify(value, escape=escape)
            for key, value in parameters.items()
        }
        statement %= parameters

        if statement:
            log_statement(statement, parameters)
            cursor.execute(statement)

        while self.statement_compiler.deferred:
            statement = self.statement_compiler.deferred.pop()
            log_statement(statement)
            cursor.execute(statement)
