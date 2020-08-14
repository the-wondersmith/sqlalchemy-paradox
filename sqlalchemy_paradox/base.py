""" Dialect Base
"""

from typing import Optional, Union, List, Dict, Any

from sqlalchemy.engine import default, reflection
from sqlalchemy.sql import compiler
from sqlalchemy import exc, util, types as sqla_types
from sqlalchemy.sql import elements, functions
from pyodbc import DatabaseError, Error
from itertools import chain
from uuid import uuid4
from re import match


chain = chain.from_iterable


# Paradox Types:
# These are relatively simple, as the underlying ODBC driver only *really* supports
# something like six different data types rather than the spectrum of modern SQL types


class DOUBLE(sqla_types.Float):
    """ Paradox DOUBLE type

        Shamelessly adapted from the MySQL DOUBLE type

        double precision / Size 15, NULLABLE, SEARCHABLE
    """

    __visit_name__ = "FLOAT"
    __sql_data_type__ = 8

    def __init__(self, precision: Optional[int] = None, scale: Optional[int] = None, asdecimal: bool = True, **kwargs: Optional[Any]) -> None:
        self.unsigned = kwargs.get("unsigned", False)
        self.zerofill = kwargs.get("zerofill", False)
        if all(
            (
                isinstance(self, DOUBLE),
                any(
                    (
                        all((precision is None, scale is not None)),
                        all((precision is not None, scale is None)),
                    )
                ),
            )
        ):
            raise exc.ArgumentError(
                "You must specify both precision and scale or omit both altogether."
            )
        super(DOUBLE, self).__init__(precision=precision, asdecimal=asdecimal, **kwargs)
        self.scale = scale

    def __repr__(self) -> str:
        return util.generic_repr(self, to_inspect=[sqla_types.Float])


class BINARY(sqla_types.BLOB):
    """ Long VarBinary
    """

    # Size 1073741823, NULLABLE, NOT SEARCHABLE
    __visit_name__: str = "BLOB"
    __sql_data_type__: int = -4


class LONGVARCHAR(sqla_types.Text):
    """ Long VarChar
    """

    # Size 1073741824, NULLABLE, NOT SEARCHABLE
    __visit_name__: str = "TEXT"
    __sql_data_type__: int = -1


class ALPHANUMERIC(sqla_types.VARCHAR):
    """ Alphanumeric / Generic VarChar
    """

    __visit_name__: str = "VARCHAR"
    __sql_data_type__: int = 12


class SHORT(sqla_types.SmallInteger):
    """ SmallInt
    """

    # Size 5, NULLABLE, SEARCHABLE
    __visit_name__: str = "SmallInteger"
    __sql_data_type__: int = 5


class PDOXDATE(sqla_types.Date):
    """ Paradox Date
    """

    # Size 10, NULLABLE, SEARCHABLE
    __visit_name__: str = "Date"
    __sql_data_type__: int = 9


Binary = BINARY
LongVarChar = LONGVARCHAR
AlphaNumeric = ALPHANUMERIC
Number = DOUBLE
Short = SHORT
PdoxDate = PDOXDATE


# ###
# Map integers returned by the "sql_data_type" column of pyodbc's Cursor.columns method to our dialect types.
#
# These names are what you would retrieve from INFORMATION_SCHEMA.COLUMNS.DATA_TYPE if Paradox
# supported those types of system views.
# ###
ischema_names = {
    -4: Binary,
    -1: LongVarChar,
    5: Short,
    8: Number,
    9: PdoxDate,
    12: AlphaNumeric,
}

SQL_KEYWORDS = {
    "ALPHANUMERIC",
    "AUTOINCREMENT",
    "BINARY",
    "BYTE",
    "COUNTER",
    "CURRENCY",
    "DATABASE",
    "DATABASENAME",
    "DATETIME",
    "DISALLOW",
    "DISTINCTROW",
    "DOUBLEFLOAT",
    "FLOAT4",
    "FLOAT8",
    "GENERAL",
    "IEEEDOUBLE",
    "IEEESINGLE",
    "IGNORE",
    "INT",
    "INTEGER1",
    "INTEGER2",
    "INTEGER4",
    "LEVEL",
    "LOGICAL",
    "LOGICAL1",
    "LONG",
    "LONGBINARY",
    "LONGCHAR",
    "LONGTEXT",
    "MEMO",
    "MONEY",
    "NOTE",
    "NUMBER",
    "OLEOBJECT",
    "OPTION",
    "OWNERACCESS",
    "PARAMETERS",
    "PERCENT",
    "PIVOT",
    "SHORT",
    "SINGLE",
    "SINGLEFLOAT",
    "SMALLINT",
    "STDEV",
    "STDEVP",
    "STRING",
    "TABLEID",
    "TEXT",
    "TOP",
    "TRANSFORM",
    "UNSIGNEDBYTE",
    "VALUES",
    "VAR",
    "VARBINARY",
    "VARP",
    "YESNO",
}

SPECIAL_CHARACTERS = {
    "~",
    "@",
    "#",
    "$",
    "%",
    "^",
    "&",
    "*",
    "_",
    "-",
    "+",
    "=",
    "\\",
    "}",
    "{",
    '"',
    "'",
    ";",
    ":",
    "?",
    "/",
    ">",
    "<",
    ",",
    ".",
    "!",
    "[",
    "]",
    "|",
}

FUNCTIONS = {
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
    functions.cube: "CUBE",
    functions.rollup: "ROLLUP",
    functions.grouping_sets: "GROUPING SETS",
}


class ParadoxSQLCompiler(compiler.SQLCompiler):
    """ Compiler
    """

    def visit_function(self, func, add_to_result_map=None, **kwargs):
        # For the most part, this is *identical* to the implementation
        # of the super method, it just surrounds the function calls
        # with {fn FUNCTION} so that the ODBC driver will pick them
        # up correctly
        if add_to_result_map is not None:
            add_to_result_map(func.name, func.name, (), func.type)

        disp = getattr(self, f"visit_{func.name.lower()}_func", None)
        if disp:
            return disp(func, **kwargs)
        else:
            name = FUNCTIONS.get(func.__class__, None)
            if name:
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
                name += "%(expr)s"

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

            ret_string = ".".join(ret_list) % {
                "expr": self.function_argspec(func, **kwargs)
            }
            ret_string = str("{fn " + ret_string + "}")
            return ret_string

    def get_select_precolumns(self, select, **kw):
        """ Paradox uses TOP after the SELECT keyword
            instead of LIMIT after the FROM clause
        """
        # (plagiarized from sqlalchemy_access/base.py)

        s = super(ParadoxSQLCompiler, self).get_select_precolumns(select, **kw)

        if getattr(select, "_simple_int_limit", False):
            # The Paradox ODBC driver doesn't support
            # bind params in the SELECT clause, so we'll
            # have to use a literal here instead
            s += f"TOP {getattr(select, '_limit')} "

        return s

    def limit_clause(self, select, **kw):
        """ Limit in Paradox is after the SELECT keyword
        """
        return ""

    # NOTE: The implementation of the *_binary methods below
    #       is effectively identical to the implementation of
    #       the super method for each function, they simply
    #       replace `LIKE` with `ALIKE` (ANSI LIKE) for improved
    #       compatibility with the underlying ODBC driver

    def visit_like_op_binary(self, binary, operator, **kw):
        escape = binary.modifiers.get("escape", None)
        left = getattr(binary.left, "_compiler_dispatch")(self, **kw)
        right = getattr(binary.right, "_compiler_dispatch")(self, **kw)
        ret_string = f"{left} ALIKE {right}"

        if escape:
            ret_string += (
                f" ESCAPE {self.render_literal_value(escape, sqla_types.STRINGTYPE)}"
            )

        return ret_string

    def visit_notlike_op_binary(self, binary, operator, **kw):
        escape = binary.modifiers.get("escape", None)
        left = getattr(binary.left, "_compiler_dispatch")(self, **kw)
        right = getattr(binary.right, "_compiler_dispatch")(self, **kw)
        ret_string = f"{left} NOT ALIKE {right} "

        if escape:
            ret_string += (
                f" ESCAPE {self.render_literal_value(escape, sqla_types.STRINGTYPE)}"
            )

        return ret_string

    def visit_ilike_op_binary(self, binary, operator, **kw):
        escape = binary.modifiers.get("escape", None)
        left = getattr(binary.left, "_compiler_dispatch")(self, **kw)
        right = getattr(binary.right, "_compiler_dispatch")(self, **kw)
        ret_string = "".join(
            ("{fn", f" LOWER({left})", "}", " ALIKE ", f" LOWER({right})", "}")
        )

        if escape:
            ret_string += (
                f" ESCAPE {self.render_literal_value(escape, sqla_types.STRINGTYPE)}"
            )

        return ret_string

    def visit_notilike_op_binary(self, binary, operator, **kw):
        escape = binary.modifiers.get("escape", None)
        left = getattr(binary.left, "_compiler_dispatch")(self, **kw)
        right = getattr(binary.right, "_compiler_dispatch")(self, **kw)
        ret_string = "".join(
            ("{fn", f" LOWER({left})", "}", " NOT ALIKE ", f" LOWER({right})", "}")
        )

        if escape:
            ret_string += (
                f" ESCAPE {self.render_literal_value(escape, sqla_types.STRINGTYPE)}"
            )

        return ret_string

    # All methods below here are effectively unimplemented, they only exist
    # to prevent an IDE from pitching a fit about required abstract methods

    def visit_sequence(self, *args, **kwargs):
        super(ParadoxSQLCompiler, self).visit_sequence(*args, **kwargs)

    def visit_empty_set_expr(self, *args, **kwargs):
        super(ParadoxSQLCompiler, self).visit_empty_set_expr(*args, **kwargs)

    def update_from_clause(self, *args, **kwargs):
        super(ParadoxSQLCompiler, self).update_from_clause(*args, **kwargs)

    def delete_extra_from_clause(self, *args, **kwargs):
        super(ParadoxSQLCompiler, self).delete_extra_from_clause(*args, **kwargs)


class ParadoxTypeCompiler(compiler.GenericTypeCompiler):
    """ Type Compiler
    """

    # The underlying driver *really* doesn't support much
    # in the way of datatypes, so this may be entirely perfunctory
    #
    # It's being done anyway to keep this library as in-line with
    # sqlalchemy-access as possible, as this library is a shameless
    # ripoff of sqlalchemy-access
    #
    # A bunch of the functions below should actually be decorated as
    # @staticmethod, but doing so will cause SQLAlchemy to pitch a fit
    # So, to keep the linters happy, they do something with the self
    # parameter by just asserting that it isn't None

    def visit_Float(self, type_, **kw):
        return super(ParadoxTypeCompiler, self).visit_FLOAT(type_, **kw)

    def visit_FLOAT(self, type_, **kw):
        return super(ParadoxTypeCompiler, self).visit_FLOAT(type_, **kw)

    def visit_DOUBLE(self, *args, **kwargs):
        if args:
            del args
        if kwargs:
            del kwargs
        assert self is not None
        return DOUBLE.__visit_name__

    def visit_BINARY(self, *args, **kwargs):
        return BINARY.__visit_name__

    def visit_LONGVARCHAR(self, *args, **kwargs):
        if args:
            del args
        if kwargs:
            del kwargs
        assert self is not None
        return LONGVARCHAR.__visit_name__

    def visit_ALPHANUMERIC(self, *args, **kwargs):
        if args:
            del args
        if kwargs:
            del kwargs
        assert self is not None
        return ALPHANUMERIC.__visit_name__

    def visit_SMALLINT(self, type_, **kw):
        return super(ParadoxTypeCompiler, self).visit_SMALLINT(type_, **kw)

    def visit_SmallInteger(self, type_, **kw):
        return super(ParadoxTypeCompiler, self).visit_SMALLINT(type_, **kw)

    def visit_SHORT(self, *args, **kwargs):
        if args:
            del args
        if kwargs:
            del kwargs
        assert self is not None
        return SHORT.__visit_name__

    def visit_Date(self, type_, **kw):
        return super(ParadoxTypeCompiler, self).visit_DATE(type_, **kw)

    def visit_DATE(self, type_, **kw):
        return super(ParadoxTypeCompiler, self).visit_DATE(type_, **kw)

    def visit_PDOXDATE(self, *args, **kwargs):
        if args:
            del args
        if kwargs:
            del kwargs
        assert self is not None
        return PDOXDATE.__visit_name__


class ParadoxExecutionContext(default.DefaultExecutionContext):
    """ Execution Context
    """

    # NOTE: The Paradox ODBC driver has an esoteric bug that causes an
    #       error to be thrown any time you attempt to execute an insert
    #       query that references column names containing invalid characters
    #       such as `#`. This is due to the fact that column names such as
    #       "Account #" are totally valid in Paradox, but invalid according to
    #       the ODBC driver's underlying engine.
    #
    #       This bug can be side-stepped by simply omitting any column whose
    #       name contains an invalid character, along with its intended value,
    #       executing the "sanitized" insert query, and then immediately executing
    #       an update statement for the reserved columns and values using the
    #       newly inserted values as in the WHERE clause

    # TODO: Figure out a way to implement the workaround detailed above

    def handle_dbapi_exception(self, e):
        print(f"{type(e)} -> {e}")
        super().handle_dbapi_exception(e)

    def get_lastrowid(self):
        try:
            super(ParadoxExecutionContext, self).get_lastrowid()
        except AttributeError:
            return None

    def create_server_side_cursor(self):
        super(ParadoxExecutionContext, self).create_server_side_cursor()

    def result(self):
        return super(ParadoxExecutionContext, self).result()

    def get_rowcount(self):
        return super(ParadoxExecutionContext, self).get_rowcount()


# noinspection PyArgumentList
class ParadoxDialect(default.DefaultDialect):
    """ Dialect
    """

    name = "paradox"

    statement_compiler = ParadoxSQLCompiler
    type_compiler = ParadoxTypeCompiler
    execution_ctx_cls = ParadoxExecutionContext

    postfetch_lastrowid = False

    __temp_tables = None
    __vetted_tables = None

    # The Microsoft Paradox ODBC driver *only* supports these ODBC API Functions:
    #   SQLColAttributes
    #   SQLColumns
    #   SQLConfigDataSource
    #   SQLDriverConnect
    #   SQLGetInfo
    #   SQLGetTypeInfo
    #   SQLSetConnectOption
    #   SQLStatistics
    #   SQLTables
    #   SQLTransact

    def do_execute(self, cursor, statement, parameters, context=None, *args, **kwargs):

        # TODO: Revisit this, figure out why the parameters aren't being compiled correctly

        params = parameters
        if all((isinstance(param, str) for param in parameters)):
            for param in parameters:
                statement = "".join(
                    (
                        statement[: statement.find("?")],
                        "'",
                        param,
                        "'",
                        statement[statement.find("?") + 1:],
                    )
                )
            params = tuple()

        super(ParadoxDialect, self).do_execute(cursor, statement, params, context)

    @staticmethod
    def _check_unicode_returns(*args, **kwargs):
        # The driver should pretty much always be running on a modern
        # Windows system, so it's more or less safe to assume we'll
        # always get a unicode string back for string values
        return True

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        pyodbc_connection = connection.engine.raw_connection()
        pyodbc_cursor = pyodbc_connection.cursor()
        result = list()
        for row in pyodbc_cursor.columns(table=table_name):
            # Try to match the underlying data_type with our implemented
            # Paradox Types, falling back to AlphaNumeric id we can't
            class_ = ischema_names.get(row.sql_data_type, AlphaNumeric)
            type_ = class_()
            if class_ in [LongVarChar, Binary, AlphaNumeric]:
                type_.length = row.column_size
            elif class_ is Number:
                type_.precision = row.column_size
                type_.scale = row.num_prec_radix
            result.append(
                {
                    "name": row.column_name,
                    "type": type_,
                    "nullable": bool(row.nullable),
                    "default": None,  # Paradox doesn't really provide a "default"
                    "autoincrement": False,  # Haven't encountered any column what would autoincrement yet
                }
            )
        return result

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, *args, **kwargs):
        """ Return information about the primary key constraint on
            table_name`.

            Given a :class:`_engine.Connection`, a string
            `table_name`, and an optional string `schema`, return primary
            key information as a dictionary with these keys:

            constrained_columns
              a list of column names that make up the primary key

            name
              optional name of the primary key constraint.
        """
        # The Microsoft Paradox ODBC driver doesn't really support any get_info style
        # commands for reflecting data about primary keys. However, due to the way
        # the Paradox tables themselves internally handle indexing, we can more or less
        # safely assume that any columns involved in a unique-together index will function
        # as a compound primary key

        uniques = self.get_unique_constraints(connection=connection, table_name=table_name, *args, **kwargs)

        if len(uniques) == 1:
            return {"name": uniques[0].get("name", None), "constrained_columns": uniques[0].get("column_names", list())}
        elif len(uniques) > 1:
            best_pk = max(uniques, key=lambda pk: len(pk.get("column_names", list())))
            return {"name": best_pk.get("name", None), "constrained_columns": best_pk.get("column_names", list())}
        else:
            return {"name": None, "constrained_columns": list()}

    def get_primary_keys(self, *args, **kwargs):
        """ Deprecated

            Returns information about the primary keys on a table
        """
        return self.get_pk_constraint(*args, **kwargs)

    @staticmethod
    def get_foreign_keys(*args, **kwargs):
        # Paradox absolutely *does not* support foreign keys
        return list()

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        pyodbc_cursor = connection.engine.raw_connection().cursor()
        table_names = list(
            set(
                (
                    x.table_name
                    for x in pyodbc_cursor.tables(tableType="TABLE").fetchall()
                )
            )
        )

        if self.__temp_tables is None:
            self.__temp_tables = set()
        if self.__vetted_tables is None:
            self.__vetted_tables = set()

        for table in table_names:
            try:
                pyodbc_cursor.execute(
                    " ".join(("SELECT", "*", "FROM", table))
                ).fetchone()
                if not match(r"^TEMP(\S)*$", table):
                    self.__vetted_tables.add(table)
                else:
                    self.__temp_tables.add(table)
            except (
                Error,
                DatabaseError,
            ):
                pass
        return list(self.__vetted_tables)

    @reflection.cache
    def get_temp_table_names(self, connection, schema=None, **kw):
        if self.__temp_tables is None:
            self.get_table_names()
        return self.__temp_tables

    @staticmethod
    def get_view_names(*args, **kwargs):
        # Paradox doesn't supply View functionality
        return []

    @staticmethod
    def get_temp_view_names(*args, **kwargs):
        # Paradox doesn't supply View functionality
        return []

    @staticmethod
    def get_view_definition(*args, **kwargs):
        # Paradox doesn't supply View functionality
        return {}

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

        indexes = {}

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
    def get_unique_constraints(self, connection, table_name, *args, **kwargs):
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
        indexes = self.get_indexes(connection=connection, table_name=table_name, *args, **kwargs)

        return list(filter(lambda index: index.get("unique", False) is True, indexes))

    @staticmethod
    def get_check_constraints(*args, **kwargs):
        # Ha ha, nope
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
        table_type = kwargs.get("tableType", False)
        pyodbc_cursor = connection.engine.raw_connection().cursor()

        table_data = pyodbc_cursor.tables(table="LOG", catalog=catalog, schema=schema, tableType=table_type).fetchone()
        comments = getattr(table_data, "remarks", None)

        return {"text": comments if comments is not None else ""}

    @reflection.cache
    def has_table(self, connection, table_name, schema=None, **kw):
        """Check the existence of a particular table in the database.

        Given a :class:`_engine.Connection` object and a string
        `table_name`, return True if the given table (possibly within
        the specified `schema`) exists in the database, False
        otherwise.
        """
        if self.__vetted_tables is None:
            self.get_table_names(connection=connection, schema=schema, **kw)

        return str(table_name) in self.__vetted_tables

    @staticmethod
    def has_sequence(*args, **kwargs):
        # Paradox doesn't support sequences, so it will never have
        # a queried sequence
        return False

    ###
    # All methods below here are effective unimplemented
    ###

    def _get_server_version_info(self, connection, **kwargs):

        return super(ParadoxDialect, self)._get_server_version_info(connection)

    def _get_default_schema_name(self, connection):

        return super(ParadoxDialect, self)._get_default_schema_name(connection)

    def do_begin_twophase(self, connection, xid):

        return super(ParadoxDialect, self).do_begin_twophase(connection, xid)

    def do_prepare_twophase(self, connection, xid):

        return super(ParadoxDialect, self).do_prepare_twophase(connection, xid)

    def do_rollback_twophase(self, connection, xid, is_prepared=True, recover=False):

        return super(ParadoxDialect, self).do_rollback_twophase(
            connection, xid, is_prepared, recover
        )

    def do_commit_twophase(self, connection, xid, is_prepared=True, recover=False):

        return super(ParadoxDialect, self).do_commit_twophase(
            connection, xid, is_prepared, recover
        )

    def do_recover_twophase(self, connection):

        return super(ParadoxDialect, self).do_recover_twophase(connection)

    def set_isolation_level(self, dbapi_conn, level):

        return super(ParadoxDialect, self).set_isolation_level(dbapi_conn, level)

    def get_isolation_level(self, dbapi_conn):

        return super(ParadoxDialect, self).get_isolation_level(dbapi_conn)
