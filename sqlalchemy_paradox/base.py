""" Dialect Base
"""

from sqlalchemy.engine import default, reflection
from sqlalchemy.sql import compiler
from sqlalchemy import exc, util, types as sqla_types
from sqlalchemy.sql import elements, functions
from pyodbc import DatabaseError, Error
from itertools import chain
from moz_sql_parser import parse as parse_sql
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

    def __init__(self, precision=None, scale=None, asdecimal=True, **kw):
        self.unsigned = kw.get("unsigned", False)
        self.zerofill = kw.get("zerofill", False)
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
        super(DOUBLE, self).__init__(precision=precision, asdecimal=asdecimal, **kw)
        self.scale = scale

    def __repr__(self):
        return util.generic_repr(self, to_inspect=[sqla_types.Float])


class BINARY(sqla_types.BLOB):
    """ Long VarBinary
    """

    # Size 1073741823, NULLABLE, NOT SEARCHABLE
    __visit_name__ = "BLOB"
    __sql_data_type__ = -4


class LONGVARCHAR(sqla_types.Text):
    """ Long VarChar
    """

    # Size 1073741824, NULLABLE, NOT SEARCHABLE
    __visit_name__ = "TEXT"
    __sql_data_type__ = -1


class ALPHANUMERIC(sqla_types.VARCHAR):
    """ Alphanumeric / Generic VarChar
    """

    __visit_name__ = "VARCHAR"
    __sql_data_type__ = 12


class SHORT(sqla_types.SmallInteger):
    """ SmallInt
    """

    # Size 5, NULLABLE, SEARCHABLE
    __visit_name__ = "SmallInteger"
    __sql_data_type__ = 5


class PDOXDATE(sqla_types.Date):
    """ Paradox Date
    """

    # Size 10, NULLABLE, SEARCHABLE
    __visit_name__ = "Date"
    __sql_data_type__ = 9


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
                name = name + "%(expr)s"

            ret_list = [name]
            for tok in func.packagenames:
                if any(
                        (
                                getattr(
                                    self.preparer,
                                    "_requires_quotes_illegal_chars",
                                    bool,
                                )(tok),
                                isinstance(name, elements.quoted_name),
                        )
                ):
                    ret_list.append(self.preparer.quote(tok))
                else:
                    ret_list.append(tok)

            ret_string = ".".join(ret_list) % {"expr": self.function_argspec(func, **kwargs)}
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

    __paradox_insert_workaround = dict()

    def handle_dbapi_exception(self, e):
        print(f"{type(e)} -> {e}")
        super().handle_dbapi_exception(e)

    def get_lastrowid(self):
        try:
            super(ParadoxExecutionContext, self).get_lastrowid()
        except AttributeError:
            return None

    # def pre_exec(self):
    #     if self.isinsert:
    #         # need to replace self.statement and self.parameters
    #         parsed_statement = getattr(self, "statement", None)
    #         self.statement = """INSERT INTO "NAMED" ("First Name", "Last Name") VALUES ('Johnny', 'The Fuck Stick')"""
    #         self.parameters = (tuple([]), )
    #         self.parameters = (tuple([]), )
    #     super(ParadoxExecutionContext, self).pre_exec()

    # def get_lastrowid(self):
    #     # The Paradox driver doesn't properly handle insert statements when
    #     # column names containing the `#` character are passed as prt of the query.
    #     # Instead, we'll have to emit a second query to populate the newly created
    #     # row with any values that were supposed to be inserted into columns with
    #     # `#`-containing names
    #     #
    #     # This is probably not the *best* idea, because there's no *real* guarantee that we're
    #     # getting or setting the right information, but hey what're ya gonna do?
    #     # Hopefully it won't cause issues
    #     if all([not self.executemany, self.isinsert]):
    #         table_name = list((str(self.compiled.string).replace("INSERT INTO ", "")).split())[0]
    #         post_fetch_query = f"""SELECT MAX("Acct #") + 1 FROM {table_name}"""
    #         ids = self.cursor.execute(post_fetch_query).fetchall()
    #         # This max call *might* help return the correct ID when multiple IDs are found
    #         ret_val = max(chain(ids))
    #         # print(f"Got:\t{ids}\nReturning:\t{ret_val}")
    #         return ret_val

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

    @staticmethod
    def get_primary_keys(*args, **kwargs):
        # Paradox may or may not support primary keys, doesn't really matter though
        return []

    @staticmethod
    def get_foreign_keys(*args, **kwargs):
        # Paradox absolutely *does not* support foreign keys
        return []

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

        vetted_table_names = list()
        if self.__temp_tables is None:
            self.__temp_tables = list()

        for table in table_names:
            try:
                pyodbc_cursor.execute(
                    " ".join(("SELECT", "*", "FROM", table))
                ).fetchone()
                if not match(r"^TEMP(\S)*$", table):
                    vetted_table_names.append(table)
                else:
                    self.__temp_tables.append(table)
            except (
                Error,
                DatabaseError,
            ):
                pass
        return vetted_table_names

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

    def get_indexes(self, connection, table_name, schema=None, **kw):
        # Currently unsure of weather or not Paradox supports indexes
        # TODO: Actually implement this
        return []

    def get_unique_constraints(self, connection, table_name, schema=None, **kw):

        return super(ParadoxDialect, self).get_unique_constraints(
            connection, table_name, schema, **kw
        )

    @staticmethod
    def get_check_constraints(*args, **kwargs):
        # Ha ha, nope
        return []

    def get_table_comment(self, connection, table_name, schema=None, **kw):

        return super(ParadoxDialect, self).get_table_comment(
            connection, table_name, schema, **kw
        )

    def has_table(self, connection, table_name, schema=None, **kw):

        return super(ParadoxDialect, self).has_table(connection, table_name, schema)

    @staticmethod
    def has_sequence(*args, **kwargs):
        # Paradox doesn't support sequences, so it will never have
        # a queried sequence
        return False

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
