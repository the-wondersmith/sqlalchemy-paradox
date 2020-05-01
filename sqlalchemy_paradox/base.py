from sqlalchemy.engine import default, reflection
from sqlalchemy.sql import compiler
from sqlalchemy import exc, util, types as sqla_types
from re import match


# Paradox Types:
# These are relatively simple, as the underlying ODBC driver only *really* supports
# something like six different data types rather than the spectrum of modern SQL types

class DOUBLE(sqla_types.Float):
    """ Paradox DOUBLE type

        Shamelessly adapted from the MySQL DOUBLE type

        double precision / Size 15, NULLABLE, SEARCHABLE
    """

    __visit_name__ = "Float"
    __sql_data_type__ = 8

    def __init__(self, precision=None, scale=None, asdecimal=True, **kw):
        self.unsigned = kw.get("unsigned", False)
        self.zerofill = kw.get("zerofill", False)
        if isinstance(self, DOUBLE) and (
            (precision is None and scale is not None)
            or (precision is not None and scale is None)
        ):
            raise exc.ArgumentError(
                "You must specify both precision and scale or omit both altogether."
            )
        super(DOUBLE, self).__init__(precision=precision, asdecimal=asdecimal, **kw)
        self.scale = scale

    def __repr__(self):
        return util.generic_repr(self, to_inspect=[sqla_types.Float])

class BINARY(sqla_types.BLOB):
    # long varbinary / Size 1073741823, NULLABLE, NOT SEARCHABLE
    __visit_name__ = "BLOB"
    __sql_data_type__ = -4

class LONGVARCHAR(sqla_types.Text):
    # long varchar  / Size 1073741824, NULLABLE, NOT SEARCHABLE
    __visit_name__ = "TEXT"
    __sql_data_type__ = -1

class ALPHANUMERIC(sqla_types.VARCHAR):
    __visit_name__ = "VARCHAR"
    __sql_data_type__ = 12

class SHORT(sqla_types.SmallInteger):
    # smallint / Size 5, NULLABLE, SEARCHABLE
    __visit_name__ = "SmallInteger"
    __sql_data_type__ = 5

class PDOXDATE(sqla_types.Date):
    # date / Size 10, NULLABLE, SEARCHABLE
    __visit_name__ = "Date"
    __sql_data_type__ = 9


Binary = BINARY
LongVarChar = LONGVARCHAR
AlphaNumeric = ALPHANUMERIC
Number = DOUBLE
Short = SHORT
PdoxDate = PDOXDATE


"""
Map integers returned by the "sql_data_type" column of pyodbc's Cursor.columns method to our dialect types.

These names are what you would retrieve from INFORMATION_SCHEMA.COLUMNS.DATA_TYPE if Paradox
supported those types of system views.
"""
ischema_names = {
    -4: Binary,
    -1: LongVarChar,
    5: Short,
    8: Number,
    9: PdoxDate,
    12: AlphaNumeric,
}

class ParadoxTypeCompiler(compiler.GenericTypeCompiler):
    # The underlying DataFlex driver *really* doesn't support much
    # in the way of datatypes, so this may be entirely perfunctory
    #
    # It's being done anyway to keep this library as in-line with
    # sqlalchemy-access as possible, as this library is a shameless
    # ripoff of sqlalchemy-access

    def visit_Float(self, type_, **kw):
        return super(ParadoxTypeCompiler, self).visit_FLOAT(type_, **kw)

    def visit_FLOAT(self, type_, **kw):
        return super(ParadoxTypeCompiler, self).visit_FLOAT(type_, **kw)

    def visit_DOUBLE(self, type_, **kw):
        return DOUBLE.__visit_name__

    def visit_BINARY(self, type_, **kw):
        return BINARY.__visit_name__

    def visit_LONGVARCHAR(self, type_, **kw):
        return LONGVARCHAR.__visit_name__

    def visit_ALPHANUMERIC(self, type_, **kw):
        return ALPHANUMERIC.__visit_name__

    def visit_SMALLINT(self, type_, **kw):
        return super(ParadoxTypeCompiler, self).visit_SMALLINT(type_, **kw)

    def visit_SmallInteger(self, type_, **kw):
        return super(ParadoxTypeCompiler, self).visit_SMALLINT(type_, **kw)

    def visit_SHORT(self, type_, **kw):
        return SHORT.__visit_name__

    def visit_Date(self, type_, **kw):
        return super(ParadoxTypeCompiler, self).visit_DATE(type_, **kw)

    def visit_DATE(self, type_, **kw):
        return super(ParadoxTypeCompiler, self).visit_DATE(type_, **kw)

    def visit_PDOXDATE(self, type_, **kw):
        return PDOXDATE.__visit_name__


class ParadoxExecutionContext(default.DefaultExecutionContext):
    def create_server_side_cursor(self):
        super(ParadoxExecutionContext, self).create_server_side_cursor()

    def result(self):
        return super(ParadoxExecutionContext, self).result()

    def get_rowcount(self):
        return super(ParadoxExecutionContext, self).get_rowcount()


class ParadoxDialect(default.DefaultDialect):

    name = "paradox"

    type_compiler = ParadoxTypeCompiler
    execution_ctx_cls = ParadoxExecutionContext

    __temp_tables = None

    def _check_unicode_returns(self, connection, additional_tests=None):
        # The driver should pretty much always be running on a modern
        # Windows system, so it's more or less safe to assume we'll
        # always get a unicode string back for string values
        return True

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        pyodbc_cnxn = connection.engine.raw_connection()
        pyodbc_crsr = pyodbc_cnxn.cursor()
        result = list()
        for row in pyodbc_crsr.columns(table=table_name):
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

    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        # Paradox may or may not support primary keys, doesn't really matter though
        return []

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        # Paradox absolutely *does not* support foreign keys
        return []

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        pyodbc_crsr = connection.engine.raw_connection().cursor()
        table_names = list(
            set(
                [x.table_name for x in pyodbc_crsr.tables(tableType="TABLE").fetchall()]
            )
        )

        vetted_table_names = list()
        if self.__temp_tables is None:
            self.__temp_tables = list()

        for table in table_names:
            try:
                pyodbc_crsr.execute(f"SELECT * FROM {table}").fetchone()
                if not match(r"^TEMP(\S)*$", table):
                    vetted_table_names.append(table)
                else:
                    self.__temp_tables.append(table)
            except Exception:
                pass
        return vetted_table_names

    @reflection.cache
    def get_temp_table_names(self, connection, schema=None, **kw):
        if self.__temp_tables is None:
            self.get_table_names()
        return self.__temp_tables

    def get_view_names(self, connection, schema=None, **kw):
        # Paradox doesn't supply View functionality
        return []

    def get_temp_view_names(self, connection, schema=None, **kw):
        # Paradox doesn't supply View functionality
        return []

    def get_view_definition(self, connection, view_name, schema=None, **kw):
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

    def get_check_constraints(self, connection, table_name, schema=None, **kw):
        # Ha ha, nope
        return []

    def get_table_comment(self, connection, table_name, schema=None, **kw):

        return super(ParadoxDialect, self).get_table_comment(
            connection, table_name, schema, **kw
        )

    def has_table(self, connection, table_name, schema=None, **kw):

        return super(ParadoxDialect, self).has_table(connection, table_name, schema)

    def has_sequence(self, connection, sequence_name, schema=None, **kw):
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
