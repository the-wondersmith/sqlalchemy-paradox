"""Testing requirements for SQLAlchemy-Paradox."""
# coding=utf-8

import sys

from sqlalchemy import util
from sqlalchemy.testing.requirements import SuiteRequirements

from sqlalchemy.testing import exclusions


class Requirements(SuiteRequirements):
    """SQLAlchemy test suite requirements for Paradox."""

    @property
    def create_table(self):
        """target platform can emit basic CreateTable DDL."""

        return exclusions.open()

    @property
    def drop_table(self):
        """target platform can emit basic DropTable DDL."""

        return exclusions.open()

    @property
    def foreign_keys(self):
        """Target database must support foreign keys."""

        return exclusions.closed()

    @property
    def on_update_cascade(self):
        """target database must support ON UPDATE..CASCADE behavior in
        foreign keys."""

        return exclusions.closed()

    @property
    def non_updating_cascade(self):
        """target database must *not* support ON UPDATE..CASCADE behavior in
        foreign keys."""
        return exclusions.closed()

    @property
    def deferrable_fks(self):
        """Insert DocString Here."""
        return exclusions.closed()

    @property
    def on_update_or_deferrable_fks(self):
        """Insert DocString Here."""
        # TODO: exclusions should be composable,
        # somehow only_if([x, y]) isn't working here, negation/conjunctions
        # getting confused.
        return exclusions.only_if(
            lambda: self.on_update_cascade.enabled or self.deferrable_fks.enabled
        )

    @property
    def self_referential_foreign_keys(self):
        """Target database must support self-referential foreign keys."""

        return exclusions.closed()

    @property
    def foreign_key_ddl(self):
        """Target database must support the DDL phrases for FOREIGN KEY."""

        return exclusions.closed()

    @property
    def named_constraints(self):
        """target database must support names for constraints."""

        return exclusions.open()

    @property
    def subqueries(self):
        """Target database must support subqueries."""

        return exclusions.closed()

    @property
    def offset(self):
        """target database can render OFFSET, or an equivalent, in a
        SELECT.
        """

        return exclusions.closed()

    @property
    def bound_limit_offset(self):
        """target database can render LIMIT and/or OFFSET using a bound
        parameter
        """

        return exclusions.closed()

    @property
    def sql_expression_limit_offset(self):
        """target database can render LIMIT and/or OFFSET with a complete
        SQL expression, such as one that uses the addition operator.
        parameter
        """

        return exclusions.closed()

    @property
    def parens_in_union_contained_select_w_limit_offset(self):
        """Target database must support parenthesized SELECT in UNION
        when LIMIT/OFFSET is specifically present.

        E.g. (SELECT ...) UNION (SELECT ..)

        This is known to fail on SQLite.

        """
        return exclusions.closed()

    @property
    def parens_in_union_contained_select_wo_limit_offset(self):
        """Target database must support parenthesized SELECT in UNION
        when OFFSET/LIMIT is specifically not present.

        E.g. (SELECT ... LIMIT ..) UNION (SELECT .. OFFSET ..)

        This is known to fail on SQLite.  It also fails on Oracle
        because without LIMIT/OFFSET, there is currently no step that
        creates an additional subquery.

        """
        return exclusions.open()

    @property
    def boolean_col_expressions(self):
        """Target database must support boolean expressions as columns"""

        return exclusions.closed()

    @property
    def nullable_booleans(self):
        """Target database allows boolean columns to store NULL."""
        return exclusions.open()

    @property
    def nullsordering(self):
        """Target backends that support nulls ordering."""

        return exclusions.closed()

    @property
    def standalone_binds(self):
        """target database/driver supports bound parameters as column expressions
        without being in the context of a typed column.

        """
        return exclusions.closed()

    @property
    def standalone_null_binds_whereclause(self):
        """target database/driver supports bound parameters with NULL in the
        WHERE clause, in situations where it has to be typed.

        """
        return exclusions.open()

    @property
    def intersect(self):
        """Target database must support INTERSECT or equivalent."""
        return exclusions.closed()

    @property
    def except_(self):
        """Target database must support EXCEPT or equivalent (i.e. MINUS)."""
        return exclusions.closed()

    @property
    def window_functions(self):
        """Target database must support window functions."""
        return exclusions.closed()

    @property
    def ctes(self):
        """Target database supports CTEs"""

        return exclusions.closed()

    @property
    def ctes_with_update_delete(self):
        """target database supports CTES that ride on top of a normal UPDATE
        or DELETE statement which refers to the CTE in a correlated subquery.

        """

        return exclusions.closed()

    @property
    def ctes_on_dml(self):
        """target database supports CTES which consist of INSERT, UPDATE
        or DELETE *within* the CTE, e.g. WITH x AS (UPDATE....)"""

        return exclusions.closed()

    @property
    def autoincrement_insert(self):
        """target platform generates new surrogate integer primary key values
        when insert() is executed, excluding the pk column."""

        return exclusions.open()

    @property
    def fetch_rows_post_commit(self):
        """target platform will allow cursor.fetchone() to proceed after a
        COMMIT.

        Typically this refers to an INSERT statement with RETURNING which
        is invoked within "autocommit".   If the row can be returned
        after the autocommit, then this rule can be open.

        """

        return exclusions.open()

    @property
    def group_by_complex_expression(self):
        """target platform supports SQL expressions in GROUP BY

        e.g.

        SELECT x + y AS somelabel FROM table GROUP BY x + y

        """

        return exclusions.closed()

    @property
    def sane_rowcount(self):
        """Insert DocString Here."""
        return exclusions.skip_if(
            lambda config: not config.db.dialect.supports_sane_rowcount,
            "driver doesn't support 'sane' rowcount",
        )

    @property
    def sane_multi_rowcount(self):
        """Insert DocString Here."""
        return exclusions.fails_if(
            lambda config: not config.db.dialect.supports_sane_multi_rowcount,
            "driver %(driver)s %(doesnt_support)s 'sane' multi row count",
        )

    @property
    def sane_rowcount_w_returning(self):
        """Insert DocString Here."""
        return exclusions.fails_if(
            lambda config: not config.db.dialect.supports_sane_rowcount_returning,
            "driver doesn't support 'sane' rowcount when returning is on",
        )

    @property
    def empty_inserts(self):
        """target platform supports INSERT with no values, i.e.
        INSERT DEFAULT VALUES or equivalent."""

        return exclusions.closed()

    @property
    def insert_from_select(self):
        """target platform supports INSERT from a SELECT."""

        return exclusions.open()

    @property
    def returning(self):
        """target platform supports RETURNING."""

        return exclusions.only_if(
            lambda config: config.db.dialect.implicit_returning,
            "%(database)s %(does_support)s 'returning'",
        )

    @property
    def tuple_in(self):
        """Target platform supports the syntax
        "(x, y) IN ((x1, y1), (x2, y2), ...)"
        """

        return exclusions.closed()

    @property
    def tuple_in_w_empty(self):
        """Target platform tuple IN w/ empty set"""
        return self.tuple_in

    @property
    def duplicate_names_in_cursor_description(self):
        """target platform supports a SELECT statement that has
        the same name repeated more than once in the columns list."""

        return exclusions.closed()

    @property
    def denormalized_names(self):
        """Target database must have 'denormalized', i.e.
        UPPERCASE as case insensitive names."""

        return exclusions.skip_if(
            lambda config: not config.db.dialect.requires_name_normalize,
            "Backend does not require denormalized names.",
        )

    @property
    def multivalues_inserts(self):
        """target database must support multiple VALUES clauses in an
        INSERT statement."""

        return exclusions.closed()

    @property
    def implements_get_lastrowid(self):
        """target dialect implements the executioncontext.get_lastrowid()
        method without reliance on RETURNING.

        """
        return exclusions.open()

    @property
    def emulated_lastrowid(self):
        """target dialect retrieves cursor.lastrowid, or fetches
        from a database-side function after an insert() construct executes,
        within the get_lastrowid() method.

        Only dialects that "pre-execute", or need RETURNING to get last
        inserted id, would return closed/fail/skip for this.

        """
        return exclusions.closed()

    @property
    def dbapi_lastrowid(self):
        """target platform includes a 'lastrowid' accessor on the DBAPI
        cursor object.

        """
        return exclusions.closed()

    @property
    def views(self):
        """Target database must support VIEWs."""

        return exclusions.closed()

    @property
    def schemas(self):
        """Target database must support external schemas, and have one
        named 'test_schema'."""

        return exclusions.closed()

    @property
    def cross_schema_fk_reflection(self):
        """target system must support reflection of inter-schema foreign
        keys"""
        return exclusions.closed()

    @property
    def implicit_default_schema(self):
        """target system has a strong concept of 'default' schema that can
        be referred to implicitly.

        basically, PostgreSQL.

        """
        return exclusions.closed()

    @property
    def server_side_cursors(self):
        """Target dialect must support server side cursors."""

        return exclusions.only_if(
            [lambda config: config.db.dialect.supports_server_side_cursors],
            "no server side cursors support",
        )

    @property
    def sequences(self):
        """Target database must support SEQUENCEs."""

        return exclusions.closed()

    @property
    def sequences_optional(self):
        """Target database supports sequences, but also optionally
        as a means of generating new PK values."""

        return exclusions.closed()

    @property
    def supports_lastrowid(self):
        """target database / driver supports cursor.lastrowid as a means
        of retrieving the last inserted primary key value.

        note that if the target DB supports sequences also, this is still
        assumed to work.  This is a new use case brought on by MariaDB 10.3.

        """
        return exclusions.closed()

    @property
    def no_lastrowid_support(self):
        """the opposite of supports_lastrowid"""
        return exclusions.NotPredicate(self.supports_lastrowid)

    @property
    def reflects_pk_names(self):
        """Insert DocString Here."""
        return exclusions.open()

    @property
    def table_reflection(self):
        """Insert DocString Here."""
        return exclusions.open()

    @property
    def comment_reflection(self):
        """Insert DocString Here."""
        return exclusions.closed()

    @property
    def view_column_reflection(self):
        """target database must support retrieval of the columns in a view,
        similarly to how a table is inspected.

        This does not include the full CREATE VIEW definition.

        """
        return self.views

    @property
    def view_reflection(self):
        """target database must support inspection of the full CREATE VIEW
        definition."""
        return self.views

    @property
    def schema_reflection(self):
        """Insert DocString Here."""
        return self.schemas

    @property
    def primary_key_constraint_reflection(self):
        """Insert DocString Here."""
        return exclusions.open()

    @property
    def foreign_key_constraint_reflection(self):
        """Insert DocString Here."""
        return exclusions.closed()

    @property
    def foreign_key_constraint_option_reflection_ondelete(self):
        """Insert DocString Here."""
        return exclusions.closed()

    @property
    def foreign_key_constraint_option_reflection_onupdate(self):
        """Insert DocString Here."""
        return exclusions.closed()

    @property
    def temp_table_reflection(self):
        """Insert DocString Here."""
        return exclusions.closed()

    @property
    def temp_table_names(self):
        """target dialect supports listing of temporary table names"""
        return exclusions.closed()

    @property
    def temporary_tables(self):
        """target database supports temporary tables"""
        return exclusions.closed()

    @property
    def temporary_views(self):
        """target database supports temporary views"""
        return exclusions.closed()

    @property
    def index_reflection(self):
        """Insert DocString Here."""
        return exclusions.open()

    @property
    def indexes_with_ascdesc(self):
        """target database supports CREATE INDEX with per-column ASC/DESC."""
        return exclusions.closed()

    @property
    def indexes_with_expressions(self):
        """target database supports CREATE INDEX against SQL expressions."""
        return exclusions.closed()

    @property
    def unique_constraint_reflection(self):
        """target dialect supports reflection of unique constraints"""
        return exclusions.open()

    @property
    def check_constraint_reflection(self):
        """target dialect supports reflection of check constraints"""
        return exclusions.closed()

    @property
    def duplicate_key_raises_integrity_error(self):
        """target dialect raises IntegrityError when reporting an INSERT
        with a primary key violation.  (hint: it should)

        """
        return exclusions.open()

    @property
    def unbounded_varchar(self):
        """Target database must support VARCHAR with no length"""

        return exclusions.open()

    @property
    def unicode_data(self):
        """Target database/dialect must support Python unicode objects with
        non-ASCII characters represented, delivered as bound parameters
        as well as in result rows.

        """
        return exclusions.open()

    @property
    def unicode_ddl(self):
        """Target driver must support some degree of non-ascii symbol
        names.
        """
        return exclusions.closed()

    @property
    def symbol_names_w_double_quote(self):
        """Target driver can create tables with a name like 'some " table'"""
        return exclusions.open()

    @property
    def datetime_literals(self):
        """target dialect supports rendering of a date, time, or datetime as a
        literal string, e.g. via the TypeEngine.literal_processor() method.

        """

        return exclusions.closed()

    @property
    def datetime(self):
        """target dialect supports representation of Python
        datetime.datetime() objects."""

        return exclusions.open()

    @property
    def datetime_microseconds(self):
        """target dialect supports representation of Python
        datetime.datetime() with microsecond objects."""

        return exclusions.open()

    @property
    def timestamp_microseconds(self):
        """target dialect supports representation of Python
        datetime.datetime() with microsecond objects but only
        if TIMESTAMP is used."""
        return exclusions.closed()

    @property
    def datetime_historic(self):
        """target dialect supports representation of Python
        datetime.datetime() objects with historic (pre 1970) values."""

        return exclusions.closed()

    @property
    def date(self):
        """target dialect supports representation of Python
        datetime.date() objects."""

        return exclusions.open()

    @property
    def date_coerces_from_datetime(self):
        """target dialect accepts a datetime object as the target
        of a date column."""

        return exclusions.open()

    @property
    def date_historic(self):
        """target dialect supports representation of Python
        datetime.datetime() objects with historic (pre 1970) values."""

        return exclusions.closed()

    @property
    def time(self):
        """target dialect supports representation of Python
        datetime.time() objects."""

        return exclusions.open()

    @property
    def time_microseconds(self):
        """target dialect supports representation of Python
        datetime.time() with microsecond objects."""

        return exclusions.closed()

    @property
    def binary_comparisons(self):
        """target database/driver can allow BLOB/BINARY fields to be compared
        against a bound parameter value.
        """

        return exclusions.open()

    @property
    def binary_literals(self):
        """target backend supports simple binary literals, e.g. an
        expression like::

            SELECT CAST('foo' AS BINARY)

        Where ``BINARY`` is the type emitted from :class:`.LargeBinary`,
        e.g. it could be ``BLOB`` or similar.

        Basically fails on Oracle.

        """

        return exclusions.closed()

    @property
    def autocommit(self):
        """target dialect supports 'AUTOCOMMIT' as an isolation_level"""
        return exclusions.closed()

    @property
    def isolation_level(self):
        """target dialect supports general isolation level settings.

        Note that this requirement, when enabled, also requires that
        the get_isolation_levels() method be implemented.

        """
        return exclusions.closed()

    def get_isolation_levels(self, config):
        """Return a structure of supported isolation levels for the current
        testing dialect.

        The structure indicates to the testing suite what the expected
        "default" isolation should be, as well as the other values that
        are accepted.  The dictionary has two keys, "default" and "supported".
        The "supported" key refers to a list of all supported levels and
        it should include AUTOCOMMIT if the dialect supports it.

        If the :meth:`.DefaultRequirements.isolation_level` requirement is
        not open, then this method has no return value.

        E.g.::

            > testing.requirements.get_isolation_levels()
            {
                "default": "READ_COMMITED",
                "supported": [
                    "SERIALIZABLE", "READ UNCOMMITTED",
                    "READ COMMITTED", "REPEATABLE READ",
                    "AUTOCOMMIT"
                ]
            }
        """

    @property
    def json_type(self):
        """target platform implements a native JSON type."""

        return exclusions.closed()

    @property
    def json_array_indexes(self):
        """target platform supports numeric array indexes
        within a JSON structure"""

        return self.json_type

    @property
    def json_index_supplementary_unicode_element(self):
        """Insert DocString Here."""
        return exclusions.closed()

    @property
    def precision_numerics_general(self):
        """target backend has general support for moderately high-precision
        numerics."""
        return exclusions.closed()

    @property
    def precision_numerics_enotation_small(self):
        """target backend supports Decimal() objects using E notation
        to represent very small values."""
        return exclusions.closed()

    @property
    def precision_numerics_enotation_large(self):
        """target backend supports Decimal() objects using E notation
        to represent very large values."""
        return exclusions.closed()

    @property
    def precision_numerics_many_significant_digits(self):
        """target backend supports values with many digits on both sides,
        such as 319438950232418390.273596, 87673.594069654243

        """
        return exclusions.closed()

    @property
    def implicit_decimal_binds(self):
        """target backend will return a selected Decimal as a Decimal, not
        a string.

        e.g.::

            expr = decimal.Decimal("15.7563")

            value = e.scalar(
                select([literal(expr)])
            )

            assert value == expr

        See :ticket:`4036`

        """

        return exclusions.open()

    @property
    def nested_aggregates(self):
        """target database can select an aggregate from a subquery that's
        also using an aggregate

        """
        return exclusions.closed()

    @property
    def recursive_fk_cascade(self):
        """target database must support ON DELETE CASCADE on a self-referential
        foreign key

        """
        return exclusions.closed()

    @property
    def precision_numerics_retains_significant_digits(self):
        """A precision numeric type will return empty significant digits,
        i.e. a value such as 10.000 will come back in Decimal form with
        the .000 maintained."""

        return exclusions.closed()

    @property
    def precision_generic_float_type(self):
        """target backend will return native floating point numbers with at
        least seven decimal places when using the generic Float type.

        """
        return exclusions.open()

    @property
    def floats_to_four_decimals(self):
        """target backend can return a floating-point number with four
        significant digits (such as 15.7563) accurately
        (i.e. without FP inaccuracies, such as 15.75629997253418).

        """
        return exclusions.open()

    @property
    def fetch_null_from_numeric(self):
        """target backend doesn't crash when you try to select a NUMERIC
        value that has a value of NULL.

        Added to support Pyodbc bug #351.
        """

        return exclusions.open()

    @property
    def text_type(self):
        """Target database must support an unbounded Text() "
        "type such as TEXT or CLOB"""

        return exclusions.open()

    @property
    def empty_strings_varchar(self):
        """target database can persist/return an empty string with a
        varchar.

        """
        return exclusions.open()

    @property
    def empty_strings_text(self):
        """target database can persist/return an empty string with an
        unbounded text."""

        return exclusions.open()

    @property
    def expressions_against_unbounded_text(self):
        """target database supports use of an unbounded textual field in a
        WHERE clause."""

        return exclusions.open()

    @property
    def selectone(self):
        """target driver must support the literal statement 'select 1'"""
        # TODO: Determine the correct value for `selectone`
        #       The Intersolv driver *does* technically support it
        #       as long as there's a FROM clause present in the query
        return exclusions.open()

    @property
    def savepoints(self):
        """Target database must support savepoints."""

        return exclusions.closed()

    @property
    def two_phase_transactions(self):
        """Target database must support two-phase transactions."""

        return exclusions.closed()

    @property
    def update_from(self):
        """Target must support UPDATE..FROM syntax"""
        return exclusions.closed()

    @property
    def delete_from(self):
        """Target must support DELETE FROM..FROM or DELETE..USING syntax"""
        return exclusions.closed()

    @property
    def update_where_target_in_subquery(self):
        """Target must support UPDATE (or DELETE) where the same table is
        present in a subquery in the WHERE clause.

        This is an ANSI-standard syntax that apparently MySQL can't handle,
        such as::

            UPDATE documents SET flag=1 WHERE documents.title IN
                (SELECT max(documents.title) AS title
                    FROM documents GROUP BY documents.user_id
                )

        """
        return exclusions.open()

    @property
    def mod_operator_as_percent_sign(self):
        """target database must use a plain percent '%' as the 'modulus'
        operator."""
        return exclusions.closed()

    @property
    def percent_schema_names(self):
        """target backend supports weird identifiers with percent signs
        in them, e.g. 'some % column'.

        this is a very weird use case but often has problems because of
        DBAPIs that use python formatting.  It's not a critical use
        case either.

        """
        return exclusions.closed()

    @property
    def order_by_col_from_union(self):
        """target database supports ordering by a column from a SELECT
        inside of a UNION

        E.g.  (SELECT id, ...) UNION (SELECT id, ...) ORDER BY id

        """
        return exclusions.open()

    @property
    def order_by_label_with_expression(self):
        """target backend supports ORDER BY a column label within an
        expression.

        Basically this::

            select data as foo from test order by foo || 'bar'

        Lots of databases including PostgreSQL don't support this,
        so this is off by default.

        """
        return exclusions.closed()

    @property
    def order_by_collation(self):
        """Insert DocString Here."""

        def check(config):
            """Insert DocString Here."""
            try:
                self.get_order_by_collation(config)
                return False
            except NotImplementedError:
                return True

        return exclusions.skip_if(check)

    def get_order_by_collation(self, config):
        """Insert DocString Here."""
        raise NotImplementedError()

    @property
    def unicode_connections(self):
        """Target driver must support non-ASCII characters being passed at
        all.
        """
        return exclusions.open()

    @property
    def graceful_disconnects(self):
        """Target driver must raise a DBAPI-level exception, such as
        InterfaceError, when the underlying connection has been closed
        and the execute() method is called.
        """
        return exclusions.open()

    @property
    def independent_connections(self):
        """
        Target must support simultaneous, independent database connections.
        """
        return exclusions.open()

    @property
    def skip_mysql_on_windows(self):
        """Catchall for a large variety of MySQL on Windows failures"""
        return exclusions.open()

    @property
    def ad_hoc_engines(self):
        """Test environment must allow ad-hoc engine/connection creation.

        DBs that scale poorly for many connections, even when closed, i.e.
        Oracle, may use the "--low-connections" option which flags this
        requirement as not present.

        """
        return exclusions.skip_if(lambda config: config.options.low_connections)

    @property
    def timing_intensive(self):
        """Insert DocString Here."""
        return exclusions.requires_tag("timing_intensive")

    @property
    def memory_intensive(self):
        """Insert DocString Here."""
        return exclusions.requires_tag("memory_intensive")

    @property
    def threading_with_mock(self):
        """Mark tests that use threading and mock at the same time - stability
        issues have been observed with coverage + python 3.3

        """
        return exclusions.skip_if(
            lambda config: util.py3k and config.options.has_coverage,
            "Stability issues with coverage + py3k",
        )

    @property
    def python2(self):
        """Insert DocString Here."""
        return exclusions.closed()

    @property
    def python3(self):
        """Insert DocString Here."""
        return exclusions.open()

    @property
    def pep520(self):
        """Insert DocString Here."""
        return self.python36

    @property
    def python36(self):
        """Insert DocString Here."""
        return exclusions.skip_if(
            lambda: sys.version_info < (3, 6),
            "Python version 3.6 or greater is required.",
        )

    @property
    def python37(self):
        """Insert DocString Here."""
        return exclusions.skip_if(
            lambda: sys.version_info < (3, 7),
            "Python version 3.7 or greater is required.",
        )

    @property
    def cpython(self):
        """Insert DocString Here."""
        return exclusions.only_if(lambda: util.cpython, "cPython interpreter needed")

    @property
    def non_broken_pickle(self):
        """Insert DocString Here."""
        from sqlalchemy.util import pickle

        return exclusions.only_if(
            lambda: not util.pypy
            and pickle.__name__ == "cPickle"
            or sys.version_info >= (3, 2),
            "Needs cPickle+cPython or newer Python 3 pickle",
        )

    @property
    def predictable_gc(self):
        """target platform must remove all cycles unconditionally when
        gc.collect() is called, as well as clean out unreferenced subclasses.

        """
        return self.cpython

    @property
    def no_coverage(self):
        """Test should be skipped if coverage is enabled.

        This is to block tests that exercise libraries that seem to be
        sensitive to coverage, such as PostgreSQL notice logging.

        """
        return exclusions.skip_if(
            lambda config: config.options.has_coverage,
            "Issues observed when coverage is enabled",
        )

    def _has_mysql_on_windows(self, config):
        return False

    def _has_mysql_fully_case_sensitive(self, config):
        return False

    @property
    def sqlite(self):
        """Insert DocString Here."""
        return exclusions.closed()

    @property
    def cextensions(self):
        """Insert DocString Here."""
        return exclusions.skip_if(
            lambda: not self._has_cextensions(), "C extensions not installed"
        )

    def _has_sqlite(self):
        return False

    def _has_cextensions(self):
        try:
            from sqlalchemy import cresultproxy, cprocessors  # noqa

            return True
        except ImportError:
            return False

    @property
    def computed_columns(self):
        """Supports computed columns"""
        return exclusions.closed()

    @property
    def computed_columns_stored(self):
        """Supports computed columns with `persisted=True`"""
        return exclusions.closed()

    @property
    def computed_columns_virtual(self):
        """Supports computed columns with `persisted=False`"""
        return exclusions.closed()

    @property
    def computed_columns_default_persisted(self):
        """If the default persistence is virtual or stored when `persisted`
        is omitted"""
        return exclusions.closed()

    @property
    def computed_columns_reflect_persisted(self):
        """If persistence information is returned by the reflection of
        computed columns"""
        return exclusions.closed()

    @property
    def supports_is_distinct_from(self):
        """Supports some form of "x IS [NOT] DISTINCT FROM y" construct.
        Different dialects will implement their own flavour, e.g.,
        sqlite will emit "x IS NOT y" instead of "x IS DISTINCT FROM y".

        .. seealso::

            :meth:`.ColumnOperators.is_distinct_from`

        """
        return exclusions.skip_if(
            lambda config: not config.db.dialect.supports_is_distinct_from,
            "driver doesn't support an IS DISTINCT FROM construct",
        )
