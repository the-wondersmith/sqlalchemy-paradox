"""Run SQLAlchemy's dialect testing suite against the Paradox dialect."""
# coding=utf-8

# noinspection PyPackageRequirements,PyUnresolvedReferences
import pytest
from typing import Any
from decimal import Decimal

from sqlalchemy.testing.suite import *
from sqlalchemy import types as sql_types

from sqlalchemy.testing import requires
from sqlalchemy.util import u
from sqlalchemy import inspect, select, case, bindparam, exc, literal, exists
from sqlalchemy import Table, Column, String, Numeric, Text


from sqlalchemy.testing.suite import ComponentReflectionTest as _ComponentReflectionTest
from sqlalchemy.testing.suite import (
    CompositeKeyReflectionTest as _CompositeKeyReflectionTest,
)
from sqlalchemy.testing.suite import CompoundSelectTest as _CompoundSelectTest
from sqlalchemy.testing.suite import DateTest as _DateTest
from sqlalchemy.testing.suite import (
    DateTimeCoercedToDateTimeTest as _DateTimeCoercedToDateTimeTest,
)
from sqlalchemy.testing.suite import (
    DateTimeMicrosecondsTest as _DateTimeMicrosecondsTest,
)
from sqlalchemy.testing.suite import DateTimeTest as _DateTimeTest
from sqlalchemy.testing.suite import EscapingTest as _EscapingTest
from sqlalchemy.testing.suite import ExceptionTest as _ExceptionTest
from sqlalchemy.testing.suite import ExistsTest as _ExistsTest
from sqlalchemy.testing.suite import ExpandingBoundInTest as _ExpandingBoundInTest
from sqlalchemy.testing.suite import InsertBehaviorTest as _InsertBehaviorTest
from sqlalchemy.testing.suite import IntegerTest as _IntegerTest
from sqlalchemy.testing.suite import LastrowidTest as _LastrowidTest
from sqlalchemy.testing.suite import LimitOffsetTest as _LimitOffsetTest
from sqlalchemy.testing.suite import NumericTest as _NumericTest
from sqlalchemy.testing.suite import OrderByLabelTest as _OrderByLabelTest
from sqlalchemy.testing.suite import QuotedNameArgumentTest as _QuotedNameArgumentTest
from sqlalchemy.testing.suite import RowFetchTest as _RowFetchTest
from sqlalchemy.testing.suite import SimpleUpdateDeleteTest as _SimpleUpdateDeleteTest
from sqlalchemy.testing.suite import StringTest as _StringTest
from sqlalchemy.testing.suite import TableDDLTest as _TableDDLTest
from sqlalchemy.testing.suite import TextTest as _TextTest
from sqlalchemy.testing.suite import TimeTest as _TimeTest
from sqlalchemy.testing.suite import UnicodeTextTest as _UnicodeTextTest
from sqlalchemy.testing.suite import UnicodeVarcharTest as _UnicodeVarcharTest


class ComponentReflectionTest(_ComponentReflectionTest):
    """Test dialect handling of reflection and introspection."""

    def _test_get_noncol_index(self, tname, ixname) -> None:
        """Insert DocString Here."""

        meta = self.metadata
        insp = inspect(meta.bind)
        indexes = insp.get_indexes(tname)

        # reflecting an index that has "x DESC" in it as the column.
        # the DB may or may not give us "x", but make sure we get the index
        # back, it has a name, it's connected to the table.
        expected_indexes = [{"unique": False, "name": str(ixname).upper()}]
        self._assert_insp_indexes(indexes, expected_indexes)

        t = Table(tname, meta, autoload_with=meta.bind)
        assert len(t.indexes) == 1
        assert list(t.indexes)[0].table is t
        assert list(t.indexes)[0].name == ixname

    def _test_get_pk_constraint(self, schema_=None) -> None:
        """Insert DocString Here."""
        meta = self.metadata
        users, addresses = self.tables.users, self.tables.email_addresses
        insp = inspect(meta.bind)

        users_cons = insp.get_pk_constraint(users.name, schema_=schema_)
        users_pkeys = users_cons["constrained_columns"]
        assert users_pkeys == ["user_id"]

        # The email_addresses table is created (for whatever reason)
        # without any primary keys or indexes. Rather than mess with
        # the test's underlying table creation routine we'll just
        # slightly modify the assertions so that either the tests
        # originally expected values or the values we know will
        # actually be returned by the Intersolv driver will allow
        # the test to pass

        addr_cons = insp.get_pk_constraint(addresses.name, schema_=schema_)
        addr_pkeys = addr_cons["constrained_columns"]
        assert addr_pkeys == ["address_id"]

        with requires.reflects_pk_names.fail_if():
            assert addr_cons["name"] in ("email_ad_pk", "PRIMARY")

    def _test_get_table_names(self, schema_=None, table_type="table", order_by=None):
        _ignore_tables = [
            "comment_test",
            "noncol_idx_test_pk",
            "noncol_idx_test_nopk",
            "local_table",
            "remote_table",
            "remote_table_2",
        ]
        meta = self.metadata

        insp = inspect(meta.bind)

        if table_type == "view":
            table_names = insp.get_view_names(schema_)
            table_names.sort()
            answer = ["email_addresses_v", "users_v"]
            assert sorted(table_names) == answer
        else:
            if order_by:
                tables = [
                    rec[0]
                    for rec in insp.get_sorted_table_and_fkc_names(schema_)
                    if rec[0]
                ]
            else:
                tables = insp.get_table_names(schema_)
            table_names = [t for t in tables if t not in _ignore_tables]

            # The built-in SQLAlchemy version of this test differentiates
            # between `order_by` columns, specifically foreign_key. Seeing
            # as the Paradox tables we're testing don't *really* have
            # or care about foreign keys, we're gonna slightly modify
            # the test to just check for the expected tables in the
            # returned list of table names instead

            answer = ["users", "email_addresses", "dingalings"]

            for t_n in answer:
                assert t_n.upper() in table_names

    def test_get_indexes(self, schema_=None) -> None:
        """Test the dialect's introspection of table indexes."""

        # The modifications to this test are extremely minor,
        # differing from the super method only by the value of
        # the expected return indexes to reflect the note below

        meta = self.metadata

        insp = inspect(meta.bind)
        indexes = insp.get_indexes("users", schema_=schema_)

        # Indexes aren't technically mandatory for Paradox tables,
        # according the Intersolv documentation. However, if a table
        # has any indexes, it must have *at least* a PRIMARY index.

        expected_indexes = [
            {"name": "PRIMARY", "unique": True, "column_names": ["user_id"]},
            {
                "name": "USERS_ALL_IDX",
                "unique": False,
                "column_names": ["user_id", "test2", "test1"],
            },
            {
                "name": "USERS_T_IDX",
                "unique": False,
                "column_names": ["test1", "test2"],
            },
        ]

        index_names = [d["name"] for d in indexes]
        for e_index in expected_indexes:
            assert e_index["name"] in index_names
            index = indexes[index_names.index(e_index["name"])]
            for key in e_index:
                assert e_index[key] == index[key], f"{e_index[key]} != {index[key]}"

    def test_numeric_reflection(self) -> None:
        """Test the dialect's handling of numeric reflection."""

        # The built-in SQLAlchemy test uses a precision of 18, but
        # the Intersolv driver maxes out at a precision of 15 and
        # doesn't appear to care about or support scale

        for typ in self._type_round_trip(sql_types.Numeric(15, 5)):
            assert isinstance(typ, sql_types.Numeric)
            assert typ.precision == 15
            assert typ.scale is None

    @pytest.mark.skip(reason="Intersolv driver doesn't support foreign keys.")
    def test_get_foreign_keys(self) -> None:
        """Test the dialect's introspection of foreign key columns."""
        # Technically the dialect implements foreign key introspection correctly,
        # and the Intersolv driver doesn't freak out when you request a list of
        # foreign keys for a given table, but it doesn't appear to return anything
        # useful either. As such, we're forced to skip this test as well.
        pass

    @pytest.mark.skip(reason="Intersolv driver doesn't support temporary tables")
    def test_get_temp_table_names(self) -> None:
        """Test the dialect's handling of indexes on temporary tables."""
        # The Intersolv driver doesn't do temp tables
        pass

    @pytest.mark.skip(reason="Intersolv driver doesn't support temporary tables")
    def test_get_temp_table_columns(self) -> None:
        """Test the dialect's introspection of columns on temporary tables."""
        # The Intersolv driver doesn't do temp tables, so it sure as hell
        # doesn't do column introspection on temporary tables either.
        pass

    @pytest.mark.skip(reason="Intersolv driver doesn't support temporary tables")
    def test_get_temp_table_indexes(self) -> None:
        """Test the dialect's introspection of indexes on temporary tables."""
        # The Intersolv driver doesn't do temp tables, so it sure as hell
        # doesn't do index introspection on temporary tables either.
        pass

    @pytest.mark.skip(reason="Paradox doesn't support views, temporary or otherwise")
    def test_get_temp_view_columns(self) -> None:
        """Test the dialect's introspection of views on temporary tables."""
        # The Intersolv driver doesn't do views or temp tables, so it sure as
        # hell doesn't do introspection on either one.
        pass

    @pytest.mark.skip(reason="Paradox doesn't support views, temporary or otherwise")
    def test_get_temp_view_names(self) -> None:
        """Test the dialect's handling of indexes on temporary tables."""
        # The Intersolv driver doesn't do views, temporary or otherwise,
        # so it sure as hell doesn't do introspection on them either.
        pass

    @pytest.mark.skip(reason="Paradox doesn't support views, temporary or otherwise")
    def test_get_temp_table_unique_constraints(self) -> None:
        """Test the dialect's handling of indexes on temporary tables."""
        # The Intersolv driver doesn't do temp tables, so it sure as hell
        # doesn't do introspection on them either.
        pass


class CompositeKeyReflectionTest(_CompositeKeyReflectionTest):
    """Test the dialect's handling of composite key reflections."""

    @pytest.mark.skip(reason="Intersolv driver doesn't support foreign keys.")
    def test_fk_column_order(self):
        """Insert DocString Here."""
        # The Intersolv driver doesn't do foreign keys.
        pass

    def test_pk_column_order(self) -> None:
        """Test the dialect's handling of primary key column ordering."""

        # The only difference between this test and the SQLAlchemy version
        # is the order of the expected column names. The SQLAlchemy version
        # expects ["name", "id", "attr"] but the dialect reorders the columns
        # to be in the order that Paradox expects them to be in. Other than
        # that, it's identical.

        meta = self.metadata
        insp = inspect(meta.bind)
        primary_key = insp.get_pk_constraint(self.tables.tb1.name)
        eq_(primary_key.get("constrained_columns"), ["id", "attr", "name"])


class CompoundSelectTest(_CompoundSelectTest):
    """Test the dialect's handling of compound select statements."""

    @pytest.mark.skip(reason="Support for aliased queries not implemented (yet)")
    def test_limit_offset_aliased_selectable_in_unions(self) -> None:
        """Test how the dialect handles unioned select statements between two aliased queries."""

        # The Intersolv driver *does* technically support aliases in general,
        # but doesn't appear to support aliased *queries* and the documentation
        # (such as it is) doesn't really clarify one way or the other. As such,
        # we're going to skip this test and just allow the dialect to raise
        # an exception if a user attempts to use it in such a way that results
        # in a sub-query-aliased union-ed statement is emitted.

        pass

    @pytest.mark.skip(reason="Support for aliased queries not implemented (yet)")
    def test_limit_offset_in_unions_from_alias(self) -> None:
        """Insert DocString Here."""

        # This test faces the same issue as `test_limit_offset_aliased_selectable_in_unions`

        pass

    @pytest.mark.skip(reason="Support for aliased queries not implemented (yet)")
    def test_limit_offset_selectable_in_unions(self) -> None:
        """Insert DocString Here."""

        # This test faces the same issue as `test_limit_offset_aliased_selectable_in_unions`

        pass

    @pytest.mark.skip(reason="Support for aliased queries not implemented (yet)")
    def test_select_from_plain_union(self) -> None:
        """insert DocString Here."""

        # This test faces the same issue as `test_limit_offset_aliased_selectable_in_unions`

        pass


class DateTest(_DateTest):
    """Test the dialect's handling of dates."""

    def test_null(self) -> None:
        """Test how the dialect handles `null` for dates."""

        date_table = self.tables.date_table

        # The builtin SQLAlchemy test method does not provide a value for the `id` column,
        # but is otherwise identical. The value had to be added to compensate for the fact
        # that the underlying table fixture doesn't have the `id` column marked as nullable
        # or autoincrement, and doesn't provide a default value. The dialect handles the table
        # creation correctly, because it internally uses Paradox's AutoIncrement type for any
        # autoincrement column. SQLAlchemy doesn't seem to care though and throws this error -
        #
        # Column 'date_table.id' is marked as a member of the primary key for table 'date_table',
        # but has no Python-side or server-side default generator indicated, nor does it indicate
        # 'autoincrement=True' or 'nullable=True', and no explicit value is passed.  Primary key
        # columns typically may not store NULL.

        statement = date_table.insert({"id": 50, "date_data": None})
        config.db.execute(statement)

        row = config.db.execute(select([date_table.c.date_data])).first()
        assert row == (None,)

    @requires.standalone_null_binds_whereclause
    def test_null_bound_comparison(self):
        # This test is identical to the super method except where annotated
        date_table = self.tables.date_table
        with config.db.connect() as conn:
            # The super method doesn't provide a value for the `id` column,
            # which triggers the error described by the annotations in the
            # `test_null` method above. An arbitrary value of 60 is supplied here
            statement = date_table.insert({"id": 60, "date_data": self.data})
            conn.execute(statement)
            stmt = select([date_table.c.id]).where(
                case(
                    [
                        (
                            bindparam("foo", type_=self.datatype) != None,
                            bindparam("foo", type_=self.datatype),
                        )
                    ],
                    else_=date_table.c.date_data,
                )
                == date_table.c.date_data
            )

            row = conn.execute(stmt, {"foo": None}).first()

            # The super method tests for equality between
            # the expected value for `id` and the retrieved
            # value. Because the select statement uses a CASE
            # which would result in a null value for the `id`
            # column if it didn't exist, we are forced to work
            # around the lack of an expected value by testing
            # for inequality with null (None)

            assert row[0] is not None

    def test_round_trip(self) -> None:
        """Test round trip."""
        date_table = self.tables.date_table

        # The builtin SQLAlchemy test method does not provide a value for the `id` column,
        # but is otherwise identical. The value had to be added to compensate for the fact
        # that the underlying table fixture doesn't have the `id` column marked as nullable
        # or autoincrement, and doesn't provide a default value. The dialect handles the table
        # creation correctly, because it internally uses Paradox's AutoIncrement type for any
        # autoincrement column. SQLAlchemy doesn't seem to care though and throws this error -
        #
        # Column 'date_table.id' is marked as a member of the primary key for table 'date_table',
        # but has no Python-side or server-side default generator indicated, nor does it indicate
        # 'autoincrement=True' or 'nullable=True', and no explicit value is passed.  Primary key
        # columns typically may not store NULL.

        statement = date_table.insert({"id": 50, "date_data": self.data})
        config.db.execute(statement)

        row = config.db.execute(select([date_table.c.date_data])).first()
        compare = self.compare or self.data

        assert row == (compare,)
        assert isinstance(row[0], type(compare))


# noinspection PyTypeChecker
class DateTimeCoercedToDateTimeTest(_DateTimeCoercedToDateTimeTest):
    """Test the dialect's handling of datetime coercions."""

    def test_null(self):
        """Test nulls."""
        return DateTest.test_null(self)

    def test_null_bound_comparison(self):
        """Test null bound comparisons."""
        return DateTest.test_null_bound_comparison(self)

    def test_round_trip(self):
        """Test round trip."""
        return DateTest.test_round_trip(self)


# noinspection PyTypeChecker
class DateTimeMicrosecondsTest(_DateTimeMicrosecondsTest):
    """Test the dialect's handling of microseconds in datetime values."""

    def test_null(self):
        """Test nulls."""
        return DateTest.test_null(self)

    def test_null_bound_comparison(self):
        """Test null bound comparisons."""
        return DateTest.test_null_bound_comparison(self)

    def test_round_trip(self) -> None:
        """Test round trip."""
        date_table = self.tables.date_table

        statement = date_table.insert({"id": 50, "date_data": self.data})
        config.db.execute(statement)

        row = config.db.execute(select([date_table.c.date_data])).first()
        compare = self.compare or self.data

        # It's not clear if Paradox supports storing microseconds as part of
        # time or timestamp columns, but the Intersolv driver definitely doesn't.
        # However, the other pertinent attributes should still be faithfully
        # stored and returned, so we'll still check for equality on those.

        for attr in ("year", "month", "day", "hour", "minute", "second"):
            assert getattr(row[0], attr, None) == getattr(compare, attr, None)
        assert isinstance(row[0], type(compare))


# noinspection PyTypeChecker
class DateTimeTest(_DateTimeTest):
    """Test the dialect's handling of datetime objects."""

    def test_null(self):
        """Test nulls."""
        return DateTest.test_null(self)

    def test_null_bound_comparison(self):
        """Test null bound comparisons."""
        return DateTest.test_null_bound_comparison(self)

    def test_round_trip(self):
        """Test round trip."""
        return DateTest.test_round_trip(self)


class EscapingTest(_EscapingTest):
    """Test the dialect's handling of escaped values."""

    metadata: Any

    @provide_metadata
    def test_percent_sign_round_trip(self):
        """test that the DBAPI accommodates for escaped / non-escaped
        percent signs in a way that matches the compiler
        """
        m = self.metadata
        t = Table("t", m, Column("data", String(50)))
        t.create(config.db)
        with config.db.begin() as conn:
            conn.execute(t.insert(), dict(data="some % value"))
            conn.execute(t.insert(), dict(data="some %% other value"))

            # The Intersolv driver is almost absurdly permissive about
            # table and column names, and seems to handle double-quoted
            # strings fairly gracefully. It doesn't require quoting of
            # strings containing percent signs, but it *does* store
            # string data with padding, so we'll have to tweak the
            # assertions for this test a smidge by wrapping the
            # `data` column with the TRIM function

            assert (
                conn.scalar(
                    select([func.trim(t.c.data).label("data")]).where(
                        t.c.data == "some % value"
                    )
                )
                == "some % value"
            )
            assert (
                conn.scalar(
                    select([func.trim(t.c.data).label("data")]).where(
                        t.c.data == "some %% other value"
                    )
                )
                == "some %% other value"
            )


class ExceptionTest(_ExceptionTest):
    """Test the dialect's raising and handling of various exceptions."""

    def test_integrity_error(self):
        """Test that the dialect raises an integrity error if you
        attempt to duplicate a primary key value.
        """

        with config.db.connect() as conn:

            trans = conn.begin()
            conn.execute(self.tables.manual_pk.insert(), {"id": 1, "data": "d1"})

            with pytest.raises(exc.DBAPIError):
                # This test is identical to the underlying SQLAlchemy version
                # except that the super method tests for exc.IntegrityError
                # whereas the error that's actually raised is exc.DBAPIError
                conn.execute(self.tables.manual_pk.insert({"id": 1, "data": "d1"}))

            trans.rollback()


class ExistsTest(_ExistsTest):
    """Test the dialect's handling of the EXISTS keyword."""

    def test_select_exists(self, connection):
        """Test the dialect's handling of EXISTS in select statements"""
        stuff = self.tables.stuff
        query = select([literal(1)]).where(exists().where(stuff.c.data == "some data"))
        comparison = connection.execute(query).fetchone()

        # This test is virtually unaltered from the SQLAlchemy version
        # with the exception of the assertion below - the super method
        # checks for equality with 1, however there is currently an
        # unknown issue with pyodbc that causes integer columns to be
        # returned as float values. It's not technically pertinent to
        # this test, but it does change the expected outcome as python
        # performs equality checks on tuples as a whole, not value-by-value.

        assert comparison == (1.0,)


class ExpandingBoundInTest(_ExpandingBoundInTest):
    """Test the dialect's handling of expanding bounds."""

    @pytest.mark.skip(reason="Paradox does not support empty set expression")
    def test_empty_set_against_integer(self):
        # Paradox does not support empty set expression
        pass

    @pytest.mark.skip(reason="Paradox does not support empty set expression")
    def test_empty_set_against_integer_negation(self):
        # Paradox does not support empty set expression
        pass

    @pytest.mark.skip(reason="Paradox does not support empty set expression")
    def test_empty_set_against_string(self):
        # Paradox does not support empty set expression
        pass

    @pytest.mark.skip(reason="Paradox does not support empty set expression")
    def test_empty_set_against_string_negation(self):
        # Paradox does not support empty set expression
        pass

    @pytest.mark.skip(reason="Paradox does not support empty set expression")
    def test_null_in_empty_set_is_false(self):
        # Paradox does not support empty set expression
        pass

    @pytest.mark.skip(reason="Paradox does not support empty set expression")
    def test_multiple_empty_sets(self):
        # Paradox does not support empty set expression
        pass


# noinspection PyComparisonWithNone
class InsertBehaviorTest(_InsertBehaviorTest):
    """Test the dialect's handling of insert queries."""

    def test_autoclose_on_insert(self):
        if requirements.returning.enabled:
            engine = engines.testing_engine(options={"implicit_returning": False})
        else:
            engine = config.db

        # NOTE: See previous notes about issue w/ SQLAlchemy's
        # handling of Paradox's AutoIncrement columns

        with engine.begin() as conn:
            r = conn.execute(
                self.tables.autoinc_pk.insert({"id": 5, "data": "some data"})
            )
        assert r._soft_closed
        assert not r.closed
        assert r.is_insert
        assert not r.returns_rows

    @requirements.insert_from_select
    def test_insert_from_select(self):
        table = self.tables.manual_pk
        config.db.execute(
            table.insert(),
            [
                dict(id=1, data="data1"),
                dict(id=2, data="data2"),
                dict(id=3, data="data3"),
            ],
        )

        config.db.execute(
            table.insert(inline=True).from_select(
                ("id", "data"),
                select([table.c.id + 5, table.c.data]).where(
                    table.c.data.in_(["data2", "data3"])
                ),
            )
        )

        # The underlying Paradox tables store string data with padding,
        # so we'll need to wrap the column in a TRIM call to get
        # the equalities to work out correctly

        result = config.db.execute(
            select([func.trim(table.c.data).label("data")]).order_by(table.c.data)
        ).fetchall()

        eq_(result, [("data1",), ("data2",), ("data2",), ("data3",), ("data3",)])

    @requirements.insert_from_select
    def test_insert_from_select_autoinc(self):
        src_table = self.tables.manual_pk
        dest_table = self.tables.autoinc_pk
        config.db.execute(
            src_table.insert(),
            [
                dict(id=1, data="data1"),
                dict(id=2, data="data2"),
                dict(id=3, data="data3"),
            ],
        )

        result = config.db.execute(
            dest_table.insert().from_select(
                ("data",),
                select([src_table.c.data]).where(
                    src_table.c.data.in_(["data2", "data3"])
                ),
            )
        )

        eq_(result.inserted_primary_key, [None])

        # The underlying Paradox tables store string data with padding,
        # so we'll need to wrap the column in a TRIM call to get
        # the equalities to work out correctly

        result = config.db.execute(
            select([func.trim(dest_table.c.data).label("data")]).order_by(
                dest_table.c.data
            )
        ).fetchall()

        eq_(result, [("data2",), ("data3",)])

    @requirements.insert_from_select
    def test_insert_from_select_with_defaults(self):
        table = self.tables.includes_defaults

        for item in (
            {"id": 1, "data": "data1"},
            {"id": 2, "data": "data2"},
            {"id": 3, "data": "data3"},
        ):
            config.db.execute(table.insert(item))
            del item

        select_1 = select([table.c.id + 5, table.c.data]).where(
            table.c.data.in_(["data2", "data3"])
        )
        statement_2 = table.insert(inline=True).from_select(("id", "data"), select_1)

        config.db.execute(statement_2)

        # NOTE: See previous notes about issue w/ Paradox's string-type storage

        result = config.db.execute(
            select(
                [
                    table.c.id,
                    func.trim(table.c.data).label("data"),
                    table.c.x,
                    table.c.y,
                ]
            ).order_by(table.c.data, table.c.id)
        ).fetchall()

        eq_(
            result,
            [
                (1, "data1", 5, 4),
                (2, "data2", 5, 4),
                (4, "data2", 5, 4),
                (3, "data3", 5, 4),
                (5, "data3", 5, 4),
            ],
        )


class IntegerTest(_IntegerTest):
    """Test the dialect's handling of integer objects."""

    metadata: Any

    @testing.provide_metadata
    def _round_trip(self, datatype, data):
        metadata = self.metadata
        int_table = Table(
            "integer_table",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("integer_data", datatype),
        )

        metadata.create_all(config.db)

        # NOTE: See previous notes about issue w/ SQLAlchemy's
        # handling of Paradox's AutoIncrement columns

        config.db.execute(int_table.insert(), {"id": 1, "integer_data": data})

        row = config.db.execute(select([int_table.c.integer_data])).first()

        eq_(row, (data,))

        assert isinstance(row[0], int)

    @pytest.mark.skip(reason="Paradox only supports integer values up to 2147483647")
    def test_huge_int(self):
        # super method uses a value of 1376537018368127, but
        # Paradox only supports up to 2147483647
        self._round_trip(sql_types.BigInteger, 2147483647)


class LastrowidTest(_LastrowidTest):
    """Test the dialect's lastrowid property."""

    def _assert_round_trip(self, table, conn):

        # NOTE: See previous notes about issue w/
        # Paradox's storing of string-type data

        row = conn.execute(
            select([table.c.id, func.trim(table.c.data).label("data")])
        ).first()
        eq_(row, (config.db.dialect.default_sequence_base, "some data"))

    def test_autoincrement_on_insert(self):

        # NOTE: See previous notes about issue w/ SQLAlchemy's
        # handling of Paradox's AutoIncrement columns

        config.db.execute(self.tables.autoinc_pk.insert({"id": 1, "data": "some data"}))
        self._assert_round_trip(self.tables.autoinc_pk, config.db)

    def test_last_inserted_id(self):

        # NOTE: See previous notes about issue w/ SQLAlchemy's
        # handling of Paradox's AutoIncrement columns

        r = config.db.execute(
            self.tables.autoinc_pk.insert({"id": 2, "data": "some data"})
        )
        pk = config.db.scalar(select([self.tables.autoinc_pk.c.id]))
        eq_(r.inserted_primary_key, [pk])


@pytest.mark.skip(
    reason="The Intersolv Paradox driver doesn't support LIMIT or OFFSET."
)
class LimitOffsetTest(_LimitOffsetTest):
    """Test the dialect's handling of limit / offset statements."""


class NumericTest(_NumericTest):
    """Test the dialect's handling of numeric objects."""

    metadata: Any

    @staticmethod
    def __num_table(md) -> Table:
        """Create a Paradox table to use for the test, if one doesn't exist already.
        """

        # The Intersolv driver doesn't allow selecting data that doesn't
        # already exist in a table, so we'll have to ensure that we have
        # an actual table from which to select data for the test.

        num_table = Table(
            "num_table",
            md,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("data", Numeric),
        )

        existence_check = (
            md.bind.raw_connection().cursor().tables("NUM_TABLE").fetchone()
        )

        if existence_check is None:
            md.create_all()
            try:
                num_table.create()
            except exc.DBAPIError:
                pass

        return num_table

    @testing.provide_metadata
    def test_decimal_coerce_round_trip(self):

        num_table = self.__num_table(md=self.metadata)

        # Unless a number is provided as a whole number, regardless of
        # it's actual type, the dialect is going to store it as Paradox's
        # NUMBER type. That means that it's *going* to come back as a float.
        # As such, we'll have to rework the test to ensure that the data we
        # need will exist in a table and then we'll have to convert the queried
        # value back to its non-float form if we want to do strict comparison tests.

        expr = Decimal("15.7563")
        num_table.insert({"id": 1, "data": expr}).execute()

        val = Decimal(str(sum(testing.db.execute(select([num_table.c.data])).first())))
        eq_(val, expr)

    @pytest.mark.skip(
        reason="The Intersolv Paradox driver doesn't support CAST or CONVERT"
    )
    def test_decimal_coerce_round_trip_w_cast(self):
        # Paradox doesn't support CAST or CONVERT
        pass

    @testing.provide_metadata
    def test_float_coerce_round_trip(self):

        num_table = self.__num_table(md=self.metadata)

        expr = 15.7563
        num_table.insert({"id": 1, "data": expr}).execute()

        val = sum(
            testing.db.execute(
                select([num_table.c.data]).where(num_table.c.data == literal(expr))
            ).first()
        )
        eq_(val, expr)

    def test_numeric_as_decimal(self):
        self._do_test(
            Numeric(precision=8, scale=4), [15.7563, Decimal("15.7563")], [15.7563],
        )

    @testing.emits_warning(r".*does \*not\* support Decimal objects natively")
    def test_render_literal_numeric(self):
        self._literal_round_trip(
            Numeric(precision=8, scale=4), [15.7563, Decimal("15.7563")], [15.7563],
        )

    @testing.provide_metadata
    def teardown(self):
        """Clean up after ourselves."""
        num_table = self.__num_table(md=self.metadata)

        try:
            num_table.drop()
        except exc.DBAPIError:
            pass

        super(NumericTest, self).teardown()


class OrderByLabelTest(_OrderByLabelTest):
    """Test the dialect's handling of order by label statements."""

    # noinspection PyTypeChecker
    def test_composed_int_desc(self):
        table = self.tables.some_table
        lx = (table.c.x + table.c.y).label("lx")

        # Something is causing either pyodbc or the Intersolv driver to return
        # float values instead of integers for columns with type LONG INTEGER.
        # TODO: Update this test if / when the bug between pyodbc and Intersolv
        #       is investigated / resolved.

        result = config.db.execute(select([lx]).order_by(lx.desc())).fetchall()

        eq_(
            list(map(lambda row: tuple((int(val) for val in row)), result)),
            [(7,), (5,), (3,)],
        )

    def test_composed_multiple(self):
        table = self.tables.some_table
        lx = (table.c.x + table.c.y).label("lx")
        ly = (func.trim(func.lower(table.c.q)) + func.trim(table.c.p)).label("ly")
        self._assert_result(
            select([lx, ly]).order_by(lx, ly.desc()),
            [(3, util.u("q1p3")), (5, util.u("q2p2")), (7, util.u("q3p1"))],
        )


class QuotedNameArgumentTest(_QuotedNameArgumentTest):
    """Test the dialect's handling of quoted name arguments."""

    @pytest.mark.skip(
        reason="The Intersolv driver doesn't support foreign key introspection"
    )
    def test_get_foreign_keys(self):
        """Test the dialect's handling of foreign key introspection."""
        pass


class RowFetchTest(_RowFetchTest):
    """Test the dialect's handling of row fetching."""

    def test_row_w_scalar_select(self):
        """test that a scalar select as a column is returned as such
        and that type conversion works OK.

        (this is half a SQLAlchemy Core test and half to catch database
        backends that may have unusual behavior with scalar selects.)

        """
        date_table = self.tables.has_dates

        # Original test code: select([datetable.alias("x").c.today]).as_scalar()
        # TODO: Determine better way to handle aliases

        s = select([date_table.c.today]).as_scalar()
        s2 = select([date_table.c.id, s.label("somelabel")])
        row = config.db.execute(s2).first()

        eq_(row["somelabel"], datetime.datetime(2006, 5, 12, 12, 0, 0))

    def test_via_int(self):
        table = self.tables.plain_pk
        row = config.db.execute(
            select([table.c.id, func.trim(table.c.data).label("data")]).order_by(
                table.c.id
            )
        ).first()

        eq_(row[0], 1)
        eq_(row[1], "d1")

    def test_via_string(self):
        table = self.tables.plain_pk
        row = config.db.execute(
            select([table.c.id, func.trim(table.c.data).label("data")]).order_by(
                table.c.id
            )
        ).first()

        eq_(row["id"], 1)
        eq_(row["data"], "d1")

    def test_via_col_object(self):
        table = self.tables.plain_pk
        # [table.c.id, func.trim(table.c.data).label("data")]
        row = config.db.execute(table.select().order_by(table.c.id)).first()

        eq_(row[self.tables.plain_pk.c.id], 1)
        # TODO: Determine how to remove reliance on python-side string stripping
        eq_(row[self.tables.plain_pk.c.data].strip(), "d1")


class SimpleUpdateDeleteTest(_SimpleUpdateDeleteTest):
    """Test the dialect's handling of simple update delete operations."""

    def test_update(self):
        t = self.tables.plain_pk
        r = config.db.execute(t.update().where(t.c.id == 2), data="d2_new")
        assert not r.is_insert
        assert not r.returns_rows

        eq_(
            config.db.execute(
                select([t.c.id, func.trim(t.c.data).label("data")]).order_by(t.c.id)
            ).fetchall(),
            [(1, "d1"), (2, "d2_new"), (3, "d3")],
        )

    def test_delete(self):
        t = self.tables.plain_pk
        r = config.db.execute(t.delete().where(t.c.id == 2))
        assert not r.is_insert
        assert not r.returns_rows
        eq_(
            config.db.execute(
                select([t.c.id, func.trim(t.c.data).label("data")]).order_by(t.c.id)
            ).fetchall(),
            [(1, "d1"), (3, "d3")],
        )


class StringTest(_StringTest):
    """Test the dialect's handling of string objects."""

    metadata: Any

    @testing.provide_metadata
    def _literal_round_trip(self, type_, input_, output, filter_=None):
        """test literal rendering """

        # for literal, we test the literal render in an INSERT
        # into a typed column.  we can then SELECT it back as its
        # official type; ideally we'd be able to use CAST here
        # but MySQL in particular can't CAST fully
        t = Table("t", self.metadata, Column("x", type_))
        t.create()

        with testing.db.connect() as conn:
            for value in input_:
                ins = (
                    t.insert()
                    .values(x=literal(value))
                    .compile(
                        dialect=testing.db.dialect,
                        compile_kwargs=dict(literal_binds=True),
                    )
                )
                conn.execute(ins)

            if self.supports_whereclause:
                stmt = select([func.trim(t.c.x).label("x")]).where(
                    t.c.x == literal(value)
                )
            else:
                stmt = select([func.trim(t.c.x).label("x")])

            stmt = stmt.compile(
                dialect=testing.db.dialect, compile_kwargs=dict(literal_binds=True),
            )
            for row in conn.execute(stmt):
                value = row[0]
                if filter_ is not None:
                    value = filter_(value)
                assert value in output

    @pytest.mark.skip(reason="Dialect-level Unicode support not implemented (yet).")
    def test_literal_non_ascii(self):
        self._literal_round_trip(
            String(40), [util.u("réve🐍 illé")], [util.u("réve🐍 illé")]
        )


class TableDDLTest(_TableDDLTest):
    """Test the dialect's handling of table-level DDL statements."""

    def _simple_roundtrip(self, table: Table):
        with config.db.begin() as conn:
            data_col: Column = table.c.get("_data", table.c.get("data"))

            conn.execute(table.insert({"id": 1, data_col.name: "some data"}))
            query = select(
                [table.c.id, func.trim(data_col).label(data_col.name)]
            ).where(table.c.id == 1)
            result = conn.execute(query).first()
            eq_(result, (1, "some data"))


class TextTest(_TextTest):
    """Test the dialect's handling of text objects."""

    @staticmethod
    def _get_result_row(connection, table):
        query = select([func.trim(table.c.text_data).label("text_data")])
        result = connection.execute(query).first()
        return result

    @testing.provide_metadata
    def _literal_round_trip(self, type_, input_, output, filter_=None):
        """test literal rendering """

        # for literal, we test the literal render in an INSERT
        # into a typed column.  we can then SELECT it back as its
        # official type; ideally we'd be able to use CAST here
        # but MySQL in particular can't CAST fully
        t = Table("t", self.metadata, Column("x", type_))
        t.create()

        with testing.db.connect() as conn:
            for value in input_:
                ins = (
                    t.insert()
                    .values(x=literal(value))
                    .compile(
                        dialect=testing.db.dialect,
                        compile_kwargs=dict(literal_binds=True),
                    )
                )
                conn.execute(ins)

            if self.supports_whereclause:
                stmt = select([func.trim(t.c.x).label("x")]).where(
                    t.c.x == literal(value)
                )
            else:
                stmt = select([func.trim(t.c.x).label("x")])

            stmt = stmt.compile(
                dialect=testing.db.dialect, compile_kwargs=dict(literal_binds=True),
            )
            for row in conn.execute(stmt):
                value = row[0]
                if filter_ is not None:
                    value = filter_(value)
                assert value in output

    def test_text_roundtrip(self):
        text_table = self.tables.text_table

        config.db.execute(text_table.insert({"id": 1, "text_data": "some text"}))
        row = self._get_result_row(config.db, text_table)
        eq_(row, ("some text",))

    @requires.empty_strings_text
    def test_text_empty_strings(self, connection):
        text_table = self.tables.text_table

        connection.execute(text_table.insert({"id": 1, "text_data": ""}))
        row = self._get_result_row(connection, text_table)
        eq_(row, (None,))

    def test_text_null_strings(self, connection):
        text_table = self.tables.text_table

        connection.execute(text_table.insert({"id": 1, "text_data": None}))
        row = self._get_result_row(connection, text_table)
        eq_(row, (None,))

    @pytest.mark.skip(reason="Dialect-level Unicode support not implemented (yet).")
    def test_literal_non_ascii(self):
        self._literal_round_trip(Text, [util.u("réve🐍 illé")], [util.u("réve🐍 illé")])


class TimeTest(_TimeTest):
    """Test the dialect's handling of time objects."""

    def test_null(self):
        date_table = self.tables.date_table

        config.db.execute(date_table.insert({"id": 1, "date_data": None}))

        row = config.db.execute(select([date_table.c.date_data])).first()
        eq_(row, (None,))

    def test_round_trip(self):
        date_table = self.tables.date_table

        config.db.execute(date_table.insert({"id": 1, "date_data": self.data}))

        row = config.db.execute(select([date_table.c.date_data])).first()

        compare = self.compare or self.data
        eq_(row, (compare,))
        assert isinstance(row[0], type(compare))

    @requires.standalone_null_binds_whereclause
    def test_null_bound_comparison(self):
        # this test is based on an Oracle issue observed in #4886.
        # passing NULL for an expression that needs to be interpreted as
        # a certain type, does the DBAPI have the info it needs to do this.
        date_table = self.tables.date_table
        with config.db.connect() as conn:
            result = conn.execute(date_table.insert({"id": 2, "date_data": self.data}))
            id_ = result.inserted_primary_key[0]
            stmt = select([date_table.c.id]).where(
                case(
                    [
                        (
                            bindparam("foo", type_=self.datatype) != None,
                            bindparam("foo", type_=self.datatype),
                        )
                    ],
                    else_=date_table.c.date_data,
                )
                == date_table.c.date_data
            )

            row = conn.execute(stmt, {"foo": None}).first()
            eq_(row[0], id_)


class UnicodeTextTest(_UnicodeTextTest):
    """Test the dialect's handling of unicode text."""

    @testing.provide_metadata
    def _literal_round_trip(self, type_, input_, output, filter_=None):
        """test literal rendering """

        # for literal, we test the literal render in an INSERT
        # into a typed column.  we can then SELECT it back as its
        # official type; ideally we'd be able to use CAST here
        # but MySQL in particular can't CAST fully
        t = Table("t", self.metadata, Column("x", type_))
        t.create()

        with testing.db.connect() as conn:
            for value in input_:
                ins = (
                    t.insert()
                    .values(x=literal(value))
                    .compile(
                        dialect=testing.db.dialect,
                        compile_kwargs=dict(literal_binds=True),
                    )
                )
                conn.execute(ins)

            if self.supports_whereclause:
                stmt = select([func.trim(t.c.x).label("x")]).where(
                    t.c.x == literal(value)
                )
            else:
                stmt = select([func.trim(t.c.x).label("x")])

            stmt = stmt.compile(
                dialect=testing.db.dialect, compile_kwargs=dict(literal_binds=True),
            )
            for row in conn.execute(stmt):
                value = row[0]
                if filter_ is not None:
                    value = filter_(value)
                assert value in output

    def _test_empty_strings(self, connection):
        unicode_table = self.tables.unicode_table

        connection.execute(unicode_table.insert({"id": 1, "unicode_data": u("")}))
        row = connection.execute(select([unicode_table.c.unicode_data])).first()
        eq_(row, (None,))

    def _test_null_strings(self, connection):
        self._test_empty_strings(connection)

    @pytest.mark.skip(reason="Dialect-level Unicode support not implemented (yet).")
    def test_literal(self):
        """Test dialect handling of literals."""
        pass

    @pytest.mark.skip(reason="Dialect-level Unicode support not implemented (yet).")
    def test_round_trip(self):
        unicode_table = self.tables.unicode_table

        config.db.execute(unicode_table.insert({"id": 1, "unicode_data": self.data}))

        row = config.db.execute(select([unicode_table.c.unicode_data])).first()

        eq_(row, (self.data,))
        assert isinstance(row[0], util.text_type)

    @pytest.mark.skip(reason="Dialect-level Unicode support not implemented (yet).")
    def test_literal_non_ascii(self):
        self._literal_round_trip(
            self.datatype, [util.u("réve🐍 illé")], [util.u("réve🐍 illé")]
        )

    @pytest.mark.skip(reason="Dialect-level Unicode support not implemented (yet).")
    def test_round_trip_executemany(self):
        """Test multiple executions w/ unicode data."""
        pass


@pytest.mark.skip(reason="Dialect-level Unicode support not implemented (yet).")
class UnicodeVarcharTest(_UnicodeVarcharTest):
    """Test the dialect's handling of unicode varchar data."""
