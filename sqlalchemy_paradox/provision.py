"""Provisioning for SQLAlchemy Testing Suite."""
# coding=utf-8

import os
from uuid import uuid4

from sqlalchemy.engine import url as sa_url
from sqlalchemy.testing.provision import create_db
from sqlalchemy.testing.provision import drop_db
from sqlalchemy.testing.provision import follower_url_from_main
from sqlalchemy.testing.provision import log
from sqlalchemy.testing.provision import post_configure_engine
from sqlalchemy.testing.provision import run_reap_dbs
from sqlalchemy.testing.provision import temp_table_keyword_args


@follower_url_from_main.for_db("paradox")
def _paradox_follower_url_from_main(url, ident):
    """Create a usable URL."""
    url = sa_url.make_url(url)
    if all(
        (
            str(url).casefold().startswith("paradox+pyodbc://"),
            any(("dsn" in str(url).casefold(), "test" in str(url).casefold())),
        )
    ):
        return url

    return sa_url.make_url("paradox+pyodbc://DSN=paradox_testing")


@post_configure_engine.for_db("paradox")
def _paradox_post_configure_engine(url, engine, follower_ident):
    """Insert DocString Here."""
    # from sqlalchemy import event
    #
    # @event.listens_for(engine, "connect")
    # def connect(dbapi_connection, connection_record):
    #     """Insert DocString Here."""
    #     # use file DBs in all cases, memory acts kind of strangely
    #     # as an attached
    #     if not follower_ident:
    #         # note this test_schema.db gets created for all test runs.
    #         # there's not any dedicated cleanup step for it.  it in some
    #         # ways corresponds to the "test.test_schema" schema that's
    #         # expected to be already present, so for now it just stays
    #         # in a given checkout directory.
    #         dbapi_connection.execute('ATTACH DATABASE "test_schema.db" AS test_schema')
    #     else:
    #         dbapi_connection.execute(
    #             'ATTACH DATABASE "%s_test_schema.db" AS test_schema' % follower_ident
    #         )
    pass


@create_db.for_db("paradox")
def _paradox_create_db(cfg, eng, ident):
    """Insert DocString Here."""
    pass


@drop_db.for_db("paradox")
def _paradox_drop_db(cfg, eng, ident):
    """Insert DocString Here."""
    # for path in ["%s.db" % ident, "%s_test_schema.db" % ident]:
    #     if os.path.exists(path):
    #         log.info("deleting paradox database file: %s" % path)
    #         os.remove(path)
    pass


@temp_table_keyword_args.for_db("paradox")
def _paradox_temp_table_keyword_args(cfg, eng):
    """Insert DocString Here."""
    return {"prefixes": ["TMP"]}


@run_reap_dbs.for_db("paradox")
def _reap_paradox_dbs(url, idents):
    """Insert DocString Here."""
    # log.info("db reaper connecting to %r", url)
    #
    # log.info("identifiers in file: %s", ", ".join(idents))
    # for ident in idents:
    #     # we don't have a config so we can't call _paradox_drop_db due to the
    #     # decorator
    #     for path in ["%s.db" % ident, "%s_test_schema.db" % ident]:
    #         if os.path.exists(path):
    #             log.info("deleting paradox database file: %s" % path)
    #             os.remove(path)
    pass
