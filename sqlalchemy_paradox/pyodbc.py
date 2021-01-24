"""SQLAlchemy Support for the Borland / Corel Paradox databases via pyodbc."""
# coding=utf-8


from .base import ParadoxDialect, strtobool, cl_in, cg
from sqlalchemy.connectors.pyodbc import PyODBCConnector
from itertools import chain
from urllib.parse import unquote_plus
from sqlalchemy.engine.url import URL
from typing import Any, List, Dict, Tuple, Union, Optional


# noinspection PyUnresolvedReferences
class ParadoxDialect_pyodbc(PyODBCConnector, ParadoxDialect):
    """A subclass of the ParadoxDialect for pyodbc."""

    pyodbc_driver_name = "Intersolv Paradox v3.11 (*.db)"

    intersolv_args = {
        "driver": {
            "long_name": "DRV",
            "description": """
            Supplied directly to pyodbc as the `driver` argument.
        """,
            "valid_values": [],
        },
        "autocommit": {
            "long_name": "AC",
            "description": """
            Supplied directly to pyodbc as the `autocommit` argument.
        """,
            "valid_values": [True, False],
        },
        "AUT": {
            "long_name": "ApplicationUsingThreads",
            "description": """
            Ensures that the driver works with multi-threaded applications.
            The default is 1, which makes the driver thread-safe. When using
            the driver with single-threaded applications, you may set this
            option to 0 to avoid additional processing required for ODBC
            thread-safety standards.
            """,
            "valid_values": [0, 1],
        },
        "CT": {
            "long_name": "CreateType",
            "description": """
            This attribute specifies the table version for Create Table statements.
            There are four valid values for this connection string: 3, 4, 5, and null (blank).
            The numeric values map to the major revision numbers of the Paradox family of products.
            To override another CreateType setting chosen during data source configuration with
            the default create type determined by the Level setting in the Paradox section of
            the IDAPI configuration file, set CreateType= (null).
            Note: When CreateType is set to 7, the Paradox driver supports table names up to 128
            characters long. For all other CreateType settings, the driver supports table names up
            to 8 characters long.
            """,
            "valid_values": ["", 3, 4, 5, 7, None],
        },
        "DB": {
            "long_name": "Database",
            "description": """
            The directory in which the Paradox files are stored. For this attribute, you can also
            specify aliases that are defined in your IDAPI configuration file, if you have one. To
            do this, enclose the alias name in colons. For example, to use the alias MYDATA, specify
            "Database=:MYDATA:"
            """,
            "valid_values": [],
        },
        "DSN": {
            "long_name": "DataSourceName",
            "description": """
            A string that identifies a Paradox data source configuration in the system information.
            Examples include "Accounting" or " Paradox-Server".
            """,
            "valid_values": [],
        },
        "DQ": {
            "long_name": "DeferQueryEvaluation",
            "description": """
            This attribute determines when a query is evaluated — after all records are read or each time
            a record is fetched. If DeferQueryEvaluation=0, the driver generates a result set when the
            first record is fetched. The driver reads all records, evaluates each one against the Where
            clause, and compiles a result set containing the records that satisfy the search criteria.
            This process slows performance when the first record is fetched, but activity performed on
            the result set after this point is much faster, because the result set has already been
            created. You do not see any additions, deletions, or changes in the database that occur
            while working from this result set.

            If DeferQueryEvaluation=1 (the default), the driver evaluates the query each time a record
            is fetched, and stops reading through the records when it finds one that matches the search
            criteria. This setting avoids the slowdown while fetching the first record, but each fetch
            takes longer because of the evaluation taking place. The data you retrieve reflect the latest
            changes to the database; however, a result set is still generated if the query is a Union of
            multiple Select statements, if it contains the Distinct keyword, or if it has an Order By or
            Group By clause.
            """,
            "valid_values": [0, 1],
        },
        "FOC": {
            "long_name": "FileOpenCache",
            "description": """
            The maximum number of unused table opens to cache. For example, when FileOpenCache=4, and a
            user opens and closes four tables, the tables are not actually closed. The driver keeps them
            open so that if another query uses one of these tables, the driver does not have to perform
            another open, which is expensive. The advantage of using file open caching is increased performance.
            The disadvantage is that a user who tries to open the table exclusively may get a locking conflict
            even though no one appears to have the table open. The initial default is 0.
            """,
            "valid_values": [],
        },
        "IS": {
            "long_name": "IntlSort",
            "description": """
            This attribute determines the order that records are retrieved when you issue a Select
            statement with an Order By clause. If IntlSort=0 (the initial default), the driver uses
            the ASCII sort order. This order sorts items alphabetically with uppercase letters preceding
            lowercase letters. For example, "A, b, C" would be sorted as "A, C, b."If IntlSort=1, the
            driver uses the international sort order as defined by your operating system. This order
            is always alphabetic, regardless of case; the letters from the previous example would be
            sorted as "A, b, C." See your operating system documentation concerning the sorting of
            accented characters.
            """,
            "valid_values": [0, 1],
        },
        "ND": {
            "long_name": "NetDir",
            "description": """
            The directory containing the PARADOX.NET file that corresponds to the database you have
            specified. If theParadox database you are using is shared on a network, then every user
            who accesses it must set this value to point to the same PARADOX.NET file. If not specified,
            this value is determined by the NetDir setting in the Paradox section of the IDAPI
            configuration file. If you are not sure how to set this value, contact your network
            administrator.
            """,
            "valid_values": [],
        },
        "PW": {
            "long_name": "Passwords",
            "description": """
            A password or list of passwords. You can add 1 to 50 passwords into the system using a
            comma-separated list of passwords. Passwords are case-sensitive.
            For example,Passwords=psw1, psw2, psw3.
            """,
            "valid_values": [],
        },
        "USF": {
            "long_name": "UltraSafeCommit",
            "description": """
            This attribute determines when the driver flushes its changes to disk. If UltraSafeCommit=1,
            the driver does this at each COMMIT. This decreases performance. The default is 0. This
            means that the driver flushes its changes to disk when the table is closed or when internal
            buffers are full. In this case, a machine "crash" before closing a table may cause recent
            changes to be lost.
            """,
            "valid_values": [0, 1],
        },
        "ULQ": {
            "long_name": "UseLongQualifiers",
            "description": """
            This attribute specifies whether the driver uses long path names as table qualifiers.
            With UseLongQualifiers set to 1 path names can be up to 255 characters. The default is 0;
            maximum length is 128 characters.
            """,
            "valid_values": [0, 1],
        },
    }

    @property
    def arg_name_map(self) -> Dict[str, Optional[Union[str, int]]]:
        """Mapping for long names to short names."""
        return {self.intersolv_args.get(key).get("long_name"): key for key in self.intersolv_args}

    def create_connect_args(self, url: URL) -> Tuple[List[Any], Dict[str, Optional[Union[int, str]]]]:
        """Create connection arguments from the supplied URL."""

        conn_args = {
            "autocommit": True,
            "DB": "C:\\Paradox",
            "ND": None,
            "AUT": 1,
            "CT": 4,
            "DQ": 0,
            "FOC": 0,
            "IS": 1,
            "USF": 0,
            "ULQ": 1,
        }

        opts = url.translate_connect_args()

        if not opts:
            opts = dict()
            opts["host"] = url.query.get("odbc_connect", None)

        if cg(opts, "host", False):
            supplied_args = {
                pair[0]: pair[1]
                for pair in map(
                    lambda entry: entry.split("="),
                    chain.from_iterable(map(lambda item: item.split(";"), unquote_plus(opts.get("host")).split("&"))),
                )
            }

            supplied_args = {
                value: cg(supplied_args, key) if not cl_in(value, supplied_args.keys()) else cg(supplied_args, value)
                for key, value in self.arg_name_map.items()
            }

            conn_args.update(supplied_args)

        if cg(conn_args, "driver", cg(conn_args, "drv")) is not None:
            conn_args["Driver"] = str(cg(conn_args, "driver", cg(conn_args, "drv")))
        else:
            conn_args["Driver"] = "{Intersolv Paradox v3.11 (*.db)}"

        if cg(conn_args, "autocommit", cg(conn_args, "ac")) is not None:
            conn_args["autocommit"] = strtobool(cg(conn_args, "autocommit", cg(conn_args, "ac")))
        else:
            conn_args["autocommit"] = True

        return (
            [],
            {key: value for key, value in conn_args.items() if value is not None},
        )

    def connect(self, *args: Any, **kwargs: Any):
        """Establish a connection using pyodbc."""

        if cg(kwargs, "dsn", False):
            return self.dbapi.connect(
                f"DSN={cg(kwargs, 'dsn', '')}",
                autocommit=cg(kwargs, "autocommit", cg(kwargs, "ac", False)),
            )

        conn_string = ";".join(
            (
                "Driver=" + cg(kwargs, "driver", "{Intersolv Paradox v3.11 (*.db)}"),
                ";".join(
                    (
                        f"{key}={value}"
                        for key, value in kwargs.items()
                        if all(
                            (
                                not cl_in(key, ("driver", "autocommit", "dsn")),
                                cl_in(key, self.intersolv_args.keys()),
                            )
                        )
                    )
                ),
            )
        )

        non_conn_args = {key: value for key, value in kwargs.items() if not cl_in(key, self.intersolv_args.keys())}

        return self.dbapi.connect(conn_string, **non_conn_args)
