""" PyODBC connector for Paradox
"""

from .base import ParadoxExecutionContext, ParadoxDialect
from sqlalchemy.connectors.pyodbc import PyODBCConnector


class ParadoxExecutionContext_pyodbc(ParadoxExecutionContext):
    """ Execution context
    """
    pass


class ParadoxDialect_pyodbc(PyODBCConnector, ParadoxDialect):
    """ Dialect
    """

    execution_ctx_cls = ParadoxExecutionContext_pyodbc

    pyodbc_driver_name = "Microsoft Paradox Driver (*.db)"

    def create_connect_args(self, url, **kwargs):
        # Whatever PyODBC does to create the connection string is probably
        # better than anything we're going to come up with
        # Truthfully, our only real concern is that we forcibly set a value
        # for autocommit otherwise the Paradox driver *will* throw an error
        conn_args = super(ParadoxDialect_pyodbc, self).create_connect_args(url)

        if all((len(conn_args) >= 2, isinstance(conn_args[1], dict))):
            # Not to contradict the comment above, but
            # we also need to remove the "trusted_connection" argument
            # from the connection string if it ends up getting passed in
            # as that *also* seems to cause the Paradox driver to pitch a fit
            temp = conn_args[0]
            filtered_args = list()
            for arg_set in temp:
                split_up = arg_set.split(";")
                filtered_args.append(";".join(x for x in split_up if "trusted_connection" not in x.lower()))

            autocommit_fix = conn_args[1]
            autocommit_fix["autocommit"] = kwargs.get("autocommit", True)

            ret_val = [filtered_args, autocommit_fix]

            return ret_val
        else:
            return conn_args

