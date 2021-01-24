"""PyTest configuration for SQLAlchemy-Paradox."""
# coding=utf-8

# noinspection PyPackageRequirements
import pytest
from sqlalchemy.dialects import registry

registry.register(
    "paradox.pyodbc", "sqlalchemy_paradox.pyodbc", "ParadoxDialect_pyodbc"
)

pytest.register_assert_rewrite("sqlalchemy.testing.assertions")

from sqlalchemy.testing.plugin.pytestplugin import *
