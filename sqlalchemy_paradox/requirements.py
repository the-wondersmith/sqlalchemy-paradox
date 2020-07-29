""" SQLAlchemy Test Suite Requirements
"""

from sqlalchemy.testing.requirements import SuiteRequirements
from sqlalchemy.testing import exclusions


class Requirements(SuiteRequirements):
    """ SQLAlchemy Test Suite Requirements
    """

    def get_order_by_collation(self, config):
        """ Required method of SuiteRequirements
        """
        return super(Requirements, self).get_order_by_collation(config)

    @property
    def table_reflection(self):
        # Example closed exclusion property
        return exclusions.closed()

    @property
    def returning(self):
        # Example open exclusion property
        return exclusions.open()

