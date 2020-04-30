from sqlalchemy.testing.requirements import SuiteRequirements
from sqlalchemy.testing import exclusions


class Requirements(SuiteRequirements):

    def get_order_by_collation(self, config):
        """ Required method of SuiteRequirements
        """
        pass

    @property
    def table_reflection(self):
        return exclusions.closed()

    @property
    def returning(self):
        return exclusions.open()

