from abc import ABC, abstractmethod


class Datasource(ABC):
    """
    Base class for all datasources
    """

    def __init__(self):
        super(Datasource, self).__init__()
        args = self.arg_parser.parse_known_args()[0]

    @abstractmethod
    def load(self, source_file):
        """
        Loads datasource
        """
        pass

    @abstractmethod
    def get_for_date(self, snapshot_date):
        """
        Returns dataset history for given date
        """
        pass
