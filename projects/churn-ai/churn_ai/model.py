# Holds the models. Each model should inherit from the base Model class.


class Model:
    """Parent class that will be inherited by all the child classes below"""

    def __init__(self, data=None, config=None):
        self.data = data
        self.config = config

    def fit(self):
        # May not be necessary
        return "yess, it worked"

    def predict(self):
        pass
