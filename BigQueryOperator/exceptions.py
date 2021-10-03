class Error(Exception):
    """Base class for exceptions in this module."""
    pass


class SqlNotSet(Error):
    """Exception raised for errors in the input.

    Attributes:
        expression -- input expression in which the error occurred
        message -- explanation of the error
    """

    def __init__(self, expression, message, func_name=False):
        self.expression = expression
        self.message = message
        self.func_name = func_name

        if self.func_name is not False:
            print(self.func_name + " - Needs a query.")