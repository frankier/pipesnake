class PipeSnakeException(Exception):
    pass


class NetException(PipeSnakeException):
    pass


class RootRequiredException(PipeSnakeException):
    pass


class NoDefaultException(PipeSnakeException):
    pass


class NoDefaultSourceException(NoDefaultException):
    pass


class NoDefaultSinkException(NoDefaultException):
    pass


class UnconnectedGraphException(PipeSnakeException):
    pass
