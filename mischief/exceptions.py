
class PipeException(Exception):
    pass


class PipeEmpty(PipeException):
    pass


class ActorFinished(Exception):
    pass


class SpawnTimeoutError(Exception):
    pass
