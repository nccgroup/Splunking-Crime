class ModelNotFoundException(RuntimeError):
    def __init__(self):
        super(ModelNotFoundException, self).__init__('Model does not exist')


class ModelNotAuthorizedException(Exception):
    def __init__(self):
        super(ModelNotAuthorizedException, self).__init__('Permission denied')


class ExperimentError(Exception):
    """
    Base class for Experiment-related exceptions

    """

    def __init__(self, message='', exception=None):
        super(ExperimentError, self).__init__(message)
        self.exception = exception


class ExperimentNotAuthorizedError(ExperimentError):
    def __init__(self, message='Permission denied', exception=None):
        super(ExperimentNotAuthorizedError, self).__init__(message, exception)


class ExperimentNotFoundError(ExperimentError):
    def __init__(self, message='Experiment does not exist', exception=None):
        super(ExperimentNotFoundError, self).__init__(message, exception)


class ExperimentValidationError(ExperimentError):
    def __init__(self, message='Experiment is invalid', exception=None):
        super(ExperimentValidationError, self).__init__(message, exception)


class LookupNotFoundException(RuntimeError):
    def __init__(self):
        super(LookupNotFoundException, self).__init__('Lookup does not exist')


class LookupAlreadyExists(RuntimeError):
    def __init__(self):
        super(LookupAlreadyExists, self).__init__('ID already in use')


class LookupNotAuthorizedException(Exception):
    def __init__(self):
        super(LookupNotAuthorizedException, self).__init__('Permission denied')
