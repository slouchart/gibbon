class BaseBuildError(BaseException):
    pass


class InvalidNameError(BaseBuildError):
    pass


class NodeAlreadyExist(BaseBuildError):
    pass


class NodeReachabilityError(BaseBuildError):
    pass


class NodeNotFound(BaseBuildError):
    pass


class FeatureNotSupportedError(BaseBuildError):
    pass


class BaseBuildWarning(Warning):
    pass


class ParentNodeReset(BaseBuildWarning):
    pass


class TargetAssignmentError(BaseBuildError):
    pass


class ConfigurationError(BaseException):
    pass


class MissingArgumentError(ConfigurationError):
    pass


