class BaseBuildError(BaseException):
    ...


class InvalidNameError(BaseBuildError):
    ...


class NodeAlreadyExist(BaseBuildError):
    ...


class NodeReachabilityError(BaseBuildError):
    ...


class NodeNotFound(BaseBuildError):
    ...


class FeatureNotSupportedError(BaseBuildError):
    ...


class BaseBuildWarning(Warning):
    ...


class ParentNodeReset(BaseBuildWarning):
    ...


class TargetAssignmentError(BaseBuildError):
    ...


class ConfigurationError(BaseException):
    ...


class MissingArgumentError(ConfigurationError):
    ...
