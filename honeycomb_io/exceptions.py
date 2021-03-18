
class HoneycombError(Exception):
    pass

class HoneycombWriteError(HoneycombError):
    pass

class HoneycombWriteErrorRetry(HoneycombWriteError):
    pass

class HoneycombWriteErrorNoRetry(HoneycombWriteError):
    pass

class HoneycombWriteErrorNoRetryCleanupFailed(HoneycombWriteError):
    pass

class HoneycombDeleteError(HoneycombError):
    pass
