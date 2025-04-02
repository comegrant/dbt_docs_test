def model_version() -> str:
    import os

    default = "testing"
    version = os.getenv("GIT_COMMIT_HASH", default)
    if not version:
        return default
    else:
        return version
