class PermanentStageError(RuntimeError):
    """A stage failure that retrying cannot fix, such as a table schema clash.

    The runner fails the step at once instead of re-running the whole window.
    """
