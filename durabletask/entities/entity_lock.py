class EntityLock:
    def __init__(self, context):
        self._context = context

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):  # TODO: Handle exceptions?
        self._context._exit_critical_section()
