from typing import Optional, Type


class StateShim:
    def __init__(self, start_state):
        self._current_state = start_state
        self._checkpoint_state = start_state

    def get_state(self, intended_type: Optional[Type]):
        if not intended_type:
            return self._current_state
        if isinstance(self._current_state, intended_type) or self._current_state is None:
            return self._current_state
        return intended_type(self._current_state)

    def set_state(self, state):
        self._current_state = state

    def commit(self):
        self._checkpoint_state = self._current_state

    def rollback(self):
        self._current_state = self._checkpoint_state

    def reset(self):
        self._current_state = None
