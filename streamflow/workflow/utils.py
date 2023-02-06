from __future__ import annotations

from typing import MutableSequence, TYPE_CHECKING

from streamflow.core.workflow import Token
from streamflow.workflow.token import (
    IterationTerminationToken,
    ListToken,
    ObjectToken,
    TerminationToken,
)

if TYPE_CHECKING:
    from typing import Any, Iterable


def check_iteration_termination(inputs: Token | Iterable[Token]) -> bool:
    return check_token_class(inputs, IterationTerminationToken)


def check_termination(inputs: Token | Iterable[Token]) -> bool:
    return check_token_class(inputs, TerminationToken)


def check_token_class(inputs: Token | Iterable[Token], cls: type[Token]):
    if isinstance(inputs, Token):
        return isinstance(inputs, cls)
    else:
        for token in inputs:
            if isinstance(token, MutableSequence):
                if check_token_class(token, cls):
                    return True
            elif isinstance(token, cls):
                return True
        return False


def get_token_value(token: Token) -> Any:
    if isinstance(token, ListToken):
        return [get_token_value(t) for t in token.value]
    elif isinstance(token, ObjectToken):
        return {k: get_token_value(v) for k, v in token.value.items()}
    elif isinstance(token.value, Token):
        return get_token_value(token.value)
    else:
        return token.value
