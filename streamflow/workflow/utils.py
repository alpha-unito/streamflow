from typing import Any, Iterable, MutableSequence, Type, Union

from streamflow.core.workflow import Token
from streamflow.workflow.token import (
    IterationTerminationToken,
    ListToken,
    ObjectToken,
    TerminationToken,
)


def check_iteration_termination(inputs: Union[Token, Iterable[Token]]) -> bool:
    return check_token_class(inputs, IterationTerminationToken)


def check_termination(inputs: Union[Token, Iterable[Token]]) -> bool:
    return check_token_class(inputs, TerminationToken)


def check_token_class(inputs: Union[Token, Iterable[Token]], cls: Type[Token]):
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
