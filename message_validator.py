"""
A message validator function to be defined here
It receives a message and then outputs whether it is valid or not. The decision
regarding the message validity is up to whoever implements the validation
function
"""
# TODO: Replace with an object with __call__()


def validate_message(message: str) -> bool:
    if not message:
        return False

    tokens = message.split(";")

    if len(tokens) != 3:
        return False
    if any(not len(t) for t in tokens):
        return False
    return True
