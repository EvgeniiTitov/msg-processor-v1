def validate_message(message: str) -> bool:
    if not message:
        return False

    tokens = message.split(";")

    if len(tokens) != 3:
        return False
    if any(not len(t) for t in tokens):
        return False
    return True
