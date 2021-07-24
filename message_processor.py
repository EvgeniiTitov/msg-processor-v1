import time


def process_message(message: str) -> None:
    for i in range(10):
        time.sleep(1)
        print(f"BUSILY PROCESSING THE MESSAGE LMAO! {message} - {0}/{i}")
    return
