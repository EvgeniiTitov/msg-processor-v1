import threading
import time

# Nice one python

def do_some_work():
    print("Doing some work")
    time.sleep(5)


def main():
    t = threading.Thread(target=do_some_work)
    t.start()

    keep_running = True
    while keep_running:
        t.join(timeout=1)

        if t.is_alive():
            print("Timed out")
            time.sleep(0.5)
        else:
            print("Complete")
            keep_running = False


if __name__ == '__main__':
    main()
