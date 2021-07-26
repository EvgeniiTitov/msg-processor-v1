import argparse
import time


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--iterate_till", type=int, default=100)
    return parser.parse_args()


def real_busy_stuff(iterations):
    sum_ = 0
    for i in range(iterations):
        sum_ += i
        print(f"{i} / {sum_}")
        time.sleep(1)
    return sum_


def main():
    args = parse_args()
    iterations = args.iterate_till
    the_sum = real_busy_stuff(iterations)
    print("THE RESULT:", the_sum)


if __name__ == "__main__":
    main()
