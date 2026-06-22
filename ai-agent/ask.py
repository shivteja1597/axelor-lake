import sys

from app import answer_question


def main() -> int:
    question = " ".join(sys.argv[1:]).strip()
    if not question:
        print("Please enter a question.")
        return 1

    print(answer_question(question))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

