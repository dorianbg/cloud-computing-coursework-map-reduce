import os
from pathlib import Path


def run_check(folder):
    folder = "test"
    files = os.listdir(folder)

    sum = 0
    for file in files:
        if not file.startswith("."):
            path = Path(folder, file)
            print(path)
            with open(path) as f:
                for line in f.readlines():
                    value = line.strip("\n").split("\t")[0]
                    sum += float(value)
    print(sum)


if __name__ == '__main__':
    run_check("test")
