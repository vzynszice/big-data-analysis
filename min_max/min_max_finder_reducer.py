
import sys

def process_line(line, current_min, current_max):
    try:
        key, value_str = line.strip().split('\t')
        value = float(value_str)

        if key == "MIN_VALUE":
            if current_min[0] is None or value < current_min[0]:
                current_min[0] = value
        elif key == "MAX_VALUE":
            if current_max[0] is None or value > current_max[0]:
                current_max[0] = value
    except ValueError:
        error_flag = True

def reducer():
    current_min = [None]
    current_max = [None]

    for line in sys.stdin:
        process_line(line, current_min, current_max)
    if current_min[0] is not None:
        print(f"global_min\t{current_min[0]}")
    if current_max[0] is not None:
        print(f"global_max\t{current_max[0]}")

if __name__ == "__main__":
    reducer()
