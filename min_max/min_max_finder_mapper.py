import sys
import csv

VALUE_COLUMN_NAME = 'arithmetic_mean'

def process_row(row):
    try:
        value_str = row.get(VALUE_COLUMN_NAME)
        if value_str is not None and value_str.strip() != "":
            value = float(value_str)
            print(f"MIN_VALUE\t{value}")
            print(f"MAX_VALUE\t{value}")
    except (ValueError, TypeError, KeyError) as e:
        error_occurred = True  

def mapper():
    csv_reader = csv.DictReader(sys.stdin)

    for row in csv_reader:
        process_row(row)

if __name__ == "__main__":
    mapper()
