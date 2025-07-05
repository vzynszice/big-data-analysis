import sys
import csv

def welford_update(value, n, mean, M2):
    n[0] += 1
    delta = value - mean[0]
    mean[0] += delta / n[0]
    delta2 = value - mean[0]
    M2[0] += delta * delta2

def process_row(row, n, mean, M2):
    try:
        value = float(row['arithmetic_mean'])
        if value >= 0:
            welford_update(value, n, mean, M2)
        else:
            negative_value_found = True        
    except (ValueError, KeyError):
        error_in_row = True

def mapper():
    n = [0]
    mean = [0.0]
    M2 = [0.0]
    csv_reader = csv.DictReader(sys.stdin)
    for row in csv_reader:
        process_row(row, n, mean, M2)
    
    if n[0] > 0:
        print(f"STATS\t{n[0]}\t{mean[0]}\t{M2[0]}")
        print(f"DEBUG: Mapper {n[0]} değer işledi, local mean={mean[0]:.4f}", 
              file=sys.stderr)

if __name__ == "__main__":
    mapper()
