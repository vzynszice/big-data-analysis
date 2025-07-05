import sys
import csv

VALUE_COLUMN_NAME = 'arithmetic_mean'

def update_statistics(x, n, mean, M2, M3):
    n1 = n[0]  
    n[0] += 1  
    delta = x - mean[0]
    delta_n = delta / n[0]
    delta_n2 = delta_n * delta_n  
    term1 = delta * delta_n * n1
    mean[0] += delta_n
    M3[0] += term1 * delta_n * (n[0] - 2) - 3 * delta_n * M2[0]  
    M2[0] += term1  

def process_row(row_dict, n, mean, M2, M3, first_line):
    if first_line[0]:
        first_line[0] = False
    
    try:
        value_str = row_dict.get(VALUE_COLUMN_NAME)
        
        if value_str is not None and value_str.strip() != "":
            x = float(value_str)
            if x >= 0:  
                update_statistics(x, n, mean, M2, M3)
            else:
                negative_value_skipped = True
        else:
            empty_value_skipped = True
    except (ValueError, TypeError, KeyError) as e:
        error_handled = True

def mapper():
    n = [0]
    mean = [0.0]
    M2 = [0.0]
    M3 = [0.0]
    first_line = [True]
    csv_reader = csv.DictReader(sys.stdin)
    
    for row_dict in csv_reader:
        process_row(row_dict, n, mean, M2, M3, first_line)

    if n[0] > 0:
        print(f"STATS_SKEW\t{n[0]}\t{mean[0]}\t{M2[0]}\t{M3[0]}")

if __name__ == "__main__":
    mapper()
