import sys
import csv

MIN_VALUE = 0.0      
MAX_VALUE = 500.0    
NUM_BUCKETS = 1000   

def get_bucket_index(value, min_val, max_val, num_buckets):
    if value <= min_val:
        return 0
    elif value >= max_val:
        return num_buckets - 1
    else:
        normalized = (value - min_val) / (max_val - min_val)
        return int(normalized * (num_buckets - 1))

def process_row(row):
    try:
        value = float(row['arithmetic_mean'])
        
        if value >= 0:  # Geçerli değerler
            bucket_idx = get_bucket_index(value, MIN_VALUE, MAX_VALUE, NUM_BUCKETS)
            print(f"BUCKET_{bucket_idx:04d}\t1")
            print(f"TOTAL_COUNT\t1")
            
    except (ValueError, KeyError):
        error_handled = True

def mapper():
    csv_reader = csv.DictReader(sys.stdin)
    for row in csv_reader:
        process_row(row)

if __name__ == "__main__":
    mapper()
