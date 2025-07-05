import sys
from collections import defaultdict

MIN_VALUE = 0.0
MAX_VALUE = 500.0
NUM_BUCKETS = 1000

def get_bucket_range(bucket_idx, min_val, max_val, num_buckets):
    bucket_width = (max_val - min_val) / num_buckets
    bucket_start = min_val + (bucket_idx * bucket_width)
    bucket_end = bucket_start + bucket_width
    return bucket_start, bucket_end

def process_line(line, bucket_counts, total_count, sample_values):
    try:
        key, value = line.strip().split('\t')
        
        if key.startswith("BUCKET_"):
            bucket_idx = int(key.split('_')[1])
            bucket_counts[bucket_idx] += int(value)
            
        elif key == "TOTAL_COUNT":
            total_count[0] += int(value)  
            
        elif key.startswith("SAMPLE_"):
            bucket_idx = int(key.split('_')[1])
            sample_values[bucket_idx].append(float(value))
            
    except ValueError:
        _ = None  

def reducer():
    bucket_counts = defaultdict(int)
    total_count = [0]  
    sample_values = defaultdict(list)

    for line in sys.stdin:
        process_line(line, bucket_counts, total_count, sample_values)
    
    total_count_value = total_count[0]
    median_position = total_count_value / 2.0
    
    print(f"=== Histogram Based Median Calculation ===")
    print(f"Total number of records: {total_count_value}")
    print(f"Median pozition: {median_position:.0f}")
    print(f"Number of buckets used: {len(bucket_counts)}")
    
    cumulative_count = 0
    median_bucket = None
    median_value = None
    
    for bucket_idx in sorted(bucket_counts.keys()):
        count_in_bucket = bucket_counts[bucket_idx]
        cumulative_count += count_in_bucket
    
        if cumulative_count >= median_position and median_bucket is None:
            median_bucket = bucket_idx
            bucket_start, bucket_end = get_bucket_range(
                bucket_idx, MIN_VALUE, MAX_VALUE, NUM_BUCKETS
            )
            
            position_in_bucket = median_position - (cumulative_count - count_in_bucket)
            fraction_in_bucket = position_in_bucket / count_in_bucket
            median_value = bucket_start + (fraction_in_bucket * (bucket_end - bucket_start))
            
            print(f"\nMedian bucket: {bucket_idx}")
            print(f"Bucket range: [{bucket_start:.4f}, {bucket_end:.4f}]")
            print(f"Number of records in the bucket: {count_in_bucket}")
            print(f"Position in bucket: {position_in_bucket:.0f}")
    
    if median_value is not None:
        print(f"\n*** Calculated Median: {median_value:.4f} ***")
        if median_bucket in sample_values and sample_values[median_bucket]:
            samples = sorted(sample_values[median_bucket])
            print(f"\nValidation - Example values ​​in this bucket:")
            print(f"Min: {min(samples):.4f}")
            print(f"Max: {max(samples):.4f}")
            print(f"Average: {sum(samples)/len(samples):.4f}")
    
    print("\n=== Histogram Distribution (First 20 bucket) ===")
    for i in range(min(20, max(bucket_counts.keys()) + 1)):
        if i in bucket_counts:
            bucket_start, bucket_end = get_bucket_range(i, MIN_VALUE, MAX_VALUE, NUM_BUCKETS)
            bar_length = int((bucket_counts[i] / max(bucket_counts.values())) * 40)
            bar = '#' * bar_length
            print(f"[{bucket_start:6.2f}-{bucket_end:6.2f}]: {bar} ({bucket_counts[i]})")

if __name__ == "__main__":
    reducer()
