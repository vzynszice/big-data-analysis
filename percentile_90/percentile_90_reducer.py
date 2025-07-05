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

def process_line(line, bucket_counts, total_count, line_count):
    line_count[0] += 1
    try:
        parts = line.strip().split('\t')
        if len(parts) == 2:
            key, value = parts
            
            if key.startswith("BUCKET_"):
                bucket_idx = int(key[7:]) 
                bucket_counts[bucket_idx] += int(value)
                
            elif key == "TOTAL_COUNT":
                total_count[0] += int(value)
        else:
            invalid_format = True
                
    except (ValueError, IndexError) as e:
        # Debug için hata mesajı
        print(f"DEBUG: Line {line_count[0]} error while processing: {e}", file=sys.stderr)
        error_logged = True

def print_histogram(bucket_counts, percentile_bucket):
    print("\n=== Histogram Distribution (First 50 Bucket) ===")
    max_count = max(bucket_counts.values()) if bucket_counts else 1
    for i in range(min(50, max(bucket_counts.keys()) + 1)):
        if i in bucket_counts:
            bucket_start, bucket_end = get_bucket_range(i, MIN_VALUE, MAX_VALUE, NUM_BUCKETS)
            count = bucket_counts[i]
            bar_length = int((count / max_count) * 40) if max_count > 0 else 0
            bar = '#' * bar_length
            marker = ""
            if i == percentile_bucket:
                marker = " <-- 90th PERCENTILE"
            
            print(f"Bucket {i:03d} [{bucket_start:6.2f}-{bucket_end:6.2f}]: {bar} ({count}){marker}")

def calculate_other_percentiles(bucket_counts, total_count_value):
    print("\n=== Other Percentile Values ​​(For Comparison) ===")
    percentiles = [
        (50, "Median"),
        (95, "95th percentile"),
        (99, "99th percentile")
    ]
    
    for percentile, label in percentiles:
        position = total_count_value * (percentile / 100.0)
        cumulative = 0
        
        for bucket_idx in sorted(bucket_counts.keys()):
            cumulative += bucket_counts[bucket_idx]
            if cumulative >= position:
                bucket_start, bucket_end = get_bucket_range(bucket_idx, MIN_VALUE, MAX_VALUE, NUM_BUCKETS)
                value = (bucket_start + bucket_end) / 2
                print(f"{percentile}th percentile ({label}): ~{value:.2f} μg/m³")
                bucket_idx = max(bucket_counts.keys()) + 1  # Döngüyü sonlandırmak için

def reducer():
    bucket_counts = defaultdict(int)
    total_count = [0]  
    line_count = [0]   
    for line in sys.stdin:
        process_line(line, bucket_counts, total_count, line_count)
    total_count_value = total_count[0]
    print(f"DEBUG: Total {line_count[0]} lines read", file=sys.stderr)
    print(f"DEBUG: {len(bucket_counts)} different buckets found", file=sys.stderr)
    
    # Sonuçları yazdır
    print(f"=== 90th Percentile Calculation Results  ===")
    print(f"Total number of records: {total_count_value}")
    
    if total_count_value == 0:
        print("ERROR: No data processed!")
        return
    percentile_90_position = total_count_value * 0.9
    print(f"90th percentile pozition: {percentile_90_position:.0f}")
    print(f"(90% of the values ​​are below this position)")
    print(f"Number of buckets used: {len(bucket_counts)}")
    
    if len(bucket_counts) == 0:
        print("\nERROR: No bucket data found!")
        print("Check the Mapper output.")
        return
        
    cumulative_count = 0
    percentile_bucket = None
    percentile_value = None
    
    for bucket_idx in sorted(bucket_counts.keys()):
        count_in_bucket = bucket_counts[bucket_idx]
        cumulative_count += count_in_bucket
        
        if cumulative_count >= percentile_90_position and percentile_bucket is None:
            percentile_bucket = bucket_idx
            bucket_start, bucket_end = get_bucket_range(
                bucket_idx, MIN_VALUE, MAX_VALUE, NUM_BUCKETS
            )
            
            position_in_bucket = percentile_90_position - (cumulative_count - count_in_bucket)
            fraction_in_bucket = position_in_bucket / count_in_bucket if count_in_bucket > 0 else 0.5
            percentile_value = bucket_start + (fraction_in_bucket * (bucket_end - bucket_start))
            
            print(f"\n90th percentile bucket: {bucket_idx}")
            print(f"Bucket range: [{bucket_start:.4f}, {bucket_end:.4f}]")
            print(f"Number of records in bucket: {count_in_bucket}")
            print(f"Pozition in bucket: {position_in_bucket:.0f}")
    
    if percentile_value is not None:
        print(f"\n*** 90th Percentile Value: {percentile_value:.4f} μg/m³ ***")
        print(f"\n=== Interpretation of Results ===")
        print(f"90% of the measurements are below {percentile_value:.2f} μg/m³.")
        
        if percentile_value <= 35:
            print("✓ EPA 24-hour standard (35 μg/m³) is met!")
        else:
            print(f"⚠ EPA standard {percentile_value - 35:.1f} μg/m³ is exceeded!")
            
    calculate_other_percentiles(bucket_counts, total_count_value)
    print_histogram(bucket_counts, percentile_bucket)

    print(f"\n=== Data Distribution Summary ===")
    print(f"Total number of buckets: {len(bucket_counts)}")
    print(f"Lowest bucket index: {min(bucket_counts.keys())}")
    print(f"Highest bucket index: {max(bucket_counts.keys())}")
    print(f"Highest bucket: {max(bucket_counts, key=bucket_counts.get)} ({max(bucket_counts.values())} records)")

if __name__ == "__main__":
    reducer()
