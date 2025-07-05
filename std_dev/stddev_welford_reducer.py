import sys
import math

def combine_statistics(n1, mean1, M2_1, n2, mean2, M2_2):
    n = n1 + n2
    if n == 0:
        return 0, 0.0, 0.0
    mean = (n1 * mean1 + n2 * mean2) / n
    delta = mean2 - mean1
    M2 = M2_1 + M2_2 + delta * delta * n1 * n2 / n
    return n, mean, M2

def process_stats_line(parts, total_stats, first_group):
    if parts[0] == "STATS" and len(parts) == 4:
        n = int(parts[1])
        mean = float(parts[2])
        M2 = float(parts[3])
        if first_group[0]:
            total_stats[0] = n
            total_stats[1] = mean
            total_stats[2] = M2
            first_group[0] = False
        else:
            combined = combine_statistics(
                total_stats[0], total_stats[1], total_stats[2],
                n, mean, M2
            )
            total_stats[0] = combined[0]  # n
            total_stats[1] = combined[1]  # mean
            total_stats[2] = combined[2]  # M2

def calculate_and_print_results(total_n, total_mean, total_M2):
    variance = total_M2 / total_n
    std_dev = math.sqrt(variance)
    if total_n > 1:
        sample_variance = total_M2 / (total_n - 1)
        sample_std_dev = math.sqrt(sample_variance)
    else:
        sample_variance = 0
        sample_std_dev = 0
    
   print(f"=== PM2.5 Statistics (Welford's Algorithm) ===")
    print(f"Total number of records: {total_n}")
    print(f"Mean (μ): {total_mean:.4f} μg/m³")
    print(f"\n--- Population Statistics (divide by N) ---")
    print(f"Population Variance (σ²): {variance:.4f}")
    print(f"Population Standard Deviation (σ): {std_dev:.4f} μg/m³")
    print(f"\n--- Sample Statistics (divide by N-1) ---")
    print(f"Sample Variance (s²): {sample_variance:.4f}")
    print(f"Sample Standard Deviation (s): {sample_std_dev:.4f} μg/m³")
    cv = (std_dev / total_mean) * 100 if total_mean != 0 else 0
    print(f"\nCoefficient of Variation (CV): {cv:.2f}%")
    print(f"\n=== Interpretation of Results ===")
    print(f"Average PM2.5 concentration {total_mean:.2f} μg/m³")

    if total_mean <= 5:
        print("✓ Below WHO annual target value (5 μg/m³) - Excellent!")
    elif total_mean <= 10:
        print("⚠ Below WHO interim target 4 (10 μg/m³) - Good")
    elif total_mean <= 15:
        print("⚠ Below WHO interim target 3 (15 μg/m³) - Fair")
    else:
        print("⚠ Above WHO target values ​​- Improvement required")
        
    print(f"\nVariability analysis:")
    if cv < 20:
        print(f"CV=%{cv:.1f} - Low variability, consistent air quality")
    elif cv < 50:
        print(f"CV=%{cv:.1f} - Medium variability, moderate fluctuations")
    else:
        print(f"CV=%{cv:.1f} - High volatility, significant fluctuations")

def reducer():
    total_stats = [0, 0.0, 0.0]
    first_group = [True]

    for line in sys.stdin:
        try:
            parts = line.strip().split('\t')
            process_stats_line(parts, total_stats, first_group)    
        except (ValueError, IndexError):
            error_occurred = True
    
    if total_stats[0] > 0:
        calculate_and_print_results(total_stats[0], total_stats[1], total_stats[2])
    else:
        print("ERROR: No valid data found!")

if __name__ == "__main__":
    reducer()
