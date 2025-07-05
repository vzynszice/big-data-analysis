import sys
import math

def combine_moments(N_A, mean_A, M2_A, M3_A, N_B, mean_B, M2_B, M3_B):
    N_total = N_A + N_B
    if N_total == 0:
        return 0, 0.0, 0.0, 0.0

    delta = mean_B - mean_A
    delta_n = delta / N_total
    M2_total = M2_A + M2_B + delta * delta_n * N_A * N_B
    M3_total = (M3_A + M3_B + 
                delta_n * delta * delta * N_A * N_B * (N_A - N_B) / N_total + 
                3.0 * delta_n * (N_A * M2_B - N_B * M2_A))               
    mean_total = (N_A * mean_A + N_B * mean_B) / N_total
    return N_total, mean_total, M2_total, M3_total

def process_stats_line(parts, total_stats, first_group):
    if parts[0] == "STATS_SKEW" and len(parts) == 5:
        n_k = int(parts[1])
        mean_k = float(parts[2])
        M2_k = float(parts[3])
        M3_k = float(parts[4])
        
        if n_k > 0:  
            if first_group[0]:
                total_stats[0] = n_k
                total_stats[1] = mean_k
                total_stats[2] = M2_k
                total_stats[3] = M3_k
                first_group[0] = False
            else:
                combined = combine_moments(
                    total_stats[0], total_stats[1], total_stats[2], total_stats[3],
                    n_k, mean_k, M2_k, M3_k
                )
                total_stats[0] = combined[0]  # n
                total_stats[1] = combined[1]  # mean
                total_stats[2] = combined[2]  # M2
                total_stats[3] = combined[3]  # M3

def print_statistics(total_n, total_mean, total_M2, total_M3):
    print(f"total_records\t{total_n}")
    print(f"global_mean\t{total_mean}")
    print(f"global_M2\t{total_M2}")
    print(f"global_M3\t{total_M3}")
    if total_n > 1:  
        sample_variance = total_M2 / (total_n - 1)
        sample_std_dev = math.sqrt(sample_variance) if sample_variance >= 0 else 0
    else:
        sample_variance = 0
        sample_std_dev = 0
        
    population_variance = total_M2 / total_n
    population_std_dev = math.sqrt(population_variance) if population_variance >= 0 else 0

    print(f"sample_variance\t{sample_variance}")
    print(f"sample_std_dev\t{sample_std_dev}")
    print(f"population_variance\t{population_variance}")
    print(f"population_std_dev\t{population_std_dev}")

    if sample_std_dev > 0 and total_n > 0:
        skew_g1 = (total_M3 / total_n) / (sample_std_dev ** 3)
        print(f"skewness_g1\t{skew_g1}")
    else:
        print(f"skewness_g1\tNaN")

def reducer():
    total_stats = [0, 0.0, 0.0, 0.0]
    first_group = [True]
    for line in sys.stdin:
        try:
            parts = line.strip().split('\t')
            process_stats_line(parts, total_stats, first_group) 
        except (ValueError, IndexError) as e:
            error_logged = True

    if total_stats[0] > 0:
        print_statistics(total_stats[0], total_stats[1], 
                        total_stats[2], total_stats[3])
    else:
        print("HATA: Hiç geçerli veri bulunamadı veya işlenemedi!", file=sys.stderr)

if __name__ == "__main__":
    reducer()
