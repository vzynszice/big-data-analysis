#!/usr/bin/env python3
"""

Bu reducer, mapper'lardan gelen bucket sayımlarını toplar ve
histogram üzerinden median değerini hesaplar.
"""

import sys
from collections import defaultdict

# Mapper ile aynı parametreleri kullan
MIN_VALUE = 0.0
MAX_VALUE = 500.0
NUM_BUCKETS = 1000

def get_bucket_range(bucket_idx, min_val, max_val, num_buckets):
    """Verilen bucket indeksi için değer aralığını hesaplar."""
    bucket_width = (max_val - min_val) / num_buckets
    bucket_start = min_val + (bucket_idx * bucket_width)
    bucket_end = bucket_start + bucket_width
    return bucket_start, bucket_end

def process_line(line, bucket_counts, total_count, sample_values):
    """Her satırı işleyen yardımcı fonksiyon"""
    try:
        key, value = line.strip().split('\t')
        
        if key.startswith("BUCKET_"):
            # Bucket sayacını artır
            bucket_idx = int(key.split('_')[1])
            bucket_counts[bucket_idx] += int(value)
            
        elif key == "TOTAL_COUNT":
            total_count[0] += int(value)  # Liste kullanarak mutable referans
            
        elif key.startswith("SAMPLE_"):
            # Örnek değerleri sakla (doğrulama için)
            bucket_idx = int(key.split('_')[1])
            sample_values[bucket_idx].append(float(value))
            
    except ValueError:
        # Hata durumunda hiçbir şey yapma
        _ = None  # Dummy işlem

def reducer():
    """Histogram bucket'larını toplar ve median'ı hesaplar."""
    
    # Bucket sayaçları
    bucket_counts = defaultdict(int)
    total_count = [0]  # Mutable referans için liste kullanıyoruz
    sample_values = defaultdict(list)
    
    # Mapper çıktılarını oku
    for line in sys.stdin:
        process_line(line, bucket_counts, total_count, sample_values)
    
    # total_count listesinden değeri al
    total_count_value = total_count[0]
    
    # Median pozisyonunu hesapla
    median_position = total_count_value / 2.0
    
    print(f"=== Histogram Tabanlı Median Hesaplama ===")
    print(f"Toplam kayıt sayısı: {total_count_value}")
    print(f"Median pozisyonu: {median_position:.0f}")
    print(f"Kullanılan bucket sayısı: {len(bucket_counts)}")
    
    # Histogram üzerinde yürüyerek median'ı bul
    cumulative_count = 0
    median_bucket = None
    median_value = None
    
    # Bucket'ları sıralı olarak işle
    for bucket_idx in sorted(bucket_counts.keys()):
        count_in_bucket = bucket_counts[bucket_idx]
        cumulative_count += count_in_bucket
        
        # Median bu bucket'ta mı?
        if cumulative_count >= median_position and median_bucket is None:
            median_bucket = bucket_idx
            bucket_start, bucket_end = get_bucket_range(
                bucket_idx, MIN_VALUE, MAX_VALUE, NUM_BUCKETS
            )
            
            # Bucket içindeki pozisyonu hesapla
            position_in_bucket = median_position - (cumulative_count - count_in_bucket)
            fraction_in_bucket = position_in_bucket / count_in_bucket
            
            # Linear interpolasyon ile median tahmin et
            median_value = bucket_start + (fraction_in_bucket * (bucket_end - bucket_start))
            
            print(f"\nMedian bucket: {bucket_idx}")
            print(f"Bucket aralığı: [{bucket_start:.4f}, {bucket_end:.4f}]")
            print(f"Bucket içindeki kayıt sayısı: {count_in_bucket}")
            print(f"Bucket içindeki pozisyon: {position_in_bucket:.0f}")
            # break yerine döngü devam edecek ama median_bucket != None olduğu için
            # bir daha bu bloğa girmeyecek
    
    if median_value is not None:
        print(f"\n*** Hesaplanan Median: {median_value:.4f} ***")
        
        # Eğer örnek değerler varsa, doğrulama yap
        if median_bucket in sample_values and sample_values[median_bucket]:
            samples = sorted(sample_values[median_bucket])
            print(f"\nDoğrulama - Bu bucket'taki örnek değerler:")
            print(f"Min: {min(samples):.4f}")
            print(f"Max: {max(samples):.4f}")
            print(f"Ortalama: {sum(samples)/len(samples):.4f}")
    
    # Ek istatistikler: Histogram görselleştirmesi
    print("\n=== Histogram Dağılımı (İlk 20 bucket) ===")
    for i in range(min(20, max(bucket_counts.keys()) + 1)):
        if i in bucket_counts:
            bucket_start, bucket_end = get_bucket_range(i, MIN_VALUE, MAX_VALUE, NUM_BUCKETS)
            bar_length = int((bucket_counts[i] / max(bucket_counts.values())) * 40)
            bar = '#' * bar_length
            print(f"[{bucket_start:6.2f}-{bucket_end:6.2f}]: {bar} ({bucket_counts[i]})")

if __name__ == "__main__":
    reducer()
