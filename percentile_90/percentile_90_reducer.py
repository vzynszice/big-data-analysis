#!/usr/bin/env python3
"""
90th Percentile Hesaplama 
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

def process_line(line, bucket_counts, total_count, line_count):
    """Her satırı işleyen yardımcı fonksiyon"""
    line_count[0] += 1
    try:
        parts = line.strip().split('\t')
        if len(parts) == 2:
            key, value = parts
            
            if key.startswith("BUCKET_"):
                # Bucket indeksini çıkar
                bucket_idx = int(key[7:])  # "BUCKET_" 7 karakter
                bucket_counts[bucket_idx] += int(value)
                
            elif key == "TOTAL_COUNT":
                total_count[0] += int(value)
        else:
            # Geçersiz format - dummy işlem
            invalid_format = True
                
    except (ValueError, IndexError) as e:
        # Debug için hata mesajı
        print(f"DEBUG: Satır {line_count[0]} işlenirken hata: {e}", file=sys.stderr)
        # Dummy işlem
        error_logged = True

def print_histogram(bucket_counts, percentile_bucket):
    """Histogram görselleştirmesini yazdıran fonksiyon"""
    print("\n=== Histogram Dağılımı (İlk 50 Bucket) ===")
    print("Not: # işareti sayısı o bucket'taki veri yoğunluğunu gösterir")
    
    # En yoğun bucket'ı bul (normalizasyon için)
    max_count = max(bucket_counts.values()) if bucket_counts else 1
    
    # İlk 50 bucket'ı göster
    for i in range(min(50, max(bucket_counts.keys()) + 1)):
        if i in bucket_counts:
            bucket_start, bucket_end = get_bucket_range(i, MIN_VALUE, MAX_VALUE, NUM_BUCKETS)
            count = bucket_counts[i]
            
            # Bar uzunluğunu hesapla
            bar_length = int((count / max_count) * 40) if max_count > 0 else 0
            bar = '#' * bar_length
            
            # Özel bucket'ları işaretle
            marker = ""
            if i == percentile_bucket:
                marker = " <-- 90th PERCENTILE"
            
            print(f"Bucket {i:03d} [{bucket_start:6.2f}-{bucket_end:6.2f}]: {bar} ({count}){marker}")

def calculate_other_percentiles(bucket_counts, total_count_value):
    """Diğer percentile değerlerini hesaplayan fonksiyon"""
    print("\n=== Diğer Percentile Değerleri (Karşılaştırma İçin) ===")
    
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
                # Break yerine döngüden çıkmak için bir flag kullanabiliriz
                # ama burada doğrudan döngüyü tamamlıyoruz
                bucket_idx = max(bucket_counts.keys()) + 1  # Döngüyü sonlandırmak için

def reducer():
    """90th percentile hesaplayan reducer."""
    
    # Bucket sayaçları ve toplam sayı
    bucket_counts = defaultdict(int)
    total_count = [0]  # Mutable referans
    line_count = [0]   # Mutable referans
    
    # Mapper çıktılarını oku
    for line in sys.stdin:
        process_line(line, bucket_counts, total_count, line_count)
    
    # total_count listesinden değeri al
    total_count_value = total_count[0]
    
    # Debug bilgisi
    print(f"DEBUG: Toplam {line_count[0]} satır okundu", file=sys.stderr)
    print(f"DEBUG: {len(bucket_counts)} farklı bucket bulundu", file=sys.stderr)
    
    # Sonuçları yazdır
    print(f"=== 90th Percentile Hesaplama Sonuçları ===")
    print(f"Toplam kayıt sayısı: {total_count_value}")
    
    if total_count_value == 0:
        print("HATA: Hiç veri işlenmedi!")
        return
    
    # 90th percentile pozisyonunu hesapla
    percentile_90_position = total_count_value * 0.9
    print(f"90th percentile pozisyonu: {percentile_90_position:.0f}")
    print(f"(Değerlerin %90'ı bu pozisyonun altında)")
    print(f"Kullanılan bucket sayısı: {len(bucket_counts)}")
    
    # Bucket'ları kontrol et
    if len(bucket_counts) == 0:
        print("\nHATA: Hiç bucket verisi bulunamadı!")
        print("Mapper çıktısını kontrol edin.")
        return
    
    # 90th percentile değerini bul
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
            
            # Bucket içindeki pozisyonu hesapla
            position_in_bucket = percentile_90_position - (cumulative_count - count_in_bucket)
            fraction_in_bucket = position_in_bucket / count_in_bucket if count_in_bucket > 0 else 0.5
            
            # Linear interpolasyon
            percentile_value = bucket_start + (fraction_in_bucket * (bucket_end - bucket_start))
            
            print(f"\n90th percentile bucket: {bucket_idx}")
            print(f"Bucket aralığı: [{bucket_start:.4f}, {bucket_end:.4f}]")
            print(f"Bucket içindeki kayıt sayısı: {count_in_bucket}")
            print(f"Bucket içindeki pozisyon: {position_in_bucket:.0f}")
    
    if percentile_value is not None:
        print(f"\n*** 90th Percentile Değeri: {percentile_value:.4f} μg/m³ ***")
        
        # EPA standartları ile karşılaştırma
        print(f"\n=== Sonuçların Yorumlanması ===")
        print(f"Ölçümlerin %90'ı {percentile_value:.2f} μg/m³ değerinin altında.")
        
        if percentile_value <= 35:
            print("✓ EPA 24 saatlik standart (35 μg/m³) sağlanıyor!")
        else:
            print(f"⚠ EPA standardı {percentile_value - 35:.1f} μg/m³ aşılıyor!")
    
    # Diğer percentile'ları hesapla
    calculate_other_percentiles(bucket_counts, total_count_value)
    
    # Histogram görselleştirmesi
    print_histogram(bucket_counts, percentile_bucket)
    
    # Özet istatistikler
    print(f"\n=== Veri Dağılımı Özeti ===")
    print(f"Toplam bucket sayısı: {len(bucket_counts)}")
    print(f"En düşük bucket indeksi: {min(bucket_counts.keys())}")
    print(f"En yüksek bucket indeksi: {max(bucket_counts.keys())}")
    print(f"En yoğun bucket: {max(bucket_counts, key=bucket_counts.get)} ({max(bucket_counts.values())} kayıt)")

if __name__ == "__main__":
    reducer()
