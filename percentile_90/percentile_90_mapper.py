#!/usr/bin/env python3
"""
90th Percentile Hesaplama - Histogram Tabanlı Mapper 

Bu mapper, median mapper'ı ile aynıdır çünkü her iki algoritma da
aynı histogram verilerine ihtiyaç duyar.
"""

import sys
import csv

# Histogram parametreleri
MIN_VALUE = 0.0      # PM2.5 negatif olamaz
MAX_VALUE = 500.0    # Makul bir üst sınır
NUM_BUCKETS = 1000   # Hassasiyet için yeterli

def get_bucket_index(value, min_val, max_val, num_buckets):
    """Verilen değer için bucket indeksini hesaplar."""
    if value <= min_val:
        return 0
    elif value >= max_val:
        return num_buckets - 1
    else:
        # Değeri normalize et ve bucket'a yerleştir
        normalized = (value - min_val) / (max_val - min_val)
        return int(normalized * (num_buckets - 1))

def process_row(row):
    """Her satırı işleyen yardımcı fonksiyon"""
    try:
        value = float(row['arithmetic_mean'])
        
        if value >= 0:  # Geçerli değerler
            # Hangi bucket'a düştüğünü bul
            bucket_idx = get_bucket_index(value, MIN_VALUE, MAX_VALUE, NUM_BUCKETS)
            
            # Bu bucket için sayacı artır
            print(f"BUCKET_{bucket_idx:04d}\t1")
            
            # Toplam sayı için de bir kayıt
            print(f"TOTAL_COUNT\t1")
            
    except (ValueError, KeyError):
        # Hatalı veya eksik değerleri sessizce atla
        # Dummy işlem - sadece bir değişken ataması
        error_handled = True

def mapper():
    """
    Her PM2.5 değerini uygun histogram bucket'ına yerleştirir.
    
    Çıktı formatı:
    BUCKET_indeks    1
    TOTAL_COUNT      1
    """
    
    csv_reader = csv.DictReader(sys.stdin)
    
    for row in csv_reader:
        process_row(row)

if __name__ == "__main__":
    mapper()
