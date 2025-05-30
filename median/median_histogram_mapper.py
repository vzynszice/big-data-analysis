#!/usr/bin/env python3
"""
Histogram Tabanlı Median Hesaplama - Mapper

Bu mapper, değerleri belirli aralıklara (bucket) yerleştirir.
Bu sayede tüm veriyi sıralamadan median'a yakın bir değer bulabiliriz.

Örnek: 0-10 arası değerler bucket_0, 10-20 arası bucket_1 gibi...
"""

import sys
import csv

# Histogram parametreleri
# Bu değerleri veri setinize göre ayarlayabilirsiniz
MIN_VALUE = 0.0      # PM2.5 negatif olamaz
MAX_VALUE = 500.0    # Makul bir üst sınır
NUM_BUCKETS = 1000   # Daha fazla bucket = daha hassas sonuç

def get_bucket_index(value, min_val, max_val, num_buckets):
    """
    Verilen değer için bucket indeksini hesaplar.
    
    Değeri 0 ile num_buckets-1 arasında bir indekse dönüştürür.
    """
    if value <= min_val:
        return 0
    elif value >= max_val:
        return num_buckets - 1
    else:
        # Değeri normalize et ve bucket'a yerleştir
        normalized = (value - min_val) / (max_val - min_val)
        return int(normalized * (num_buckets - 1))

def mapper():
    """
    Her PM2.5 değerini uygun histogram bucket'ına yerleştirir.
    
    Çıktı formatı:
    BUCKET_indeks    1
    TOTAL_COUNT      1
    """
    
    csv_reader = csv.DictReader(sys.stdin)
    
    for row in csv_reader:
        try:
            value = float(row['arithmetic_mean'])
            
            if value >= 0:  # Geçerli değerler
                # Hangi bucket'a düştüğünü bul
                bucket_idx = get_bucket_index(value, MIN_VALUE, MAX_VALUE, NUM_BUCKETS)
                
                # Bu bucket için sayacı artır
                print(f"BUCKET_{bucket_idx:04d}\t1")
                
                # Toplam sayı için de bir kayıt
                print(f"TOTAL_COUNT\t1")
                
                # Debug için: gerçek değeri de kaydet (opsiyonel)
                # Bu, doğruluk kontrolü için kullanılabilir
                if bucket_idx < 10:  # Sadece ilk birkaç bucket için
                    print(f"SAMPLE_{bucket_idx:04d}\t{value}")
                
        except (ValueError, KeyError):
            pass

if __name__ == "__main__":
    mapper()