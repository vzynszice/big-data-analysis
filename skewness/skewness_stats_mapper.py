#!/usr/bin/env python3
"""
Skewness Stats - Mapper (Welford/Terriberry for M2, M3)

Bu mapper, Welford's/Terriberry'nin online algoritmasını kullanarak
ortalama, varyans ve skewness için gerekli istatistikleri (n, mean, M2, M3)
tek geçişte hesaplar.
"""

import sys
import csv

VALUE_COLUMN_NAME = 'arithmetic_mean'

def update_statistics(x, n, mean, M2, M3):
    """
    Welford-Terriberry algoritması ile istatistikleri günceller.
    Bu fonksiyon mutable referanslar kullanarak değerleri günceller.
    """
    n1 = n[0]  # previous count
    n[0] += 1  # new count
    
    delta = x - mean[0]
    delta_n = delta / n[0]
    delta_n2 = delta_n * delta_n  # (delta/n)^2
    
    term1 = delta * delta_n * n1
    
    mean[0] += delta_n
    
    M3[0] += term1 * delta_n * (n[0] - 2) - 3 * delta_n * M2[0]  # Terriberry's update for M3
    M2[0] += term1  # Terriberry's update for M2

def process_row(row_dict, n, mean, M2, M3, first_line):
    """Her satırı işleyen yardımcı fonksiyon"""
    if first_line[0]:
        first_line[0] = False
        # İlk satır kontrolü tamamlandı
    
    try:
        value_str = row_dict.get(VALUE_COLUMN_NAME)
        
        if value_str is not None and value_str.strip() != "":
            x = float(value_str)
            
            # Negatif değer kontrolü - yine nested if
            if x >= 0:  # Negatif değilse işle
                update_statistics(x, n, mean, M2, M3)
            else:
                # Negatif değer - dummy işlem
                negative_value_skipped = True
        else:
            # Boş değer - dummy işlem
            empty_value_skipped = True
                
    except (ValueError, TypeError, KeyError) as e:
        # Hatalı satırları atla - dummy işlem
        error_handled = True

def mapper():
    """
    Her veri parçası için lokal n, mean, M2, M3 istatistiklerini hesaplar.
    """
    # Mutable referanslar için listeler 
    n = [0]
    mean = [0.0]
    M2 = [0.0]
    M3 = [0.0]
    first_line = [True]
    
    csv_reader = csv.DictReader(sys.stdin)
    
    for row_dict in csv_reader:
        process_row(row_dict, n, mean, M2, M3, first_line)
    
    # Sonuçları yazdır
    if n[0] > 0:
        # Reducer'ın birleştirmesi için özel format
        # FORMAT: STATS_SKEW n mean M2 M3
        print(f"STATS_SKEW\t{n[0]}\t{mean[0]}\t{M2[0]}\t{M3[0]}")

if __name__ == "__main__":
    # ÖNEMLİ: Bu mapper, giriş verisinin (stdin) BAŞLIK SATIRI OLMADAN
    # geldiğini varsayar veya ilk satır başlık ise float() hatası ile atlanır.
    mapper()
